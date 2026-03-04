"""Convert SEC EDGAR filing HTML into clean markdown for RAG.

Uses BeautifulSoup for document structure and pandas.read_html() for table
extraction. Produces markdown with:
- Section context headers above financial statement tables
- Merged currency columns ($40,000 instead of | $ | 40,000 |)
- No empty spacer columns
- No HTML tags
- Flattened multi-row headers
"""

import re
import warnings
import pandas as pd
from bs4 import BeautifulSoup, Tag

warnings.filterwarnings("ignore", category=FutureWarning)

# SEC financial statement heading patterns
FINANCIAL_STATEMENT_PATTERNS = [
    "CONSOLIDATED BALANCE SHEETS",
    "CONSOLIDATED STATEMENTS OF OPERATIONS",
    "CONSOLIDATED STATEMENTS OF COMPREHENSIVE INCOME",
    "CONSOLIDATED STATEMENTS OF CHANGES IN EQUITY",
    "CONSOLIDATED STATEMENTS OF CASH FLOWS",
    "NOTES TO CONSOLIDATED FINANCIAL STATEMENTS",
    "INDEX TO CONSOLIDATED FINANCIAL STATEMENTS",
]

ITEM_RE = re.compile(r"^ITEM\s*\d+[A-Z]?\.?\s+", re.IGNORECASE)

BOILERPLATE_PATTERNS = [
    re.compile(r"^Table of Contents$", re.IGNORECASE),
    re.compile(r"^\d{1,3}$"),
    re.compile(r"^See accompanying notes", re.IGNORECASE),
    re.compile(r"^The accompanying notes are an integral part", re.IGNORECASE),
    re.compile(r"^of Contents$", re.IGNORECASE),
]

UNITS_RE = re.compile(
    r"\(in\s+(millions|billions|thousands)(?:,?\s*except\s+[^)]+)?\)",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Heading / boilerplate helpers
# ---------------------------------------------------------------------------

def _is_heading_element(el: Tag) -> bool:
    if el.name == "div":
        span = el.find("span", style=re.compile(r"font-weight:\s*700"))
        if span:
            text = span.get_text(strip=True)
            if text and len(text) > 5:
                return True
    if el.name in ("h1", "h2", "h3", "h4", "h5", "h6"):
        return True
    return False


def _extract_heading_text(el: Tag) -> str:
    text = el.get_text(strip=True)
    return re.sub(r"[\xa0\s]+", " ", text).strip()


def _is_financial_statement(heading: str) -> bool:
    upper = heading.upper()
    return any(pat in upper for pat in FINANCIAL_STATEMENT_PATTERNS)


def _is_item_header(heading: str) -> bool:
    return bool(ITEM_RE.match(heading))


def _is_boilerplate(text: str) -> bool:
    return any(pat.match(text) for pat in BOILERPLATE_PATTERNS)


# ---------------------------------------------------------------------------
# Table header analysis
# ---------------------------------------------------------------------------

def _extract_units(df: pd.DataFrame) -> str | None:
    for i in range(min(5, len(df))):
        row_text = " ".join(str(v) for v in df.iloc[i] if pd.notna(v))
        m = UNITS_RE.search(row_text)
        if m:
            return m.group(0)
    return None


def _find_data_start(df: pd.DataFrame) -> int:
    """Find the first row that contains actual data (not headers/units).

    SEC tables can have many header/spacer rows before data (up to ~10+).
    """
    for i in range(min(15, len(df))):
        row = df.iloc[i]
        non_null = [(j, str(v).strip()) for j, v in enumerate(row)
                    if pd.notna(v) and str(v).strip()]
        if not non_null:
            continue
        first_text = non_null[0][1]
        # Skip header-like rows
        if re.search(r"(For the|% Change|in millions|in billions|in thousands)",
                     first_text, re.IGNORECASE):
            continue
        # Skip rows that are just years
        if all(re.match(r"^20\d{2}$", v) for _, v in non_null):
            continue
        # Skip rows where all values are identical (colspan header fill)
        unique_vals = set(v for _, v in non_null)
        if len(unique_vals) == 1 and len(non_null) > 2:
            # All columns say the same thing — it's a header
            val = next(iter(unique_vals))
            if re.search(r"(For the|September|in millions|in billions)", val, re.IGNORECASE):
                continue
        has_dollar = any(v == "$" for _, v in non_null)
        has_number = any(re.match(r"^[\d,]+\.?\d*$", v) for _, v in non_null)
        if (has_dollar or has_number) and not re.match(r"^20\d{2}$", first_text):
            return i
    return 0


def _extract_grouped_years(
    df: pd.DataFrame, data_start: int
) -> list[tuple[str, str]]:
    """Extract years from headers with column-group context (super-headers).

    For tables with multi-level headers (e.g. "U.S." | "International" above
    year rows), returns ``[(group, year), ...]`` preserving *all* occurrences
    so each data column gets a unique header like ``U.S. FY2025``.

    When no super-headers are found, returns ``[("", year), ...]`` with unique
    years (backward-compatible with the old flat year list).
    """
    if data_start < 1:
        return []

    # ------------------------------------------------------------------
    # 1. Find the first header row that contains year values.
    #    Collapse adjacent duplicates produced by colspan expansion so
    #    that each *logical* year column maps to one entry.
    # ------------------------------------------------------------------
    year_row_idx: int | None = None
    year_positions: list[tuple[int, str]] = []  # (col_index, year)

    for i in range(data_start):
        row = df.iloc[i]
        positions: list[tuple[int, str]] = []
        prev_year: str | None = None
        prev_col = -2
        for col_idx in range(len(row)):
            v = row.iloc[col_idx]
            s = str(v).strip() if pd.notna(v) else ""
            if re.match(r"^20\d{2}$", s):
                # Skip adjacent duplicate of the same year (colspan artefact)
                if s == prev_year and col_idx == prev_col + 1:
                    prev_col = col_idx
                    continue
                positions.append((col_idx, s))
                prev_year = s
                prev_col = col_idx
            else:
                prev_year = None
                prev_col = -2
        if positions:
            year_row_idx = i
            year_positions = positions
            break

    if not year_positions:
        return []

    # ------------------------------------------------------------------
    # 2. Look for a super-header row *above* the year row.
    #    A super-header row has >=2 distinct non-trivial text values at
    #    the same column positions as the years (e.g. "U.S." over the
    #    first three year columns, "International" over the next three).
    # ------------------------------------------------------------------
    if year_row_idx is not None and year_row_idx > 0:
        for i in range(year_row_idx - 1, -1, -1):
            row = df.iloc[i]
            col_groups: dict[int, str] = {}
            for col_idx, _ in year_positions:
                if col_idx < len(row):
                    v = row.iloc[col_idx]
                    s = str(v).strip() if pd.notna(v) else ""
                    if s and not re.match(r"^20\d{2}$", s) and s not in ("$", "%"):
                        col_groups[col_idx] = s

            if not col_groups:
                continue

            unique_groups = set(col_groups.values())
            if len(unique_groups) >= 2:
                # Found meaningful super-headers — build grouped list.
                result: list[tuple[str, str]] = []
                for col_idx, year in year_positions:
                    group = col_groups.get(col_idx, "")
                    result.append((group, year))
                return result

    # ------------------------------------------------------------------
    # 3. No super-headers — return unique years (backward compatible).
    # ------------------------------------------------------------------
    seen: set[str] = set()
    result = []
    for _, year in year_positions:
        if year not in seen:
            seen.add(year)
            result.append(("", year))
    return result


def _extract_pct_change_headers(df: pd.DataFrame, data_start: int) -> list[str]:
    headers = []
    for i in range(data_start):
        row = df.iloc[i]
        for v in row:
            if pd.notna(v):
                s = str(v).strip()
                if re.search(r"20\d{2}\s*vs\.?\s*20\d{2}", s) and s not in headers:
                    headers.append(s)
    return headers


# ---------------------------------------------------------------------------
# Core table cleaning
# ---------------------------------------------------------------------------

def _collapse_colspan_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse columns that are duplicated by colspan expansion.

    SEC HTML uses colspan to span cells across multiple columns. When pandas
    expands these, it creates adjacent columns where:
    - In rows with $, col A = "$" and col B = value (different)
    - In rows without $, col A = col B = value (identical, from colspan)

    This function detects such column pairs and merges them: keeping the $
    in the appropriate rows and the value in all rows.
    """
    if len(df.columns) < 2:
        return df

    cols = list(df.columns)
    cols_to_drop = set()
    i = 0
    while i < len(cols) - 1:
        col_a = cols[i]
        col_b = cols[i + 1]

        if col_a in cols_to_drop:
            i += 1
            continue

        # Check if col_a is "$" when col_b has a value, and col_a == col_b otherwise
        is_currency_pair = True
        has_dollar = False
        for row_idx in df.index:
            a_val = df.at[row_idx, col_a]
            b_val = df.at[row_idx, col_b]
            a_str = str(a_val).strip() if pd.notna(a_val) else ""
            b_str = str(b_val).strip() if pd.notna(b_val) else ""

            if a_str == "$" and b_str:
                has_dollar = True
            elif a_str == b_str:
                pass  # colspan duplicate
            elif a_str == "" and b_str == "":
                pass  # both empty
            elif a_str and not b_str:
                # a has value, b is empty — not a currency pair
                is_currency_pair = False
                break
            else:
                is_currency_pair = False
                break

        if is_currency_pair and has_dollar:
            # Merge: col_a gets "$" + value from col_b for $ rows,
            # just the value for other rows. Drop col_b.
            for row_idx in df.index:
                a_val = df.at[row_idx, col_a]
                b_val = df.at[row_idx, col_b]
                a_str = str(a_val).strip() if pd.notna(a_val) else ""
                b_str = str(b_val).strip() if pd.notna(b_val) else ""

                if a_str == "$" and b_str:
                    df.at[row_idx, col_a] = f"${_format_number(b_str)}"
                # else: col_a already has the value from colspan
            cols_to_drop.add(col_b)
            i += 2  # skip past the pair
        else:
            i += 1

    if cols_to_drop:
        df = df.drop(columns=list(cols_to_drop))
    return df


def _parse_table_bs4(table_html: str) -> pd.DataFrame | None:
    """Parse an HTML table into a DataFrame of strings using BS4 directly."""
    soup = BeautifulSoup(table_html, "html.parser")
    table = soup.find("table")
    if not table:
        return None
    rows = []
    for tr in table.find_all("tr"):
        cells = []
        for td in tr.find_all(["td", "th"]):
            colspan = int(td.get("colspan", 1))
            text = td.get_text(strip=True)
            cells.append(text)
            for _ in range(colspan - 1):
                cells.append(text)
        if cells:
            rows.append(cells)
    if not rows:
        return None
    max_cols = max(len(r) for r in rows)
    for r in rows:
        while len(r) < max_cols:
            r.append("")
    return pd.DataFrame(rows)


def _clean_financial_table(table_html: str) -> tuple[str | None, str]:
    """Clean a financial table from HTML to markdown.

    Returns (units_note, markdown_table).
    """
    try:
        df = _parse_table_bs4(table_html)
    except Exception:
        return None, ""
    if df is None or df.empty:
        return None, ""

    # Extract units before we start modifying
    units = _extract_units(df)

    # Find where data starts (skip header rows)
    data_start = _find_data_start(df)
    year_groups = _extract_grouped_years(df, data_start)
    pct_headers = _extract_pct_change_headers(df, data_start)

    # Slice to data rows only
    df = df.iloc[data_start:].reset_index(drop=True)

    if df.empty:
        return units, ""

    # Step 1: Drop columns that are all-NaN in data rows
    df = df.dropna(axis=1, how="all")
    if df.empty:
        return units, ""

    # Step 2: Drop columns where all non-NaN values are empty strings
    cols_with_data = [col for col in df.columns
                      if any(str(v).strip() for v in df[col].dropna())]
    if not cols_with_data:
        return units, ""
    df = df[cols_with_data].copy()

    # Step 3: Deduplicate fully identical adjacent columns
    keep = [df.columns[0]]
    for i in range(1, len(df.columns)):
        if not df[df.columns[i]].equals(df[df.columns[i - 1]]):
            keep.append(df.columns[i])
    df = df[keep].copy()

    # Step 4: Collapse currency/colspan pairs ($ + value → $value)
    df = _collapse_colspan_duplicates(df)

    # Step 5: Handle remaining standalone $ columns (pure $ columns)
    col_list = list(df.columns)
    merged_cols = set()
    for col in col_list:
        vals = [str(v).strip() for v in df[col].dropna()]
        if vals and all(v in ("$", "") for v in vals) and any(v == "$" for v in vals):
            # Find next value column
            ci = col_list.index(col)
            for ni in range(ci + 1, len(col_list)):
                nc = col_list[ni]
                if nc not in merged_cols:
                    for row_idx in df.index:
                        cv = df.at[row_idx, col]
                        nv = df.at[row_idx, nc]
                        if pd.notna(cv) and str(cv).strip() == "$" and pd.notna(nv):
                            df.at[row_idx, nc] = f"${_format_number(str(nv).strip())}"
                    merged_cols.add(col)
                    break

    # Step 6: Handle standalone % columns
    for col in col_list:
        if col in merged_cols:
            continue
        vals = [str(v).strip() for v in df[col].dropna()]
        if vals and all(v in ("%", "") for v in vals) and any(v == "%" for v in vals):
            ci = col_list.index(col)
            for pi in range(ci - 1, -1, -1):
                pc = col_list[pi]
                if pc not in merged_cols:
                    for row_idx in df.index:
                        pct_v = df.at[row_idx, col]
                        prev_v = df.at[row_idx, pc]
                        if (pd.notna(pct_v) and str(pct_v).strip() == "%"
                                and pd.notna(prev_v)):
                            df.at[row_idx, pc] = f"{str(prev_v).strip()}%"
                    merged_cols.add(col)
                    break

    # Drop merged columns
    if merged_cols:
        df = df.drop(columns=[c for c in merged_cols if c in df.columns])

    # Step 7: Drop any newly empty columns
    keep_cols = [col for col in df.columns
                 if any(str(v).strip() for v in df[col] if pd.notna(v))]
    if not keep_cols:
        return units, ""
    df = df[keep_cols].copy()

    # Step 8: Clean cells
    df = df.fillna("")
    for col in df.columns:
        df[col] = df[col].apply(_clean_cell)

    # Step 9: Format standalone numbers
    for col in df.columns:
        df[col] = df[col].apply(_maybe_format_number)

    # Step 10: Drop all-empty rows
    df = df[df.apply(lambda row: any(str(v).strip() for v in row), axis=1)]
    df = df.reset_index(drop=True)

    if df.empty:
        return units, ""

    # Step 11: Assign headers
    headers = _build_headers(year_groups, pct_headers, df)
    df.columns = headers

    return units, _df_to_markdown(df)


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _format_number(s: str) -> str:
    s = s.strip()
    if "," in s:
        return s
    try:
        if "." in s:
            num = float(s)
            dec_places = len(s.split(".")[1])
            return f"{num:,.{dec_places}f}"
        else:
            neg = False
            clean = s
            if s.startswith("(") and s.endswith(")"):
                neg = True
                clean = s[1:-1]
            num = int(clean)
            result = f"{num:,}"
            return f"({result})" if neg else result
    except (ValueError, OverflowError):
        return s


def _maybe_format_number(val: str) -> str:
    val = str(val).strip()
    if not val or "$" in val or "%" in val or "," in val:
        return val
    clean = val
    if clean.startswith("(") and clean.endswith(")"):
        clean = clean[1:-1]
    if re.match(r"^\d+\.?\d*$", clean):
        return _format_number(val)
    return val


def _clean_cell(val) -> str:
    s = str(val).strip()
    if s in ("nan", "NaN", "None", ""):
        return ""
    s = re.sub(r"<[^>]+>", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _build_headers(year_groups: list[tuple[str, str]], pct_headers: list[str],
                   df: pd.DataFrame) -> list[str]:
    """Assign column headers using extracted year groups and pct-change labels.

    *year_groups* is a list of ``(group_name, year)`` tuples produced by
    ``_extract_grouped_years``.  When super-headers are present the group
    name is non-empty (e.g. ``"U.S."``) and the header becomes
    ``"U.S. FY2025"``; otherwise just ``"FY2025"``.
    """
    n_cols = len(df.columns)
    headers = [""] * n_cols

    if not year_groups or n_cols < 2:
        return headers

    has_groups = any(g for g, _ in year_groups)

    def _fmt(group: str, year: str) -> str:
        if has_groups and group:
            return f"{group} FY{year}"
        return f"FY{year}"

    # Identify value columns (majority numeric/currency)
    value_col_indices = []
    for col_idx, col in enumerate(df.columns):
        vals = [str(v).strip() for v in df[col] if str(v).strip()]
        numeric_count = sum(
            1 for v in vals
            if re.match(r"^[\$\(]?-?[\d,]+\.?\d*\)?%?$", v) or v == "—"
        )
        if vals and numeric_count / len(vals) > 0.3:
            value_col_indices.append(col_idx)

    n_value = len(value_col_indices)
    n_years = len(year_groups)
    n_pct = len(pct_headers)

    if n_value == 0:
        return headers

    if n_pct > 0 and n_value == n_years + n_pct:
        for i, (group, year) in enumerate(year_groups):
            if i < n_value:
                headers[value_col_indices[i]] = _fmt(group, year)
        for j, ph in enumerate(pct_headers):
            idx = n_years + j
            if idx < n_value:
                clean = re.sub(r"(20\d{2})", r"FY\1", ph)
                clean = clean.replace("vs.", "vs").replace("  ", " ")
                headers[value_col_indices[idx]] = clean
    elif n_value == n_years:
        for i, (group, year) in enumerate(year_groups):
            headers[value_col_indices[i]] = _fmt(group, year)
    else:
        for i, (group, year) in enumerate(year_groups):
            if i < n_value:
                headers[value_col_indices[i]] = _fmt(group, year)

    return headers


def _df_to_markdown(df: pd.DataFrame) -> str:
    headers = list(df.columns)
    rows = df.values.tolist()

    alignments = []
    for col in df.columns:
        vals = [str(v).strip() for v in df[col] if str(v).strip()]
        numeric_count = sum(
            1 for v in vals
            if re.match(r"^[\$\(]?-?[\d,]+\.?\d*\)?%?$", v)
        )
        alignments.append("---:" if vals and numeric_count / len(vals) > 0.5 else "---")

    lines = ["| " + " | ".join(headers) + " |",
             "|" + "|".join(alignments) + "|"]
    for row in rows:
        cells = [str(v) if str(v).strip() else "" for v in row]
        lines.append("| " + " | ".join(cells) + " |")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main converter class
# ---------------------------------------------------------------------------

class SECHTMLToMarkdown:
    """Convert SEC EDGAR filing HTML to clean markdown."""

    def convert(self, html: str, filing_meta: dict | None = None) -> str:
        soup = BeautifulSoup(html, "html.parser")

        # Remove inline XBRL hidden data and display:none elements
        for el in soup.find_all(style=re.compile(r"display:\s*none", re.IGNORECASE)):
            el.decompose()
        for el in soup.find_all(re.compile(r"^ix:", re.IGNORECASE)):
            if el.name and el.name.lower().startswith("ix:hidden"):
                el.decompose()

        body = soup.find("body") or soup

        output_parts: list[str] = []
        current_heading = ""
        seen_tables: set[int] = set()

        for element in body.children:
            if not isinstance(element, Tag):
                continue
            self._process_element(element, output_parts, current_heading, seen_tables)
            current_heading = self._last_heading(output_parts, current_heading)

        result = "\n\n".join(filter(None, output_parts))
        return re.sub(r"<[^>]+>", "", result)

    def _last_heading(self, parts: list[str], default: str) -> str:
        for p in reversed(parts):
            if p.startswith("## ") or p.startswith("### "):
                return p.lstrip("#").strip()
        return default

    def _process_element(self, element: Tag, output_parts: list[str],
                         current_heading: str, seen_tables: set[int]) -> None:
        if id(element) in seen_tables:
            return

        # Table
        if element.name == "table":
            seen_tables.add(id(element))
            cur = self._last_heading(output_parts, current_heading)
            self._process_table(element, cur, output_parts)
            return

        # Heading (bold-styled span in a div, or HTML heading tag)
        added_heading = False
        if _is_heading_element(element):
            heading_text = _extract_heading_text(element)
            if heading_text and not _is_boilerplate(heading_text):
                if _is_financial_statement(heading_text) or _is_item_header(heading_text):
                    output_parts.append(f"## {heading_text}")
                elif len(heading_text) < 120:
                    output_parts.append(f"### {heading_text}")
                added_heading = True

        # Recurse into divs for nested tables
        if element.name == "div":
            nested_tables = element.find_all("table", recursive=False)
            if nested_tables:
                for child in element.children:
                    if isinstance(child, Tag):
                        self._process_element(child, output_parts,
                                              current_heading, seen_tables)
                return

            # If heading was already added above, don't re-detect
            if added_heading:
                return

        # Regular text
        text = element.get_text(strip=True)
        if text and len(text) > 3 and not _is_boilerplate(text):
            text = re.sub(r"[\xa0]+", " ", text)
            text = re.sub(r"\s+", " ", text).strip()
            if len(text) > 10 or not text.isdigit():
                output_parts.append(text)

    def _process_table(self, table_el: Tag, current_heading: str,
                       output_parts: list[str]) -> None:
        rows = table_el.find_all("tr")
        if len(rows) < 2:
            return

        units, md = _clean_financial_table(str(table_el))

        if not md or not md.strip():
            return

        if units and _is_financial_statement(current_heading):
            output_parts.append(f"*{units}*")

        output_parts.append(md)
