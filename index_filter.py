"""Shared index-constituent filtering for S&P 500 / Dow 30.

Used by both download_edgar.py and ingest_edgar.py to filter filings
to specific market indices. Fetches constituents from Wikipedia and
caches them locally for a week.
"""

import json
import re
from datetime import datetime
from pathlib import Path

import httpx

_INDEX_CACHE_DIR = Path(__file__).resolve().parent / "data"


def _fetch_wikipedia_table_tickers(url: str, symbol_col: int = 0) -> set[str]:
    """Fetch tickers from a Wikipedia page with a sortable table.

    Parses the first wikitable and extracts text from ``symbol_col``
    of each body row.  Works for both the S&P 500 and Dow 30 pages.
    """
    headers = {"User-Agent": "RagoraDemo/1.0 (https://ragora.app; contact@ragora.app)"}
    resp = httpx.get(url, follow_redirects=True, timeout=30, headers=headers)
    resp.raise_for_status()
    html = resp.text

    # Find first wikitable
    table_match = re.search(
        r"<table[^>]*class=\"[^\"]*wikitable[^\"]*\"[^>]*>(.*?)</table>", html, re.S
    )
    if not table_match:
        return set()

    table_html = table_match.group(1)

    # Extract body rows (skip header)
    rows = re.findall(r"<tr>(.*?)</tr>", table_html, re.S)
    tickers: set[str] = set()
    for row in rows:
        cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.S)
        if len(cells) <= symbol_col:
            continue
        # Strip HTML tags and whitespace
        raw = re.sub(r"<[^>]+>", "", cells[symbol_col]).strip()
        if not raw or raw.lower() in ("symbol", "ticker", "ticker symbol"):
            continue
        tickers.add(raw.upper().replace(".", "-"))  # BRK.B -> BRK-B
    return tickers


def _load_cached_tickers(name: str) -> set[str] | None:
    cache_file = _INDEX_CACHE_DIR / f".{name}_tickers.json"
    if not cache_file.exists():
        return None
    try:
        data = json.loads(cache_file.read_text(encoding="utf-8"))
        age_hours = (
            datetime.now() - datetime.fromisoformat(data["fetched_at"])
        ).total_seconds() / 3600
        if age_hours > 24 * 7:  # refresh weekly
            return None
        return set(data["tickers"])
    except Exception:
        return None


def _save_cached_tickers(name: str, tickers: set[str]):
    _INDEX_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = _INDEX_CACHE_DIR / f".{name}_tickers.json"
    cache_file.write_text(
        json.dumps(
            {"fetched_at": datetime.now().isoformat(), "tickers": sorted(tickers)}
        ),
        encoding="utf-8",
    )


def get_sp500_tickers() -> set[str]:
    cached = _load_cached_tickers("sp500")
    if cached:
        return cached
    print("  Fetching S&P 500 constituents from Wikipedia...")
    tickers = _fetch_wikipedia_table_tickers(
        "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        symbol_col=0,
    )
    if tickers:
        _save_cached_tickers("sp500", tickers)
        print(f"  Got {len(tickers)} S&P 500 tickers")
    return tickers


def get_dow30_tickers() -> set[str]:
    cached = _load_cached_tickers("dow30")
    if cached:
        return cached
    print("  Fetching Dow 30 constituents from Wikipedia...")
    tickers = _fetch_wikipedia_table_tickers(
        "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average",
        symbol_col=2,
    )
    if tickers:
        _save_cached_tickers("dow30", tickers)
        print(f"  Got {len(tickers)} Dow 30 tickers")
    return tickers


def resolve_index_tickers(filter_str: str) -> set[str]:
    """Parse --index-filter value and return union of requested index tickers."""
    names = [n.strip().lower() for n in filter_str.split(",") if n.strip()]
    tickers: set[str] = set()
    for name in names:
        if name in ("sp500", "s&p500", "s&p"):
            tickers |= get_sp500_tickers()
        elif name in ("dow", "dow30", "djia"):
            tickers |= get_dow30_tickers()
        else:
            # Treat as a path to a file with one ticker per line
            path = Path(name)
            if path.is_file():
                for line in path.read_text(encoding="utf-8").splitlines():
                    t = line.strip().upper()
                    if t and not t.startswith("#"):
                        tickers.add(t)
            else:
                print(f"Warning: Unknown index filter '{name}', ignoring")
    return tickers


def ticker_from_filepath(filepath: Path) -> str:
    """Extract ticker from frontmatter without reading the full file."""
    with open(filepath, encoding="utf-8") as fh:
        first_line = fh.readline().strip()
        if first_line != "---":
            # No frontmatter -- infer from filename (TICKER_CIK_DATE.md)
            return filepath.stem.split("_")[0].upper()
        for line in fh:
            stripped = line.strip()
            if stripped == "---":
                break
            key, sep, val = stripped.partition(":")
            if sep and key.strip() == "ticker":
                raw = val.strip().strip('"').strip("'").upper()
                if raw and raw != "UNKNOWN":
                    return raw
    # Fallback: infer from filename
    return filepath.stem.split("_")[0].upper()
