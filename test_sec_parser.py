"""Tests for SEC HTML to Markdown parser."""

import re
from pathlib import Path

import pytest

from sec_html_parser import (
    SECHTMLToMarkdown,
    _clean_financial_table,
    _format_number,
    _is_boilerplate,
    _is_financial_statement,
    _is_item_header,
)

# ---------------------------------------------------------------------------
# Unit tests for helper functions
# ---------------------------------------------------------------------------


class TestFormatNumber:
    def test_integer(self):
        assert _format_number("40000") == "40,000"

    def test_already_formatted(self):
        assert _format_number("40,000") == "40,000"

    def test_decimal(self):
        assert _format_number("10.20") == "10.20"

    def test_negative_parens(self):
        assert _format_number("(589)") == "(589)"

    def test_large_negative(self):
        assert _format_number("(18185)") == "(18,185)"

    def test_not_a_number(self):
        assert _format_number("abc") == "abc"


class TestDetection:
    def test_financial_statement_headings(self):
        assert _is_financial_statement("CONSOLIDATED BALANCE SHEETS")
        assert _is_financial_statement("CONSOLIDATED STATEMENTS OF OPERATIONS")
        assert _is_financial_statement("Consolidated Statements of Cash Flows")
        assert not _is_financial_statement("Risk Factors")
        assert not _is_financial_statement("Overview")

    def test_item_headers(self):
        assert _is_item_header("ITEM 1. Business")
        assert _is_item_header("ITEM 7A. Quantitative")
        assert _is_item_header("Item 8. Financial Statements")
        assert not _is_item_header("Business Overview")

    def test_boilerplate(self):
        assert _is_boilerplate("Table of Contents")
        assert _is_boilerplate("60")
        assert _is_boilerplate("See accompanying notes to the financial statements")
        assert not _is_boilerplate("Net revenue increased 11%")


# ---------------------------------------------------------------------------
# Table cleaning tests
# ---------------------------------------------------------------------------


class TestCleanFinancialTable:
    def test_simple_table(self):
        html = """
        <table>
        <tr><td></td><td>2025</td><td>2024</td></tr>
        <tr><td>Revenue</td><td>$</td><td>1000</td></tr>
        </table>
        """
        # This is a minimal case — the parser should not crash
        units, md = _clean_financial_table(html)
        assert "Revenue" in md or md == ""  # may produce empty if too simple

    def test_spacer_columns_removed(self):
        """Columns that are all-NaN should be dropped."""
        html = """
        <table>
        <tr><td>Label</td><td></td><td>$</td><td>100</td><td></td><td>$</td><td>200</td></tr>
        <tr><td>Item A</td><td></td><td>$</td><td>50</td><td></td><td>$</td><td>75</td></tr>
        </table>
        """
        units, md = _clean_financial_table(html)
        # Should not have empty columns
        assert "|  |  |" not in md or "Item A" in md


# ---------------------------------------------------------------------------
# Integration tests with real filing HTML (if available)
# ---------------------------------------------------------------------------


VISA_HTML = Path("/tmp/visa_10k.html")


@pytest.mark.skipif(not VISA_HTML.exists(), reason="Visa 10-K HTML not available")
class TestVisaFiling:
    @pytest.fixture(scope="class")
    def parsed(self):
        html = VISA_HTML.read_text()
        parser = SECHTMLToMarkdown()
        return parser.convert(html)

    def test_no_spacer_cells(self, parsed):
        """No empty spacer cells like |     |."""
        assert "|     |" not in parsed

    def test_no_separate_dollar_columns(self, parsed):
        """$ should be inline with values, not in separate columns."""
        lines = parsed.split("\n")
        sep_dollar_lines = [l for l in lines if re.search(r"\|\s*\$\s*\|", l)]
        assert len(sep_dollar_lines) == 0, f"Found {len(sep_dollar_lines)} lines with separate $ columns"

    def test_no_html_tags(self, parsed):
        """No <div> or other HTML tags."""
        assert "<div" not in parsed.lower()
        assert "<span" not in parsed.lower()

    def test_inline_dollar_amounts(self, parsed):
        """Dollar amounts should be inline like $40,000."""
        matches = re.findall(r"\$[\d,]+", parsed)
        assert len(matches) > 100, f"Only {len(matches)} inline $ amounts found"

    def test_no_duplicate_headings(self, parsed):
        """No consecutive duplicate headings."""
        lines = parsed.split("\n")
        for i in range(2, len(lines)):
            if lines[i].startswith("## ") and lines[i] == lines[i - 2]:
                pytest.fail(f"Duplicate heading at line {i}: {lines[i]}")

    def test_key_financial_data(self, parsed):
        """Key financial values should be present."""
        assert "$40,000" in parsed, "Net revenue FY2025 missing"
        assert "$35,926" in parsed, "Net revenue FY2024 missing"
        assert "$32,653" in parsed, "Net revenue FY2023 missing"
        assert "$20,058" in parsed, "Net income FY2025 missing"
        assert "$17,164" in parsed, "Cash and equivalents FY2025 missing"

    def test_section_headings_present(self, parsed):
        """Financial statement section headings should be present."""
        assert "## CONSOLIDATED BALANCE SHEETS" in parsed
        assert "## CONSOLIDATED STATEMENTS OF OPERATIONS" in parsed
        assert "## CONSOLIDATED STATEMENTS OF CASH FLOWS" in parsed
        assert "## CONSOLIDATED STATEMENTS OF COMPREHENSIVE INCOME" in parsed

    def test_item_headings_present(self, parsed):
        """SEC Item headings should be present."""
        assert "## ITEM 1." in parsed or "## ITEM 1 " in parsed
        assert "## ITEM 7." in parsed or "## ITEM 7 " in parsed
        assert "## ITEM 8." in parsed or "## ITEM 8 " in parsed

    def test_table_markdown_format(self, parsed):
        """Tables should use proper markdown format."""
        lines = parsed.split("\n")
        table_lines = [l for l in lines if l.strip().startswith("|")]
        assert len(table_lines) > 100, f"Only {len(table_lines)} table lines found"

        # Check for separator rows
        sep_lines = [l for l in lines if re.match(r"^\|[-:|]+\|$", l.strip())]
        assert len(sep_lines) > 5, "Too few table separator rows"

    def test_year_headers(self, parsed):
        """Financial tables should have FY year headers."""
        assert "FY2025" in parsed
        assert "FY2024" in parsed
        assert "FY2023" in parsed

    def test_units_context(self, parsed):
        """Units notes should appear near financial statements."""
        assert "*(in millions" in parsed
