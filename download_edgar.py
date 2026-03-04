#!/usr/bin/env python3
"""Download SEC filings (10-K, 10-Q) as clean markdown from EDGAR.

Usage:
    python download_edgar.py                          # Download S&P 500 10-K (default)
    python download_edgar.py --index-filter dow       # Only Dow 30 companies
    python download_edgar.py --index-filter sp500,dow # Both indices
    python download_edgar.py --months 12              # Last 12 months of 10-K filings
    python download_edgar.py --include-10q            # Also download 10-Q filings
    python download_edgar.py --limit 10               # Test with 10 filings
    python download_edgar.py --update                 # Scan only new quarters since last run
    python download_edgar.py --retry                  # Retry only previously failed downloads
    python download_edgar.py --rescan                 # Force full re-scan of EDGAR index
    python download_edgar.py --reset                  # Delete state and start fresh

Requires: pip install edgartools
Set EDGAR_IDENTITY env var (defaults to "Ragora contact@ragora.app").
"""

import argparse
import json
import os
import signal
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Callable, TypeVar

from index_filter import resolve_index_tickers
from sec_html_parser import SECHTMLToMarkdown

_ticker_cache: dict[str, str] = {}
_shutdown_requested = False
_sec_parser = SECHTMLToMarkdown()
T = TypeVar("T")


def _handle_sigint(signum, frame):
    global _shutdown_requested
    if _shutdown_requested:
        print("\nForce exit.")
        sys.exit(1)
    _shutdown_requested = True
    print("\nInterrupt received — finishing current item and saving state...")


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------

def load_state(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            print(f"Warning: Could not load state file ({e}), starting fresh.")
    return {"last_index_scan": None, "scan_params": {}, "filings": {}}


def save_state(state: dict, path: Path):
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _throttle(seconds: float):
    if seconds > 0:
        time.sleep(seconds)


def _retry(
    action: str, fn: Callable[[], T], max_retries: int, backoff: float = 1.0
) -> T:
    last_err = None
    for attempt in range(max_retries + 1):
        if _shutdown_requested:
            raise InterruptedError("shutdown requested")
        try:
            return fn()
        except Exception as e:
            last_err = e
            if attempt >= max_retries:
                break
            wait = backoff * (2 ** attempt)
            print(f"\n    {action}: {e} (retry {attempt+1}/{max_retries} in {wait:.1f}s)")
            traceback.print_exc()
            time.sleep(wait)
    raise RuntimeError(f"{action} failed after {max_retries+1} attempt(s): {last_err}")


def _safe_date(value) -> str:
    if value is None:
        return ""
    raw = str(value).strip()
    if not raw:
        return ""
    if "T" in raw:
        return raw.split("T", 1)[0]
    if " " in raw:
        return raw.split(" ", 1)[0]
    return raw


def _parse_date(value) -> datetime | None:
    raw = _safe_date(value)
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d")
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# EDGAR index scanning (Phase 1) — network-light, returns lightweight dicts
# ---------------------------------------------------------------------------

def _quarters_in_range(months_back: int, start_from: str | None = None):
    now = datetime.now()
    cur_y, cur_q = now.year, (now.month - 1) // 3 + 1

    if start_from:
        start = datetime.fromisoformat(start_from)
    else:
        y, m = now.year, now.month - months_back
        while m <= 0:
            m += 12
            y -= 1
        start = now.replace(year=y, month=m, day=1)

    sy, sq = start.year, (start.month - 1) // 3 + 1
    quarters = []
    for y in range(cur_y, sy - 1, -1):
        for q in range(4, 0, -1):
            if y == cur_y and q > cur_q:
                continue
            if y == sy and q < sq:
                continue
            if y < sy:
                continue
            quarters.append((y, q))
    return quarters


def build_filing_index(
    months_back: int = 36,
    limit: int = 0,
    start_from: str | None = None,
    delay: float = 0.2,
    max_retries: int = 3,
    form_types: list[str] | None = None,
) -> dict[str, dict]:
    """Scan EDGAR for filings, return {accession_number: lightweight_dict}."""
    from edgar import get_filings

    form_types = form_types or ["10-K"]
    quarters = _quarters_in_range(months_back, start_from=start_from)
    result: dict[str, dict] = {}

    for year, quarter in quarters:
        print(f"  Scanning {year} Q{quarter}...", end=" ", flush=True)
        try:
            qcount = 0
            for ft in form_types:
                def fetch(ft=ft):
                    _throttle(delay)
                    return get_filings(year, quarter).filter(form=ft)

                filings = _retry(f"index {ft} {year}Q{quarter}", fetch, max_retries)
                if filings is None:
                    continue
                for f in filings:
                    acc = str(getattr(f, "accession_number", "") or "")
                    if not acc or acc in result:
                        continue
                    src = (
                        getattr(f, "filing_url", None)
                        or getattr(f, "url", None)
                        or getattr(f, "link", None)
                    )
                    result[acc] = {
                        "cik": str(f.cik).zfill(10),
                        "accession_number": acc,
                        "company": str(getattr(f, "company", "") or ""),
                        "form_type": str(getattr(f, "form", "") or "").upper(),
                        "filing_date": _safe_date(getattr(f, "filing_date", "")),
                        "source_url": str(src or ""),
                    }
                    qcount += 1
                    if limit and len(result) >= limit:
                        break
                if limit and len(result) >= limit:
                    break
            print(f"{qcount} filings ({len(result)} total)")
        except Exception as e:
            print(f"error: {e}")
        if limit and len(result) >= limit:
            break
    return result


# ---------------------------------------------------------------------------
# Ticker-based EDGAR scanning (used with --index-filter)
# ---------------------------------------------------------------------------

def build_filing_index_by_tickers(
    tickers: set[str],
    months_back: int = 36,
    limit: int = 0,
    delay: float = 0.2,
    max_retries: int = 3,
    form_types: list[str] | None = None,
) -> dict[str, dict]:
    """Look up filings for specific tickers on EDGAR."""
    from edgar import Company

    form_types = form_types or ["10-K"]
    now = datetime.now()
    y, m = now.year, now.month - months_back
    while m <= 0:
        m += 12
        y -= 1
    cutoff = datetime(y, m, 1)

    result: dict[str, dict] = {}
    sorted_tickers = sorted(tickers)

    for idx, ticker in enumerate(sorted_tickers, 1):
        if _shutdown_requested:
            break
        print(f"  [{idx}/{len(sorted_tickers)}] {ticker}...", end=" ", flush=True)
        try:
            count = 0
            for ft in form_types:
                def fetch(t=ticker, f=ft):
                    _throttle(delay)
                    return Company(t).get_filings(form=f)

                filings = _retry(f"lookup {ticker} {ft}", fetch, max_retries)
                if filings is None:
                    continue
                for f in filings:
                    fd = _parse_date(getattr(f, "filing_date", ""))
                    if fd and fd < cutoff:
                        break  # filings are sorted newest-first
                    acc = str(getattr(f, "accession_number", "") or "")
                    if not acc or acc in result:
                        continue
                    src = (
                        getattr(f, "filing_url", None)
                        or getattr(f, "url", None)
                        or getattr(f, "link", None)
                    )
                    result[acc] = {
                        "cik": str(f.cik).zfill(10),
                        "ticker": ticker,
                        "accession_number": acc,
                        "company": str(getattr(f, "company", "") or ""),
                        "form_type": str(getattr(f, "form", "") or "").upper(),
                        "filing_date": _safe_date(getattr(f, "filing_date", "")),
                        "source_url": str(src or ""),
                    }
                    count += 1
                    if limit and len(result) >= limit:
                        break
                if limit and len(result) >= limit:
                    break
            print(f"{count} filings ({len(result)} total)")
        except Exception as e:
            print(f"error: {e}")
        if limit and len(result) >= limit:
            break
    return result


# ---------------------------------------------------------------------------
# Merge index into state — pure dict work, zero network calls
# ---------------------------------------------------------------------------

def merge_index_into_state(state: dict, index: dict[str, dict]) -> int:
    """Add new filings to state. Returns count of newly added entries."""
    new = 0
    for acc, entry in index.items():
        if acc in state["filings"]:
            continue
        new += 1
        fdt = _parse_date(entry["filing_date"])
        state["filings"][acc] = {
            "cik": entry["cik"],
            "accession_number": acc,
            "company": entry["company"],
            "ticker": entry.get("ticker", ""),
            "form_type": entry["form_type"],
            "filing_date": entry["filing_date"],
            "filing_year": fdt.year if fdt else None,
            "filing_quarter": ((fdt.month - 1) // 3 + 1) if fdt else None,
            "source_url": entry["source_url"],
            "filename": "",
            "status": "pending",
            "error": None,
            "updated_at": datetime.now().isoformat(),
        }
    return new


# ---------------------------------------------------------------------------
# Scan disk for already-downloaded files (recover from missing state)
# ---------------------------------------------------------------------------

def _scan_existing_files(output_dir: Path) -> set[str]:
    """Return set of filenames found on disk."""
    return {md.name for md in output_dir.rglob("*.md")}


def mark_already_downloaded(state: dict, output_dir: Path) -> int:
    """Check pending filings against files on disk, mark as success. Returns count."""
    existing = _scan_existing_files(output_dir)
    if not existing:
        return 0
    marked = 0
    for info in state["filings"].values():
        if info["status"] != "pending":
            continue
        fn = info.get("filename", "")
        if fn and fn in existing:
            info["status"] = "success"
            marked += 1
    return marked


# ---------------------------------------------------------------------------
# Download (Phase 2) — one EDGAR call per filing
# ---------------------------------------------------------------------------

def _resolve_ticker(filing) -> str:
    cik = str(filing.cik)
    if cik in _ticker_cache:
        return _ticker_cache[cik]
    ticker = "UNKNOWN"
    try:
        entity = filing.get_entity()
        tickers = getattr(entity, "tickers", None)
        if tickers and len(tickers) > 0:
            ticker = str(tickers[0]).upper()
    except Exception:
        pass
    _ticker_cache[cik] = ticker
    return ticker


def _fiscal_year(filing) -> int | None:
    """Derive fiscal year from period_of_report (preferred) or filing_date."""
    por = _parse_date(getattr(filing, "period_of_report", None))
    if por:
        return por.year
    fd = _parse_date(getattr(filing, "filing_date", None))
    if fd:
        return fd.year
    return None


def _make_filename(ticker: str, form: str, filing) -> str:
    safe_ticker = ticker.replace("/", "_").replace("\\", "_")
    safe_form = form.replace("/", "-")
    # Use period_of_report date — unique per form type per company per period.
    # Works for all filing types: 10-K, 10-Q, 8-K, DEF 14A, etc.
    por = _safe_date(getattr(filing, "period_of_report", None))
    if por:
        return f"{safe_ticker}_{safe_form}_{por}.md"
    # Fallback to filing date
    fd = _safe_date(getattr(filing, "filing_date", ""))
    if fd:
        return f"{safe_ticker}_{safe_form}_{fd}.md"
    # Last resort: accession number (guaranteed unique)
    acc = str(getattr(filing, "accession_number", "unknown"))
    return f"{safe_ticker}_{safe_form}_{acc}.md"


def _build_frontmatter(filing, ticker: str, cik: str) -> str:
    fd = _safe_date(getattr(filing, "filing_date", ""))
    try:
        por = _safe_date(getattr(filing, "period_of_report", ""))
    except Exception:
        por = ""
    form = str(getattr(filing, "form", "10-K") or "10-K").upper()
    src = (
        getattr(filing, "filing_url", None)
        or getattr(filing, "url", None)
        or getattr(filing, "link", None)
    )
    esc = lambda v: json.dumps(str(v), ensure_ascii=False)

    lines = [
        "---",
        f"company: {esc(getattr(filing, 'company', ''))}",
        f"ticker: {esc(ticker)}",
        f"cik: {esc(cik)}",
        f"form_type: {esc(form)}",
        f"filing_date: {esc(fd)}",
    ]
    if por:
        lines.append(f"period_of_report: {esc(por)}")
    if src:
        lines.append(f"source_url: {esc(src)}")
    lines.extend(["---", ""])
    return "\n".join(lines)


def _fetch_filing_html(filing) -> str | None:
    """Fetch raw HTML directly from SEC, bypassing edgartools' parser."""
    import httpx

    url = getattr(filing, "filing_url", None)
    if not url or not url.endswith((".htm", ".html")):
        return None
    identity = os.environ.get("EDGAR_IDENTITY", "Ragora contact@ragora.app")
    resp = httpx.get(url, headers={"User-Agent": identity},
                     follow_redirects=True, timeout=60)
    if resp.status_code == 200 and "html" in resp.headers.get("content-type", ""):
        return resp.text
    return None


def download_one(filing, output_dir: Path, idx: int, total: int,
                 delay: float = 0.2, max_retries: int = 3):
    """Download a single filing to disk. Returns (status, filename, error)."""
    cik = str(filing.cik).zfill(10)
    ticker = _resolve_ticker(filing)
    form = str(getattr(filing, "form", "10-K") or "10-K").upper()
    filename = _make_filename(ticker, form, filing)

    ticker_dir = output_dir / ticker.replace("/", "_").replace("\\", "_")
    ticker_dir.mkdir(parents=True, exist_ok=True)
    filepath = ticker_dir / filename

    if filepath.exists():
        print(f"  [{idx}/{total}] {ticker} {form} - {filing.company} (exists, skipped)")
        return "skipped", filename, None

    print(f"  [{idx}/{total}] {ticker} {form} - {filing.company}...", end=" ", flush=True)
    try:
        def fetch():
            _throttle(delay)
            html = _fetch_filing_html(filing)
            if html:
                return _sec_parser.convert(html, filing_meta={
                    "company": str(getattr(filing, "company", "")),
                    "ticker": ticker,
                    "form_type": form,
                })
            return filing.markdown()

        content = _retry(f"download {ticker} {form}", fetch, max_retries)
        if not content:
            print("empty")
            return "failed", filename, "empty content"

        filepath.write_text(_build_frontmatter(filing, ticker, cik) + content, encoding="utf-8")
        print(f"OK ({len(content):,} chars)")
        return "success", filename, None
    except Exception as e:
        print(f"error: {e}")
        return "failed", filename, str(e)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Download SEC filings as markdown")
    parser.add_argument("--limit", type=int, default=0, help="Max filings (0 = all)")
    parser.add_argument("--output", default="data/edgar_filings", help="Output directory")
    parser.add_argument("--months", type=int, default=6, help="Months back to scan")
    parser.add_argument("--include-10q", action="store_true", help="Also download 10-Q filings")
    parser.add_argument("--update", action="store_true", help="Incremental scan since last run")
    parser.add_argument("--retry", action="store_true", help="Retry failed downloads only")
    parser.add_argument("--rescan", action="store_true", help="Force full re-scan of index")
    parser.add_argument("--reset", action="store_true", help="Delete state and start fresh")
    parser.add_argument("--delay", type=float, default=0.2, help="Delay between API calls (s)")
    parser.add_argument("--max-retries", type=int, default=3, help="Max retries per call")
    parser.add_argument("--index-filter", type=str, default="sp500",
                        help="Only download companies in these indices. Comma-separated: sp500, dow. "
                             "Can also be a path to a file with one ticker per line. Default: sp500")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _handle_sigint)

    from edgar import set_identity
    identity = os.environ.get("EDGAR_IDENTITY", "Ragora contact@ragora.app")
    set_identity(identity)
    print(f"EDGAR identity: {identity}")

    form_types = ["10-K"]
    if args.include_10q:
        form_types.append("10-Q")
    print(f"Form types: {', '.join(form_types)}")

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {output_dir.resolve()}")

    state_path = output_dir / ".download_state.json"
    if args.reset:
        if state_path.exists():
            state_path.unlink()
            print("State file deleted.")
    state = load_state(state_path)
    if state["filings"]:
        print(f"Loaded state: {len(state['filings'])} filing(s) tracked")

    # --- Resolve index filter ---
    index_filter = (args.index_filter or "").strip().lower()
    allowed_tickers: set[str] | None = None
    if index_filter:
        allowed_tickers = resolve_index_tickers(index_filter)
        if allowed_tickers:
            print(f"Index filter ({index_filter}): {len(allowed_tickers)} tickers")
        else:
            print(f"Warning: Could not fetch tickers for '{index_filter}', downloading all")

    # --- Phase 1: Scan EDGAR index ---
    prev = state.get("scan_params", {})
    params_changed = (
        prev.get("months_back") != args.months
        or prev.get("form_types") != form_types
        or prev.get("index_filter") != index_filter
        or (prev.get("limit") and not args.limit)
    )
    need_scan = args.rescan or args.update or not state["filings"] or params_changed

    if need_scan:
        # When filter changes, clear old state to avoid stale unfiltered filings
        if params_changed and state["filings"]:
            print("  Scan parameters changed — clearing cached index.")
            state["filings"] = {}

        if allowed_tickers:
            print(f"\nPhase 1: Scanning EDGAR for {len(allowed_tickers)} tickers (last {args.months} months)...")
            index = build_filing_index_by_tickers(
                allowed_tickers, months_back=args.months, limit=args.limit,
                delay=args.delay, max_retries=args.max_retries, form_types=form_types,
            )
        else:
            start_from = None
            if args.update and state.get("last_index_scan"):
                start_from = state["last_index_scan"]
                print(f"\nPhase 1: Incremental scan (since {start_from[:10]})...")
            else:
                print(f"\nPhase 1: Scanning EDGAR index (last {args.months} months)...")

            index = build_filing_index(
                months_back=args.months, limit=args.limit, start_from=start_from,
                delay=args.delay, max_retries=args.max_retries, form_types=form_types,
            )

        if not index and not state["filings"]:
            print("No filings found.")
            sys.exit(1)

        new_count = merge_index_into_state(state, index)
        state["last_index_scan"] = datetime.now().isoformat()
        state["scan_params"] = {"months_back": args.months, "limit": args.limit,
                                "form_types": form_types, "index_filter": index_filter}
        save_state(state, state_path)
        print(f"  Index: {len(state['filings'])} total ({new_count} new)")
    else:
        print("\nPhase 1: Skipped (cached). Use --rescan or --update to refresh.")

    # --- Check disk for files already downloaded (handles missing/reset state) ---
    marked = mark_already_downloaded(state, output_dir)
    if marked:
        print(f"  Found {marked} filing(s) already on disk, marked as done.")
        save_state(state, state_path)

    # --- Phase 2: Download ---
    if args.retry:
        to_process = {a: i for a, i in state["filings"].items() if i["status"] == "failed"}
        print(f"\nRetry mode: {len(to_process)} failed filing(s)")
    else:
        to_process = {a: i for a, i in state["filings"].items() if i["status"] in ("pending", "failed")}

    # Filter Phase 2 to only allowed tickers (handles cached state from prior unfiltered runs)
    if allowed_tickers:
        before = len(to_process)
        # Build set of CIKs belonging to allowed tickers from already-resolved filings
        allowed_ciks: set[str] = set()
        for info in state["filings"].values():
            t = info.get("ticker", "").upper().replace(".", "-")
            if t and t in allowed_tickers:
                allowed_ciks.add(info.get("cik", ""))
        # Keep filings whose ticker matches, or whose CIK matches a known allowed ticker
        filtered = {}
        for a, info in to_process.items():
            t = info.get("ticker", "").upper().replace(".", "-")
            if t and t in allowed_tickers:
                filtered[a] = info
            elif info.get("cik", "") in allowed_ciks:
                filtered[a] = info
        to_process = filtered
        if before != len(to_process):
            print(f"  Index filter: {len(to_process)}/{before} filings match allowed tickers")

    if args.limit:
        to_process = dict(list(to_process.items())[:args.limit])

    total = len(to_process)
    if total == 0:
        done = sum(1 for i in state["filings"].values() if i["status"] == "success")
        print(f"\nNothing to download. {done} filing(s) already done.")
        return

    print(f"\nPhase 2: Downloading {total} filing(s)...")
    ok = skip = fail = 0
    t0 = time.time()

    for i, (acc, info) in enumerate(to_process.items(), 1):
        if _shutdown_requested:
            print(f"\nStopping early ({i-1}/{total} processed).")
            break

        cik = info.get("cik", "")
        ft = info.get("form_type", "10-K")

        try:
            from edgar import Company

            def fetch_filings():
                _throttle(args.delay)
                return Company(cik).get_filings(form=ft)

            filings = _retry(f"lookup CIK {cik} {ft}", fetch_filings, args.max_retries)

            # Match by accession number, then date, then latest
            filing = None
            for f in filings:
                if str(f.accession_number) == acc:
                    filing = f
                    break
            if not filing:
                target = _safe_date(info.get("filing_date", ""))
                for f in filings:
                    if _safe_date(getattr(f, "filing_date", "")) == target:
                        filing = f
                        break
            if not filing:
                filing = filings[0] if filings else None

            if not filing:
                print(f"  [{i}/{total}] CIK {cik} - not found, skipping")
                info.update(status="failed", error="not found on EDGAR", updated_at=datetime.now().isoformat())
                fail += 1
                continue

            result, filename, error = download_one(
                filing, output_dir, i, total, delay=args.delay, max_retries=args.max_retries,
            )
            info["filename"] = filename
            info["error"] = error
            info["company"] = str(getattr(filing, "company", info.get("company", "")))
            info["ticker"] = _resolve_ticker(filing)
            info["filing_date"] = _safe_date(getattr(filing, "filing_date", ""))
            info["accession_number"] = str(getattr(filing, "accession_number", ""))
            info["updated_at"] = datetime.now().isoformat()

            if result == "skipped":
                info["status"] = "success"
                skip += 1
            elif result == "success":
                info["status"] = "success"
                ok += 1
            else:
                info["status"] = "failed"
                fail += 1

        except Exception as e:
            fail += 1
            info.update(status="failed", error=str(e), updated_at=datetime.now().isoformat())
            print(f"  [{i}/{total}] CIK {cik} - error: {e}")

        if i % 10 == 0:
            save_state(state, state_path)

    save_state(state, state_path)

    elapsed = time.time() - t0
    s = sum(1 for i in state["filings"].values() if i["status"] == "success")
    f = sum(1 for i in state["filings"].values() if i["status"] == "failed")
    p = sum(1 for i in state["filings"].values() if i["status"] == "pending")
    print(f"\nDone in {elapsed:.0f}s. This run: {ok} downloaded, {skip} skipped, {fail} failed")
    print(f"Overall: {s} success, {f} failed, {p} pending")
    print(f"State: {state_path.resolve()}")
    print(f"Files: {output_dir.resolve()}")


if __name__ == "__main__":
    main()
