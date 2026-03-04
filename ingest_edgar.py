#!/usr/bin/env python3
"""Ingest downloaded SEC filings into Ragora with rich retrieval metadata.

Usage:
    export RAGORA_API_KEY="sk_live_..."
    export RAGORA_COLLECTION="sec-filings"

    python ingest_edgar.py                          # Ingest S&P 500 filings (default)
    python ingest_edgar.py --index-filter dow       # Only Dow 30 companies
    python ingest_edgar.py --index-filter sp500,dow # Both indices
    python ingest_edgar.py --input ./my_filings/    # Custom input dir
    python ingest_edgar.py --limit 5                # Test with 5 files
    python ingest_edgar.py --concurrency 5          # 5 concurrent uploads
    python ingest_edgar.py --wait                   # Wait for processing to complete
    python ingest_edgar.py --retry                  # Retry only previously failed uploads
    python ingest_edgar.py --rescan                 # Force API dedup check
    python ingest_edgar.py --reset                  # Delete state and start fresh

Requires: pip install ragora httpx
"""

import argparse
import asyncio
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

from index_filter import resolve_index_tickers, ticker_from_filepath

# Cancellation event for graceful shutdown
_cancel_event: asyncio.Event | None = None


class UploadError(Exception):
    """Raised when direct multipart upload fails."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


def _install_signal_handler(loop: asyncio.AbstractEventLoop):
    """Set cancel event on SIGINT for graceful async shutdown."""
    import signal

    def handler():
        if _cancel_event and not _cancel_event.is_set():
            print("\nInterrupt received — finishing current uploads and saving state...")
            _cancel_event.set()
        else:
            # Second interrupt: force exit
            print("\nForce exit.")
            sys.exit(1)

    loop.add_signal_handler(signal.SIGINT, handler)


def load_state(path: Path) -> dict:
    """Load state from JSON file, or return a fresh state dict."""
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            print(f"Warning: Could not load state file ({e}), starting fresh.")
    return {"version": 1, "collection_slug": None, "uploads": {}}


def save_state(state: dict, path: Path):
    """Atomically write state to JSON file."""
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)


def is_transient_error(e: Exception) -> bool:
    """Check if an error is transient and worth retrying."""
    from ragora import RagoraException

    if isinstance(e, UploadError):
        return e.status_code in (408, 429) or (e.status_code is not None and e.status_code >= 500)
    if isinstance(e, RagoraException):
        return e.status_code is not None and (e.status_code == 429 or e.status_code >= 500)

    # Connection errors, timeouts
    if isinstance(e, (ConnectionError, TimeoutError, OSError, httpx.HTTPError)):
        return True

    error_str = str(e).lower()
    return any(
        keyword in error_str
        for keyword in ("timeout", "connection", "502", "503", "504", "rate limit")
    )


def _parse_frontmatter_scalar(value: str) -> Any:
    """Parse basic YAML scalar types used by demo frontmatter."""
    value = value.strip()
    if not value:
        return ""

    if (value.startswith('"') and value.endswith('"')) or (
        value.startswith("'") and value.endswith("'")
    ):
        value = value[1:-1]

    lower = value.lower()
    if lower == "true":
        return True
    if lower == "false":
        return False

    if re.fullmatch(r"-?\d+", value):
        try:
            return int(value)
        except ValueError:
            return value

    if re.fullmatch(r"-?\d+\.\d+", value):
        try:
            return float(value)
        except ValueError:
            return value

    return value


def split_frontmatter(text: str) -> tuple[dict[str, Any], str]:
    """Parse YAML frontmatter and return (metadata, body_text)."""
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return {}, text

    closing_idx = None
    for idx in range(1, len(lines)):
        if lines[idx].strip() == "---":
            closing_idx = idx
            break

    if closing_idx is None:
        return {}, text

    meta: dict[str, Any] = {}
    for line in lines[1:closing_idx]:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        key, sep, raw_val = stripped.partition(":")
        if not sep:
            continue
        meta[key.strip()] = _parse_frontmatter_scalar(raw_val.strip())

    body = "\n".join(lines[closing_idx + 1 :]).lstrip("\n")
    return meta, body


def _normalize_date(value: Any) -> str | None:
    """Normalize date-like values to YYYY-MM-DD."""
    if value is None:
        return None

    raw = str(value).strip()
    if not raw:
        return None

    if "T" in raw:
        raw = raw.split("T", 1)[0]
    if " " in raw:
        raw = raw.split(" ", 1)[0]

    try:
        return datetime.strptime(raw, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        return None


def _to_iso_start_of_day(date_str: str | None) -> str | None:
    """Convert YYYY-MM-DD to ISO UTC start-of-day string."""
    if not date_str:
        return None
    return f"{date_str}T00:00:00Z"


def _infer_cik_from_filename(filename: str) -> str | None:
    match = re.search(r"_(\d{10})_", filename)
    if not match:
        return None
    return match.group(1)


def _normalize_cik(value: Any) -> str:
    raw = str(value or "").strip()
    digits = "".join(ch for ch in raw if ch.isdigit())
    if not digits:
        return ""
    return digits.zfill(10)


def _infer_filing_date_from_filename(filename: str) -> str | None:
    match = re.search(r"_(\d{4}-\d{2}-\d{2})\.md$", filename)
    if not match:
        return None
    return match.group(1)


def _safe_slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug


def _normalize_tag(value: str) -> str:
    """Normalize tags for consistent filtering."""
    normalized = value.strip().lower()
    normalized = re.sub(r"\s+", "-", normalized)
    normalized = re.sub(r"[^a-z0-9:_\-./]", "", normalized)
    return normalized[:80]


def _clean_metadata(meta: dict[str, Any]) -> dict[str, Any]:
    """Drop empty values while preserving 0/False."""
    cleaned: dict[str, Any] = {}
    for key, value in meta.items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        if isinstance(value, (list, dict)) and not value:
            continue
        cleaned[key] = value
    return cleaned


def _build_ingestion_profile(
    frontmatter: dict[str, Any],
    filepath: Path,
) -> dict[str, Any]:
    """Build enriched upload metadata/tags/versioning for a SEC filing."""
    filename = filepath.name

    ticker = str(frontmatter.get("ticker") or "UNKNOWN").upper().strip()
    company = str(frontmatter.get("company") or "").strip()

    cik = _normalize_cik(frontmatter.get("cik")) or (_infer_cik_from_filename(filename) or "")

    form_type = str(frontmatter.get("form_type") or "10-K").upper().strip()
    accession_number = str(frontmatter.get("accession_number") or "").strip()

    filing_date = _normalize_date(frontmatter.get("filing_date")) or _infer_filing_date_from_filename(filename)
    period_of_report = _normalize_date(frontmatter.get("period_of_report"))

    filing_year = frontmatter.get("filing_year")
    if filing_year is None:
        date_for_year = period_of_report or filing_date
        filing_year = int(date_for_year[:4]) if date_for_year else None
    elif isinstance(filing_year, str) and filing_year.isdigit():
        filing_year = int(filing_year)

    filing_quarter = frontmatter.get("filing_quarter")
    if filing_quarter is None:
        date_for_quarter = period_of_report or filing_date
        if date_for_quarter:
            month = int(date_for_quarter[5:7])
            filing_quarter = ((month - 1) // 3) + 1
    elif isinstance(filing_quarter, str) and filing_quarter.isdigit():
        filing_quarter = int(filing_quarter)

    effective_at = _to_iso_start_of_day(filing_date)
    document_time = _to_iso_start_of_day(filing_date)

    form_token = re.sub(r"[^a-z0-9]+", "-", form_type.lower()).strip("-") or "10-k"
    entity_token = cik or (ticker.lower() if ticker and ticker != "UNKNOWN" else _safe_slug(filepath.stem))
    # Use period_of_report (or filing_date) for unique relative paths.
    # Fiscal year alone causes collisions for quarterly filings (3 10-Qs per year).
    period_label = (period_of_report or filing_date or "").replace("-", "") or "unknown"
    stable_relative_path = f"sec/{entity_token}/{form_token}-{period_label}.md"

    source_url = str(frontmatter.get("source_url") or "").strip()

    tags: list[str] = [
        "sec",
        "edgar",
        f"form:{form_type.lower()}",
    ]
    if ticker and ticker != "UNKNOWN":
        tags.append(f"ticker:{ticker}")
    if cik:
        tags.append(f"cik:{cik}")
    if filing_year:
        tags.append(f"year:{filing_year}")
    if filing_quarter:
        tags.append(f"quarter:q{filing_quarter}")
    if company:
        tags.append(f"company:{_safe_slug(company)}")

    normalized_tags = []
    seen = set()
    for tag in tags:
        norm = _normalize_tag(tag)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        normalized_tags.append(norm)

    metadata = _clean_metadata(
        {
            "document_type": "sec_filing",
            "source": "sec-edgar",
            "sec_form": form_type,
            "company": company,
            "ticker": ticker,
            "cik": cik,
            "filing_date": filing_date,
            "filing_year": filing_year,
            "filing_quarter": filing_quarter,
            "period_of_report": period_of_report,
            "source_url": source_url,
        }
    )

    return {
        "relative_path": stable_relative_path,
        "effective_at": effective_at,
        "document_time": document_time,
        "source_type": "sec_filing",
        "source_name": "sec-edgar",
        "domain": "financial",
        "custom_tags": normalized_tags,
        "metadata": metadata,
    }


async def resolve_collection(client, slug: str) -> str:
    """Get existing collection or create a new one. Returns collection ID."""
    try:
        coll = await client.get_collection(slug)
        print(f"Using existing collection: {coll.name} ({coll.id})")
        return coll.id
    except Exception:
        pass

    print(f"Collection '{slug}' not found, creating...")
    coll = await client.create_collection(name=slug, slug=slug)
    print(f"Created collection: {coll.name} ({coll.id})")
    return coll.id


async def fetch_existing_filenames(client, collection_id: str) -> set[str]:
    """Paginate through all documents to build a set of existing filenames."""
    existing = set()
    offset = 0
    limit = 200
    while True:
        doc_list = await client.list_documents(
            collection_id=collection_id, limit=limit, offset=offset
        )
        for doc in doc_list.data:
            existing.add(doc.filename)
        if not doc_list.has_more:
            break
        offset += limit
    return existing


async def upload_document_with_metadata(
    upload_http: httpx.AsyncClient,
    *,
    collection_id: str,
    filename: str,
    file_content: bytes,
    relative_path: str | None,
    profile: dict[str, Any],
) -> str:
    """Upload via multipart/form-data with metadata support."""
    form_data: dict[str, str] = {
        "collection_id": collection_id,
        "source_type": profile["source_type"],
        "source_name": profile["source_name"],
        "domain": profile["domain"],
        "custom_tags": json.dumps(profile["custom_tags"], ensure_ascii=False),
        "metadata": json.dumps(profile["metadata"], ensure_ascii=False),
    }
    if relative_path:
        form_data["relative_path"] = relative_path
    if profile.get("effective_at"):
        form_data["effective_at"] = profile["effective_at"]
    if profile.get("document_time"):
        form_data["document_time"] = profile["document_time"]

    files = {"file": (filename, file_content, "text/markdown")}

    try:
        response = await upload_http.post(
            "/v1/documents",
            data=form_data,
            files=files,
        )
    except httpx.HTTPError as e:
        raise UploadError(f"Upload transport error: {e}") from e

    if not response.is_success:
        message = response.text
        try:
            payload = response.json()
            if isinstance(payload, dict):
                if isinstance(payload.get("error"), dict):
                    message = payload["error"].get("message") or payload["error"].get("error") or message
                elif payload.get("error"):
                    message = str(payload["error"])
                elif payload.get("message"):
                    message = str(payload["message"])
        except json.JSONDecodeError:
            pass
        raise UploadError(message, status_code=response.status_code)

    try:
        payload = response.json()
    except json.JSONDecodeError as e:
        raise UploadError(f"Invalid JSON response from upload API: {response.text}") from e

    if not isinstance(payload, dict):
        raise UploadError(f"Unexpected upload response format: {payload}")

    document_id = payload.get("id")
    if not document_id and isinstance(payload.get("data"), dict):
        document_id = payload["data"].get("id")

    return str(document_id or "")


async def upload_one(
    client,
    upload_http: httpx.AsyncClient,
    filepath: Path,
    input_dir: Path,
    collection_id: str,
    index: int,
    total: int,
    wait: bool,
    keep_frontmatter: bool,
    semaphore: asyncio.Semaphore,
    max_retries: int = 3,
):
    """Upload a single filing file with retry logic for transient errors."""
    async with semaphore:
        if _cancel_event and _cancel_event.is_set():
            return "cancelled", None, None

        raw_text = filepath.read_text(encoding="utf-8")
        frontmatter, body_text = split_frontmatter(raw_text)
        upload_text = raw_text if keep_frontmatter else (body_text or raw_text)

        profile = _build_ingestion_profile(frontmatter, filepath)

        try:
            relative_path_actual = filepath.relative_to(input_dir).as_posix()
        except ValueError:
            relative_path_actual = filepath.name

        if isinstance(profile.get("metadata"), dict):
            profile["metadata"]["original_relative_path"] = relative_path_actual

        upload_relative_path = str(profile.get("relative_path") or relative_path_actual)

        last_error = None
        for attempt in range(max_retries + 1):
            if _cancel_event and _cancel_event.is_set():
                return "cancelled", None, None

            try:
                try:
                    document_id = await upload_document_with_metadata(
                        upload_http,
                        collection_id=collection_id,
                        filename=filepath.name,
                        file_content=upload_text.encode("utf-8"),
                        relative_path=upload_relative_path,
                        profile=profile,
                    )
                    print(f"  [{index}/{total}] {filepath.name} — uploaded ({document_id or 'pending-id'})")

                    if wait and document_id:
                        status = await client.wait_for_document(document_id)
                        print(
                            f"           → {status.status} ({status.chunk_count} chunks)"
                        )

                    return "uploaded", document_id or None, None

                except UploadError as e:
                    if e.status_code == 409:
                        print(
                            f"  [{index}/{total}] {filepath.name} — skipped (duplicate)"
                        )
                        return "skipped", None, None
                    raise

            except Exception as e:
                last_error = e
                if attempt < max_retries and is_transient_error(e):
                    backoff = 2**attempt  # 1s, 2s, 4s
                    print(
                        f"  [{index}/{total}] {filepath.name} — transient error, retrying in {backoff}s... ({e})"
                    )
                    await asyncio.sleep(backoff)
                    continue
                break

        print(f"  [{index}/{total}] {filepath.name} — error: {last_error}")
        return "failed", None, str(last_error)


async def main():
    global _cancel_event

    parser = argparse.ArgumentParser(
        description="Ingest SEC filings into Ragora"
    )
    parser.add_argument(
        "--input",
        type=str,
        default="data/edgar_filings",
        help="Input directory with .md filing files (default: data/edgar_filings)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max number of files to ingest (0 = all)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=3,
        help="Number of concurrent uploads (default: 3)",
    )
    parser.add_argument(
        "--wait",
        action="store_true",
        help="Wait for each document to finish processing",
    )
    parser.add_argument(
        "--retry",
        action="store_true",
        help="Only process previously failed uploads",
    )
    parser.add_argument(
        "--rescan",
        action="store_true",
        help="Re-upload all files, ignoring both local state and API dedup",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Delete state file and start fresh",
    )
    parser.add_argument(
        "--keep-frontmatter",
        action="store_true",
        help="Upload markdown with YAML frontmatter included (default strips it)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Max retries for transient upload errors (default: 3)",
    )
    parser.add_argument(
        "--index-filter",
        type=str,
        default="sp500",
        help="Only ingest companies in these indices. Comma-separated: sp500, dow (e.g. 'sp500,dow'). Use 'none' to disable. Can also be a path to a file with one ticker per line. Default: sp500",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be ingested without uploading anything",
    )
    parser.add_argument(
        "-y", "--yes",
        action="store_true",
        help="Skip confirmation prompt and proceed immediately",
    )
    args = parser.parse_args()

    # Set up cancellation
    _cancel_event = asyncio.Event()
    try:
        _install_signal_handler(asyncio.get_running_loop())
    except NotImplementedError:
        # Signal handlers not supported on this platform in async context
        pass

    # Validate env vars
    api_key = os.environ.get("RAGORA_API_KEY")
    if not api_key:
        print("Error: RAGORA_API_KEY environment variable is required")
        sys.exit(1)

    collection_slug = os.environ.get("RAGORA_COLLECTION")
    if not collection_slug:
        print("Error: RAGORA_COLLECTION environment variable is required")
        sys.exit(1)

    base_url = (os.environ.get("RAGORA_BASE_URL") or "https://api.ragora.app").rstrip("/")

    # Scan input directory
    input_dir = Path(args.input)
    if not input_dir.is_dir():
        print(f"Error: Input directory not found: {input_dir.resolve()}")
        sys.exit(1)

    files = sorted(input_dir.rglob("*.md"))
    if not files:
        print(f"No .md files found in {input_dir.resolve()}")
        sys.exit(1)

    print(f"Found {len(files)} filing(s) in {input_dir.resolve()}")

    # Filter by index constituents
    index_filter = (args.index_filter or "").strip().lower()
    if index_filter and index_filter != "none":
        allowed_tickers = resolve_index_tickers(index_filter)
        if allowed_tickers:
            before = len(files)
            files = [f for f in files if ticker_from_filepath(f).replace(".", "-") in allowed_tickers]
            print(f"Index filter ({index_filter}): {len(allowed_tickers)} tickers — kept {len(files)}/{before} filings")
        else:
            print(f"Warning: Could not fetch tickers for '{index_filter}', proceeding without filter")

    if args.limit:
        files = files[: args.limit]

    # State management
    state_path = input_dir / ".ingest_state.json"

    if args.reset:
        if state_path.exists():
            state_path.unlink()
            print("State file deleted.")
        state = load_state(state_path)
    else:
        state = load_state(state_path)
        if state["uploads"]:
            print(f"Loaded state: {len(state['uploads'])} upload(s) tracked")

    state["collection_slug"] = collection_slug

    # Initialize client
    from ragora import RagoraClient

    async with RagoraClient(api_key=api_key, base_url=base_url) as client:
        async with httpx.AsyncClient(
            base_url=base_url,
            timeout=120.0,
            headers={
                "Authorization": f"Bearer {api_key}",
                "User-Agent": "ragora-demo-sec-ingest/1.0",
            },
        ) as upload_http:
            # Resolve collection
            collection_id = await resolve_collection(client, collection_slug)

            # Determine dedup strategy
            if args.rescan:
                # --rescan: skip all dedup, re-upload everything
                already_done = set()
                print("Rescan mode: ignoring dedup, all files will be re-uploaded")
            elif (
                state["uploads"]
                and state["collection_slug"] == collection_slug
            ):
                # Use local state for dedup — no API call needed
                already_done = {
                    name
                    for name, info in state["uploads"].items()
                    if info["status"] in ("uploaded", "skipped")
                }
                print(
                    f"Using local state for dedup ({len(already_done)} already processed)"
                )
            else:
                # Fetch from API
                print("Checking for existing documents via API...")
                existing = await fetch_existing_filenames(client, collection_id)
                if existing:
                    print(f"  {len(existing)} document(s) already in collection")
                already_done = set()
                # Mark API-found duplicates in state
                for f in files:
                    if f.name in existing:
                        state["uploads"][f.name] = {
                            "status": "skipped",
                            "document_id": None,
                            "error": None,
                            "updated_at": datetime.now().isoformat(),
                        }
                        already_done.add(f.name)
                save_state(state, state_path)

            # Determine which files to process
            if args.retry:
                new_files = [
                    f
                    for f in files
                    if f.name in state["uploads"]
                    and state["uploads"][f.name]["status"] == "failed"
                ]
                print(f"\nRetry mode: {len(new_files)} failed file(s) to retry")
            else:
                new_files = [f for f in files if f.name not in already_done]

            skipped_dedup = len(files) - len(new_files)
            if skipped_dedup and not args.retry:
                print(f"  Skipping {skipped_dedup} already-processed file(s)")

            if not new_files:
                print("Nothing to upload — all files already processed.")
                save_state(state, state_path)
                return

            # --- Pre-upload summary ---
            total = len(new_files)
            total_bytes = sum(f.stat().st_size for f in new_files)

            # Group by ticker for breakdown
            ticker_counts: dict[str, int] = {}
            ticker_bytes: dict[str, int] = {}
            for f in new_files:
                t = ticker_from_filepath(f)
                ticker_counts[t] = ticker_counts.get(t, 0) + 1
                ticker_bytes[t] = ticker_bytes.get(t, 0) + f.stat().st_size

            def _fmt_size(n: int) -> str:
                if n >= 1 << 30:
                    return f"{n / (1 << 30):.1f} GB"
                if n >= 1 << 20:
                    return f"{n / (1 << 20):.1f} MB"
                if n >= 1 << 10:
                    return f"{n / (1 << 10):.1f} KB"
                return f"{n} B"

            print(f"\n{'=' * 60}")
            print(f"  Ingestion Summary")
            print(f"{'=' * 60}")
            print(f"  Collection:  {collection_slug}")
            print(f"  Files:       {total}")
            print(f"  Total size:  {_fmt_size(total_bytes)}")
            print(f"  Companies:   {len(ticker_counts)}")
            print(f"  Concurrency: {args.concurrency}")
            if skipped_dedup:
                print(f"  Skipped:     {skipped_dedup} (already processed)")
            print(f"{'─' * 60}")

            # Show top companies by file count
            sorted_tickers = sorted(ticker_counts.items(), key=lambda x: x[1], reverse=True)
            show_n = min(15, len(sorted_tickers))
            for ticker, count in sorted_tickers[:show_n]:
                size = _fmt_size(ticker_bytes[ticker])
                print(f"  {ticker:<10} {count:>4} filing(s)  {size:>10}")
            if len(sorted_tickers) > show_n:
                rest_count = sum(c for _, c in sorted_tickers[show_n:])
                rest_bytes = sum(ticker_bytes[t] for t, _ in sorted_tickers[show_n:])
                print(f"  ... and {len(sorted_tickers) - show_n} more companies ({rest_count} filings, {_fmt_size(rest_bytes)})")
            print(f"{'=' * 60}")

            if args.dry_run:
                print("\n[DRY RUN] No files were uploaded.")
                return

            if not args.yes:
                try:
                    answer = input("\nProceed with ingestion? [y/N] ").strip().lower()
                except (EOFError, KeyboardInterrupt):
                    answer = ""
                if answer not in ("y", "yes"):
                    print("Aborted.")
                    return

            print(f"\nUploading {total} file(s) (concurrency={args.concurrency})...\n")

            semaphore = asyncio.Semaphore(args.concurrency)

            # Process sequentially in batches for state persistence
            uploaded = 0
            skipped = skipped_dedup
            failed = 0
            batch_size = args.concurrency * 2

            for batch_start in range(0, total, batch_size):
                if _cancel_event.is_set():
                    print(f"\nStopping early (processed {batch_start}/{total}).")
                    break

                batch = new_files[batch_start : batch_start + batch_size]
                tasks = [
                    upload_one(
                        client,
                        upload_http,
                        f,
                        input_dir,
                        collection_id,
                        batch_start + j,
                        total,
                        args.wait,
                        args.keep_frontmatter,
                        semaphore,
                        max_retries=args.max_retries,
                    )
                    for j, f in enumerate(batch, 1)
                ]
                results = await asyncio.gather(*tasks)

                # Update state for this batch
                for f, (status, doc_id, error) in zip(batch, results):
                    state["uploads"][f.name] = {
                        "status": status,
                        "document_id": doc_id,
                        "error": error,
                        "updated_at": datetime.now().isoformat(),
                    }
                    if status == "uploaded":
                        uploaded += 1
                    elif status == "skipped":
                        skipped += 1
                    elif status == "failed":
                        failed += 1

                # Save state after each batch
                save_state(state, state_path)

            # Final save
            save_state(state, state_path)

            # Summary
            total_uploaded = sum(
                1
                for info in state["uploads"].values()
                if info["status"] == "uploaded"
            )
            total_failed = sum(
                1 for info in state["uploads"].values() if info["status"] == "failed"
            )
            total_skipped = sum(
                1 for info in state["uploads"].values() if info["status"] == "skipped"
            )
            print(
                f"\nDone. This run — Uploaded: {uploaded}, Skipped: {skipped}, Failed: {failed}"
            )
            print(
                f"Overall — Uploaded: {total_uploaded}, Skipped: {total_skipped}, Failed: {total_failed}"
            )
            print(f"State saved to {state_path.resolve()}")


if __name__ == "__main__":
    asyncio.run(main())
