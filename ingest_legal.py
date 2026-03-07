#!/usr/bin/env python3
"""Ingest downloaded court opinions into Ragora with retrieval metadata.

Reuses upload machinery from ingest_edgar.py — only the metadata/tag
building is legal-specific.

Usage:
    export RAGORA_API_KEY="sk_live_..."
    export RAGORA_COLLECTION="court-opinions"

    python ingest_legal.py                    # Ingest all downloaded opinions
    python ingest_legal.py --limit 10         # Test with 10 files
    python ingest_legal.py --wait             # Wait for processing
    python ingest_legal.py --dry-run -y       # Preview what would upload
    python ingest_legal.py --retry            # Retry failed uploads
    python ingest_legal.py --courts scotus    # Only ingest SCOTUS opinions

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

# Reuse common upload machinery from the SEC ingest script
from ingest_edgar import (
    UploadError,
    _clean_metadata,
    _install_signal_handler,
    _normalize_date,
    _normalize_tag,
    _safe_slug,
    _to_iso_start_of_day,
    fetch_existing_filenames,
    is_transient_error,
    load_state,
    resolve_collection,
    save_state,
    split_frontmatter,
    upload_document_with_metadata,
)

_cancel_event: asyncio.Event | None = None


# ---------------------------------------------------------------------------
# Legal-specific ingestion profile
# ---------------------------------------------------------------------------

def _build_ingestion_profile(
    frontmatter: dict[str, Any],
    filepath: Path,
) -> dict[str, Any]:
    """Build upload metadata/tags for a court opinion."""
    cluster_id = str(frontmatter.get("cluster_id") or "").strip()
    case_name = str(frontmatter.get("case_name") or "").strip()
    case_name_short = str(frontmatter.get("case_name_short") or "").strip()
    case_name_full = str(frontmatter.get("case_name_full") or "").strip()
    court = str(frontmatter.get("court") or "").strip().lower()
    court_name = str(frontmatter.get("court_name") or "").strip()
    date_filed = _normalize_date(frontmatter.get("date_filed"))
    docket_number = str(frontmatter.get("docket_number") or "").strip()
    judges_raw = str(frontmatter.get("judges") or "").strip()
    panel_raw = str(frontmatter.get("panel") or "").strip()
    citations_raw = str(frontmatter.get("citations") or "").strip()
    source_url = str(frontmatter.get("source_url") or "").strip()
    precedential_status = str(frontmatter.get("precedential_status") or "").strip().lower()
    nature_of_suit = str(frontmatter.get("nature_of_suit") or "").strip()
    disposition = str(frontmatter.get("disposition") or "").strip()
    posture = str(frontmatter.get("posture") or "").strip()
    attorneys = str(frontmatter.get("attorneys") or "").strip()
    citation_count = frontmatter.get("citation_count") or 0
    scdb_id = str(frontmatter.get("scdb_id") or "").strip()
    scdb_decision_direction = str(frontmatter.get("scdb_decision_direction") or "").strip()
    scdb_votes_majority = frontmatter.get("scdb_votes_majority")
    scdb_votes_minority = frontmatter.get("scdb_votes_minority")

    effective_at = _to_iso_start_of_day(date_filed)

    # Stable document key: court_opinion/{court}/{cluster_id}.md
    entity_token = cluster_id or _safe_slug(filepath.stem)
    stable_relative_path = f"court_opinion/{court}/{entity_token}.md"

    # Parse year from date_filed
    filed_year = None
    if date_filed and len(date_filed) >= 4:
        try:
            filed_year = int(date_filed[:4])
        except ValueError:
            pass

    # --- Tags ---
    tags: list[str] = ["court-opinion"]

    # Court tag
    if court:
        tags.append(f"court:{court}")

    # Broad category
    if court == "scotus":
        tags.append("scotus")
    elif court:
        tags.append("federal-appellate")

    # Precedential status (critical for filtering)
    if precedential_status:
        tags.append(f"status:{_safe_slug(precedential_status)}")

    # Year
    if filed_year:
        tags.append(f"year:{filed_year}")

    # Docket number
    if docket_number:
        tags.append(f"docket:{_safe_slug(docket_number)}")

    # Disposition (affirmed, reversed, remanded, etc.)
    if disposition:
        tags.append(f"disposition:{_safe_slug(disposition)}")

    # Nature of suit (legal topic area)
    if nature_of_suit:
        tags.append(f"nos:{_safe_slug(nature_of_suit)}")

    # SCDB decision direction (SCOTUS only: liberal/conservative)
    if scdb_decision_direction:
        tags.append(f"direction:{_safe_slug(scdb_decision_direction)}")

    # Citations
    if citations_raw:
        for cite in citations_raw.split(","):
            cite = cite.strip()
            if cite:
                tags.append(f"citation:{_safe_slug(cite)}")

    # Judges — prefer structured panel, fall back to judges string
    judge_source = panel_raw or judges_raw
    if judge_source:
        for judge in judge_source.split(","):
            judge = judge.strip()
            if judge:
                tags.append(f"judge:{_safe_slug(judge)}")

    # Deduplicate and normalize
    normalized_tags = []
    seen: set[str] = set()
    for tag in tags:
        norm = _normalize_tag(tag)
        if norm and norm not in seen:
            seen.add(norm)
            normalized_tags.append(norm)

    metadata = _clean_metadata({
        "document_type": "court_opinion",
        "source": "courtlistener",
        "cluster_id": cluster_id,
        "case_name": case_name,
        "case_name_short": case_name_short,
        "case_name_full": case_name_full,
        "court": court,
        "court_name": court_name,
        "date_filed": date_filed,
        "docket_number": docket_number,
        "judges": judges_raw,
        "panel": panel_raw,
        "citations": citations_raw,
        "precedential_status": precedential_status,
        "nature_of_suit": nature_of_suit,
        "disposition": disposition,
        "posture": posture,
        "attorneys": attorneys,
        "citation_count": citation_count if citation_count else None,
        "scdb_id": scdb_id,
        "scdb_decision_direction": scdb_decision_direction,
        "scdb_votes_majority": scdb_votes_majority,
        "scdb_votes_minority": scdb_votes_minority,
        "source_url": source_url,
    })

    return {
        "relative_path": stable_relative_path,
        "effective_at": effective_at,
        "document_time": effective_at,
        "source_type": "court_opinion",
        "source_name": "courtlistener",
        "domain": "legal",
        "custom_tags": normalized_tags,
        "metadata": metadata,
    }


# ---------------------------------------------------------------------------
# Upload one file (mirrors ingest_pubmed.upload_one)
# ---------------------------------------------------------------------------

async def upload_one(
    client,
    upload_http: httpx.AsyncClient,
    filepath: Path,
    input_dir: Path,
    collection_id: str,
    index: int,
    total: int,
    wait: bool,
    semaphore: asyncio.Semaphore,
    max_retries: int = 3,
):
    async with semaphore:
        if _cancel_event and _cancel_event.is_set():
            return "cancelled", None, None

        raw_text = filepath.read_text(encoding="utf-8")
        frontmatter, body_text = split_frontmatter(raw_text)
        upload_text = body_text or raw_text

        profile = _build_ingestion_profile(frontmatter, filepath)

        try:
            rel = filepath.relative_to(input_dir).as_posix()
        except ValueError:
            rel = filepath.name
        if isinstance(profile.get("metadata"), dict):
            profile["metadata"]["original_relative_path"] = rel

        upload_relative_path = str(profile.get("relative_path") or rel)

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
                        print(f"           → {status.status} ({status.chunk_count} chunks)")
                    return "uploaded", document_id or None, None
                except UploadError as e:
                    if e.status_code == 409:
                        print(f"  [{index}/{total}] {filepath.name} — skipped (duplicate)")
                        return "skipped", None, None
                    raise
            except Exception as e:
                last_error = e
                if attempt < max_retries and is_transient_error(e):
                    backoff = 2 ** attempt
                    print(f"  [{index}/{total}] {filepath.name} — retrying in {backoff}s... ({e})")
                    await asyncio.sleep(backoff)
                    continue
                break

        print(f"  [{index}/{total}] {filepath.name} — error: {last_error}")
        return "failed", None, str(last_error)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    global _cancel_event

    parser = argparse.ArgumentParser(description="Ingest court opinions into Ragora")
    parser.add_argument("--input", default="data/court_opinions", help="Input directory")
    parser.add_argument("--limit", type=int, default=0, help="Max files (0 = all)")
    parser.add_argument("--concurrency", type=int, default=3, help="Concurrent uploads")
    parser.add_argument("--wait", action="store_true", help="Wait for processing")
    parser.add_argument("--retry", action="store_true", help="Retry failed uploads only")
    parser.add_argument("--rescan", action="store_true", help="Re-upload all files, ignoring dedup")
    parser.add_argument("--reset", action="store_true", help="Delete state, start fresh")
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--dry-run", action="store_true", help="Preview without uploading")
    parser.add_argument("-y", "--yes", action="store_true", help="Skip confirmation")
    parser.add_argument("--courts", type=str, default=None,
                        help="Only ingest opinions from these courts (comma-separated)")
    args = parser.parse_args()

    _cancel_event = asyncio.Event()
    try:
        _install_signal_handler(asyncio.get_running_loop())
    except NotImplementedError:
        pass

    api_key = os.environ.get("RAGORA_API_KEY")
    if not api_key:
        print("Error: RAGORA_API_KEY environment variable is required")
        sys.exit(1)
    collection_slug = os.environ.get("RAGORA_COLLECTION")
    if not collection_slug:
        print("Error: RAGORA_COLLECTION environment variable is required")
        sys.exit(1)
    base_url = (os.environ.get("RAGORA_BASE_URL") or "https://api.ragora.app").rstrip("/")

    input_dir = Path(args.input)
    if not input_dir.is_dir():
        print(f"Error: Input directory not found: {input_dir.resolve()}")
        sys.exit(1)

    files = sorted(input_dir.rglob("*.md"))
    if not files:
        print(f"No .md files found in {input_dir.resolve()}")
        sys.exit(1)
    print(f"Found {len(files)} opinion(s) in {input_dir.resolve()}")

    # Filter by court if specified
    if args.courts:
        selected_courts = {c.strip().lower() for c in args.courts.split(",")}
        before = len(files)
        files = [f for f in files if f.parent.name in selected_courts]
        print(f"Court filter ({args.courts}): kept {len(files)}/{before} files")

    if args.limit:
        files = files[:args.limit]

    # State
    state_path = input_dir / ".ingest_state.json"
    if args.reset:
        if state_path.exists():
            state_path.unlink()
            print("State file deleted.")
    state = load_state(state_path)
    if state["uploads"]:
        print(f"Loaded state: {len(state['uploads'])} upload(s) tracked")
    state["collection_slug"] = collection_slug

    from ragora import RagoraClient

    async with RagoraClient(api_key=api_key, base_url=base_url) as client:
        async with httpx.AsyncClient(
            base_url=base_url, timeout=120.0,
            headers={"Authorization": f"Bearer {api_key}", "User-Agent": "ragora-demo-legal-ingest/1.0"},
        ) as upload_http:
            collection_id = await resolve_collection(client, collection_slug)

            # Dedup
            already_done: set[str] = set()
            if args.rescan:
                print("Rescan mode: ignoring dedup, all files will be re-uploaded")
            elif (
                state["uploads"]
                and state["collection_slug"] == collection_slug
            ):
                already_done = {n for n, info in state["uploads"].items()
                                if info["status"] in ("uploaded", "skipped")}
                print(f"Using local state for dedup ({len(already_done)} already processed)")
            else:
                print("Checking for existing documents via API...")
                existing = await fetch_existing_filenames(client, collection_id)
                if existing:
                    print(f"  {len(existing)} document(s) already in collection")
                for f in files:
                    if f.name in existing:
                        state["uploads"][f.name] = {
                            "status": "skipped", "document_id": None,
                            "error": None, "updated_at": datetime.now().isoformat(),
                        }
                        already_done.add(f.name)
                save_state(state, state_path)

            if args.retry:
                new_files = [f for f in files
                             if f.name in state["uploads"] and state["uploads"][f.name]["status"] == "failed"]
                print(f"\nRetry mode: {len(new_files)} failed file(s)")
            else:
                new_files = [f for f in files if f.name not in already_done]

            skipped_dedup = len(files) - len(new_files)
            if skipped_dedup and not args.retry:
                print(f"  Skipping {skipped_dedup} already-processed file(s)")

            if not new_files:
                print("Nothing to upload — all files already processed.")
                return

            total = len(new_files)
            total_bytes = sum(f.stat().st_size for f in new_files)

            # Group by court for breakdown
            court_counts: dict[str, int] = {}
            court_bytes: dict[str, int] = {}
            for f in new_files:
                c = f.parent.name
                court_counts[c] = court_counts.get(c, 0) + 1
                court_bytes[c] = court_bytes.get(c, 0) + f.stat().st_size

            def _fmt_size(n: int) -> str:
                if n >= 1 << 30:
                    return f"{n / (1 << 30):.1f} GB"
                if n >= 1 << 20:
                    return f"{n / (1 << 20):.1f} MB"
                if n >= 1 << 10:
                    return f"{n / (1 << 10):.1f} KB"
                return f"{n} B"

            print(f"\n{'=' * 60}")
            print(f"  Court Opinion Ingestion Summary")
            print(f"{'=' * 60}")
            print(f"  Collection:  {collection_slug}")
            print(f"  Files:       {total}")
            print(f"  Total size:  {_fmt_size(total_bytes)}")
            print(f"  Courts:      {len(court_counts)}")
            print(f"  Concurrency: {args.concurrency}")
            if skipped_dedup:
                print(f"  Skipped:     {skipped_dedup} (already processed)")
            print(f"{'─' * 60}")

            sorted_courts = sorted(court_counts.items(), key=lambda x: x[1], reverse=True)
            for court_code, count in sorted_courts:
                size = _fmt_size(court_bytes[court_code])
                print(f"  {court_code:<10} {count:>4} opinion(s)  {size:>10}")
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

            print(f"\nUploading {total} file(s)...\n")
            semaphore = asyncio.Semaphore(args.concurrency)
            uploaded = skipped = failed = 0
            batch_size = args.concurrency * 2

            for batch_start in range(0, total, batch_size):
                if _cancel_event.is_set():
                    break
                batch = new_files[batch_start:batch_start + batch_size]
                tasks = [
                    upload_one(
                        client, upload_http, f, input_dir, collection_id,
                        batch_start + j, total, args.wait, semaphore,
                        max_retries=args.max_retries,
                    )
                    for j, f in enumerate(batch, 1)
                ]
                results = await asyncio.gather(*tasks)

                for f, (status, doc_id, error) in zip(batch, results):
                    state["uploads"][f.name] = {
                        "status": status, "document_id": doc_id,
                        "error": error, "updated_at": datetime.now().isoformat(),
                    }
                    if status == "uploaded":
                        uploaded += 1
                    elif status == "skipped":
                        skipped += 1
                    elif status == "failed":
                        failed += 1
                save_state(state, state_path)

            save_state(state, state_path)
            print(f"\nDone. Uploaded: {uploaded}, Skipped: {skipped + skipped_dedup}, Failed: {failed}")
            print(f"State saved to {state_path.resolve()}")


if __name__ == "__main__":
    asyncio.run(main())
