#!/usr/bin/env python3
"""Ingest downloaded PubMed articles into Ragora with retrieval metadata.

Reuses upload machinery from ingest_edgar.py — only the metadata/tag
building is PubMed-specific.

Usage:
    export RAGORA_API_KEY="sk_live_..."
    export RAGORA_COLLECTION="pubmed-glp1"

    python ingest_pubmed.py                    # Ingest all downloaded articles
    python ingest_pubmed.py --limit 10         # Test with 10 files
    python ingest_pubmed.py --wait             # Wait for processing
    python ingest_pubmed.py --dry-run -y       # Preview what would upload
    python ingest_pubmed.py --retry            # Retry failed uploads

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
# PubMed-specific ingestion profile
# ---------------------------------------------------------------------------

def _build_ingestion_profile(
    frontmatter: dict[str, Any],
    filepath: Path,
) -> dict[str, Any]:
    """Build upload metadata/tags for a PubMed article."""
    pmid = str(frontmatter.get("pmid") or "").strip()
    title = str(frontmatter.get("title") or "").strip()
    journal = str(frontmatter.get("journal") or "").strip()
    pub_date = _normalize_date(frontmatter.get("published"))
    doi = str(frontmatter.get("doi") or "").strip()
    pmc_id = str(frontmatter.get("pmc_id") or "").strip()
    mesh_raw = str(frontmatter.get("mesh_terms") or "").strip()

    effective_at = _to_iso_start_of_day(pub_date)

    # Stable document key: pubmed/{pmid}
    entity_token = pmid or _safe_slug(filepath.stem)
    stable_relative_path = f"pubmed/{entity_token}.md"

    # Parse year from pub_date
    pub_year = None
    if pub_date and len(pub_date) >= 4:
        try:
            pub_year = int(pub_date[:4])
        except ValueError:
            pass

    # Tags
    tags: list[str] = ["pubmed"]
    if pmid:
        tags.append(f"pmid:{pmid}")
    if pmc_id:
        tags.append(f"pmc:{pmc_id.lower()}")
    if journal:
        tags.append(f"journal:{_safe_slug(journal)}")
    if pub_year:
        tags.append(f"year:{pub_year}")
    if doi:
        tags.append(f"doi:{doi}")

    # Add MeSH terms as tags (very useful for filtering)
    if mesh_raw:
        for term in mesh_raw.split(","):
            term = term.strip()
            if term:
                tags.append(f"mesh:{_safe_slug(term)}")

    normalized_tags = []
    seen: set[str] = set()
    for tag in tags:
        norm = _normalize_tag(tag)
        if norm and norm not in seen:
            seen.add(norm)
            normalized_tags.append(norm)

    source_url = str(frontmatter.get("source_url") or "").strip()

    metadata = _clean_metadata({
        "document_type": "pubmed_article",
        "source": "pubmed",
        "pmid": pmid,
        "pmc_id": pmc_id,
        "title": title,
        "journal": journal,
        "published": pub_date,
        "doi": doi,
        "authors": str(frontmatter.get("authors") or "").strip(),
        "mesh_terms": mesh_raw,
        "source_url": source_url,
    })

    return {
        "relative_path": stable_relative_path,
        "effective_at": effective_at,
        "document_time": effective_at,
        "source_type": "pubmed_article",
        "source_name": "pubmed",
        "domain": "medical",
        "custom_tags": normalized_tags,
        "metadata": metadata,
    }


# ---------------------------------------------------------------------------
# Upload one file (mirrors ingest_edgar.upload_one but uses PubMed profile)
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

    parser = argparse.ArgumentParser(description="Ingest PubMed articles into Ragora")
    parser.add_argument("--input", default="data/pubmed_articles", help="Input directory")
    parser.add_argument("--limit", type=int, default=0, help="Max files (0 = all)")
    parser.add_argument("--concurrency", type=int, default=3, help="Concurrent uploads")
    parser.add_argument("--wait", action="store_true", help="Wait for processing")
    parser.add_argument("--retry", action="store_true", help="Retry failed uploads only")
    parser.add_argument("--rescan", action="store_true", help="Re-upload all files, ignoring both local state and API dedup")
    parser.add_argument("--reset", action="store_true", help="Delete state, start fresh")
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--dry-run", action="store_true", help="Preview without uploading")
    parser.add_argument("-y", "--yes", action="store_true", help="Skip confirmation")
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
    print(f"Found {len(files)} article(s) in {input_dir.resolve()}")

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
            headers={"Authorization": f"Bearer {api_key}", "User-Agent": "ragora-demo-pubmed-ingest/1.0"},
        ) as upload_http:
            collection_id = await resolve_collection(client, collection_slug)

            # Dedup
            already_done: set[str] = set()
            if args.rescan:
                # --rescan: skip all dedup, re-upload everything
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
            print(f"\n{'=' * 50}")
            print(f"  PubMed Ingestion Summary")
            print(f"{'=' * 50}")
            print(f"  Collection:  {collection_slug}")
            print(f"  Files:       {total}")
            print(f"  Total size:  {total_bytes / 1024:.0f} KB")
            print(f"  Concurrency: {args.concurrency}")
            print(f"{'=' * 50}")

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
