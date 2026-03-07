#!/usr/bin/env python3
"""Download court opinions from CourtListener (SCOTUS + federal appellate courts).

Usage:
    python download_legal.py                          # All courts, last 12 months
    python download_legal.py --courts scotus           # SCOTUS only
    python download_legal.py --courts ca9,ca5          # Specific circuits
    python download_legal.py --list-courts             # Show available courts
    python download_legal.py --months 6                # Last 6 months
    python download_legal.py --limit 10                # Test with 10 opinions
    python download_legal.py --update                  # Resume from last cursor
    python download_legal.py --retry                   # Retry failed downloads
    python download_legal.py --reset                   # Delete state and start fresh

Requires: pip install httpx markdownify
Set COURTLISTENER_TOKEN env var (free API token from courtlistener.com).
"""

import argparse
import json
import os
import re
import signal
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import httpx
from markdownify import markdownify as html_to_md

_shutdown_requested = False

# ---------------------------------------------------------------------------
# Courts
# ---------------------------------------------------------------------------

COURTS: dict[str, str] = {
    "scotus": "Supreme Court of the United States",
    "ca1": "U.S. Court of Appeals for the First Circuit",
    "ca2": "U.S. Court of Appeals for the Second Circuit",
    "ca3": "U.S. Court of Appeals for the Third Circuit",
    "ca4": "U.S. Court of Appeals for the Fourth Circuit",
    "ca5": "U.S. Court of Appeals for the Fifth Circuit",
    "ca6": "U.S. Court of Appeals for the Sixth Circuit",
    "ca7": "U.S. Court of Appeals for the Seventh Circuit",
    "ca8": "U.S. Court of Appeals for the Eighth Circuit",
    "ca9": "U.S. Court of Appeals for the Ninth Circuit",
    "ca10": "U.S. Court of Appeals for the Tenth Circuit",
    "ca11": "U.S. Court of Appeals for the Eleventh Circuit",
    "cadc": "U.S. Court of Appeals for the D.C. Circuit",
    "cafc": "U.S. Court of Appeals for the Federal Circuit",
}

ALL_COURT_CODES = list(COURTS.keys())


def _handle_sigint(signum, frame):
    global _shutdown_requested
    if _shutdown_requested:
        print("\nForce exit.")
        sys.exit(1)
    _shutdown_requested = True
    print("\nInterrupt received — finishing current item and saving state...")


# ---------------------------------------------------------------------------
# State persistence (same pattern as download_edgar.py)
# ---------------------------------------------------------------------------

def load_state(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            print(f"Warning: Could not load state file ({e}), starting fresh.")
    return {"cursors": {}, "clusters": {}}


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


def _retry(action: str, fn, max_retries: int, backoff: float = 1.0):
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
            time.sleep(wait)
    raise RuntimeError(f"{action} failed after {max_retries+1} attempt(s): {last_err}")


def _safe_slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug[:60]


def _api_get(client: httpx.Client, url: str, params: dict | None = None) -> dict:
    """Make an authenticated GET to CourtListener API."""
    resp = client.get(url, params=params)
    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", "60"))
        print(f"\n  Rate limited — waiting {retry_after}s...")
        time.sleep(retry_after)
        resp = client.get(url, params=params)
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Phase 1: Cluster discovery via Search API (fast, indexed)
# ---------------------------------------------------------------------------

def discover_clusters_search(
    client: httpx.Client,
    courts: list[str],
    date_gte: str,
    state: dict,
    delay: float = 0.2,
    max_retries: int = 3,
    limit: int = 0,
    update: bool = False,
) -> int:
    """Use /search/?type=o to discover clusters. Returns count of new clusters added.

    The search API is indexed and ~100x faster than /clusters/ endpoint.
    It queries all courts in a single call via the court= param.
    """
    base_url = "https://www.courtlistener.com/api/rest/v4/search/"
    params = {
        "type": "o",
        "court": ",".join(courts),
        "filed_after": date_gte,
        "order_by": "dateFiled desc",
    }

    # Resume from saved cursor if updating
    cursor_url = None
    if update and "__search__" in state.get("cursors", {}):
        cursor_url = state["cursors"]["__search__"]
        print(f"  Resuming from saved cursor")

    new_count = 0
    page = 0

    while True:
        if _shutdown_requested:
            break

        page += 1

        def fetch(url=cursor_url, p=params):
            _throttle(delay)
            if url:
                return _api_get(client, url)
            return _api_get(client, base_url, params=p)

        try:
            data = _retry(f"search page {page}", fetch, max_retries)
        except Exception as e:
            print(f"  error: {e}")
            break

        results = data.get("results", [])
        if not results:
            break

        for r in results:
            cluster_id = str(r.get("cluster_id") or "")
            if not cluster_id or cluster_id in state["clusters"]:
                continue

            court_id = str(r.get("court_id") or "").strip()
            if court_id not in COURTS:
                continue

            # Extract citation strings from search result
            citation_list = r.get("citation", [])
            citation_strs = citation_list if isinstance(citation_list, list) else []

            # Search API returns sibling_ids (opinion IDs within the cluster)
            sibling_ids = r.get("sibling_ids", [])
            opinion_urls = [
                f"https://www.courtlistener.com/api/rest/v4/opinions/{oid}/"
                for oid in sibling_ids if oid
            ]

            # Panel names from search result
            panel_names = r.get("panel_names", [])
            if not isinstance(panel_names, list):
                panel_names = []

            state["clusters"][cluster_id] = {
                "cluster_id": cluster_id,
                "case_name": str(r.get("caseName") or "").strip(),
                "case_name_short": "",
                "case_name_full": str(r.get("caseNameFull") or "").strip(),
                "court": court_id,
                "court_name": COURTS.get(court_id, court_id),
                "date_filed": str(r.get("dateFiled") or "").strip(),
                "judges": str(r.get("judge") or "").strip(),
                "panel": ", ".join(panel_names),
                "citations": ", ".join(str(c) for c in citation_strs),
                "docket_number": str(r.get("docketNumber") or "").strip(),
                "docket_id": str(r.get("docket_id") or "").strip(),
                "opinion_urls": opinion_urls,
                "precedential_status": str(r.get("status") or "").strip(),
                "nature_of_suit": str(r.get("suitNature") or "").strip(),
                "disposition": "",
                "citation_count": r.get("citeCount") or 0,
                "scdb_id": str(r.get("scdb_id") or "").strip(),
                "scdb_decision_direction": "",
                "scdb_votes_majority": None,
                "scdb_votes_minority": None,
                # Heavy text fields fetched in Phase 2 (cluster detail)
                "syllabus": str(r.get("syllabus") or "").strip(),
                "headnotes": "",
                "procedural_history": str(r.get("procedural_history") or "").strip(),
                "posture": str(r.get("posture") or "").strip(),
                "attorneys": str(r.get("attorney") or "").strip(),
                "status": "pending",
                "filename": "",
                "error": None,
                "updated_at": datetime.now().isoformat(),
            }
            new_count += 1

            if limit and new_count >= limit:
                break

        count_str = f" (count: {data.get('count', '?')})" if page == 1 else ""
        print(f"  Page {page}: {len(results)} results, {new_count} new{count_str}")

        # Save cursor for resumption
        next_url = data.get("next")
        if next_url:
            state["cursors"]["__search__"] = next_url

        if limit and new_count >= limit:
            break
        if not next_url:
            break

        cursor_url = next_url

    return new_count


# ---------------------------------------------------------------------------
# Phase 2: Download opinions
# ---------------------------------------------------------------------------

def _clean_opinion_html(html: str) -> str:
    """Clean CourtListener HTML before markdown conversion.

    html_with_citations wraps plain_text in <pre> with <a> citation links.
    We unwrap <pre> so markdownify doesn't render it as a code block, and
    strip all HTML tags to get clean plain text.
    """
    # Remove <pre> wrapper
    html = re.sub(r"</?pre[^>]*>", "", html)

    # Remove all opening tags (handles multi-line attributes, aria-description, etc.)
    html = re.sub(r"<[a-zA-Z][^>]*>", "", html)

    # Remove all closing tags
    html = re.sub(r"</[a-zA-Z]+>", "", html)

    # Decode common HTML entities
    html = html.replace("&amp;", "&")
    html = html.replace("&lt;", "<")
    html = html.replace("&gt;", ">")
    html = html.replace("&quot;", '"')
    html = html.replace("&#39;", "'")
    html = html.replace("&nbsp;", " ")

    return html


def _clean_plain_text(text: str) -> str:
    """Clean up pre-formatted court opinion plain text.

    Removes excessive indentation, page numbers/headers, and normalizes
    whitespace while preserving paragraph structure.
    """
    lines = text.splitlines()
    cleaned = []
    for line in lines:
        stripped = line.strip()
        # Skip empty lines (preserve as paragraph breaks)
        if not stripped:
            if cleaned and cleaned[-1] != "":
                cleaned.append("")
            continue
        # Skip page number lines (just a number, or "Cite as:" lines)
        if re.match(r"^\d+$", stripped):
            continue
        # Skip repeated case name headers that appear on every page
        if re.match(r"^\d+\s+[A-Z][A-Z\s.]+v\.\s+[A-Z]", stripped):
            continue
        cleaned.append(stripped)

    text = "\n".join(cleaned)
    # Rejoin lines that are part of the same paragraph (not followed by blank line)
    # Court plain text hard-wraps at ~60 chars
    text = re.sub(r"(?<=[a-z0-9,;:\.\)\"\–\-])\n(?=[a-z0-9\(\"A-Z§])", " ", text)
    # Collapse multiple spaces
    text = re.sub(r"  +", " ", text)
    # Collapse excessive blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _extract_opinion_text(opinion_data: dict) -> tuple[str, str]:
    """Extract text from an opinion response. Returns (text, opinion_type)."""
    opinion_type = str(opinion_data.get("type") or "").strip()

    # Try structured HTML fields first (not html_with_citations which is pre-wrapped)
    for field in ("html", "html_lawbox", "html_columbia", "html_anon_2020"):
        html = opinion_data.get(field) or ""
        if html:
            text = html_to_md(html, strip=["img"], heading_style="ATX")
            text = re.sub(r"\n{3,}", "\n\n", text).strip()
            if len(text) > 100:
                return text, opinion_type

    # html_with_citations: pre-wrapped plain text with <a> citation links
    html = opinion_data.get("html_with_citations") or ""
    if html:
        cleaned_html = _clean_opinion_html(html)
        text = _clean_plain_text(cleaned_html)
        if len(text) > 100:
            return text, opinion_type

    # Raw plain text fallback
    plain = opinion_data.get("plain_text") or ""
    if plain:
        text = _clean_plain_text(plain)
        return text, opinion_type

    return "", opinion_type


_OPINION_TYPE_NAMES = {
    "010combined": "Combined Opinion",
    "015unamimous": "Unanimous Opinion",
    "020lead": "Lead Opinion",
    "025plurality": "Plurality Opinion",
    "030concurrence": "Concurrence",
    "035concurrenceinpart": "Concurrence in Part",
    "040dissent": "Dissent",
    "050addendum": "Addendum",
    "060remittitur": "Remittitur",
    "070rehearing": "Rehearing",
    "080onbandon": "On Bandon",
    "090onthemerits": "On the Merits",
}


def _opinion_type_label(raw_type: str) -> str:
    """Human-readable label for opinion type."""
    if raw_type in _OPINION_TYPE_NAMES:
        return _OPINION_TYPE_NAMES[raw_type]
    if raw_type:
        return raw_type.replace("_", " ").title()
    return "Opinion"


def _build_frontmatter(info: dict) -> str:
    esc = lambda v: json.dumps(str(v), ensure_ascii=False)
    lines = [
        "---",
        f"case_name: {esc(info['case_name'])}",
        f"court: {esc(info['court'])}",
        f"court_name: {esc(info['court_name'])}",
        f"date_filed: {esc(info['date_filed'])}",
        f"cluster_id: {esc(info['cluster_id'])}",
    ]
    if info.get("docket_number"):
        lines.append(f"docket_number: {esc(info['docket_number'])}")
    if info.get("judges"):
        lines.append(f"judges: {esc(info['judges'])}")
    if info.get("panel"):
        lines.append(f"panel: {esc(info['panel'])}")
    if info.get("citations"):
        lines.append(f"citations: {esc(info['citations'])}")
    if info.get("precedential_status"):
        lines.append(f"precedential_status: {esc(info['precedential_status'])}")
    if info.get("nature_of_suit"):
        lines.append(f"nature_of_suit: {esc(info['nature_of_suit'])}")
    if info.get("disposition"):
        lines.append(f"disposition: {esc(info['disposition'])}")
    if info.get("posture"):
        lines.append(f"posture: {esc(info['posture'])}")
    if info.get("attorneys"):
        lines.append(f"attorneys: {esc(info['attorneys'])}")
    if info.get("citation_count"):
        lines.append(f"citation_count: {info['citation_count']}")
    if info.get("case_name_short"):
        lines.append(f"case_name_short: {esc(info['case_name_short'])}")
    if info.get("case_name_full"):
        lines.append(f"case_name_full: {esc(info['case_name_full'])}")
    # SCOTUS-specific SCDB fields
    if info.get("scdb_id"):
        lines.append(f"scdb_id: {esc(info['scdb_id'])}")
    if info.get("scdb_decision_direction"):
        lines.append(f"scdb_decision_direction: {esc(info['scdb_decision_direction'])}")
    if info.get("scdb_votes_majority") is not None:
        lines.append(f"scdb_votes_majority: {info['scdb_votes_majority']}")
    if info.get("scdb_votes_minority") is not None:
        lines.append(f"scdb_votes_minority: {info['scdb_votes_minority']}")
    source_url = f"https://www.courtlistener.com/opinion/{info['cluster_id']}/{_safe_slug(info['case_name'])}/"
    lines.append(f"source_url: {esc(source_url)}")
    lines.append('domain: "legal"')
    lines.extend(["---", ""])
    return "\n".join(lines)


def _make_filename(court: str, cluster_id: str, case_name: str) -> str:
    slug = _safe_slug(case_name)[:40]
    return f"{court}_cluster-{cluster_id}_{slug}.md"


def _opinion_id_from_url(url: str) -> str:
    """Extract opinion ID from a CourtListener API URL."""
    # URL like: https://www.courtlistener.com/api/rest/v4/opinions/12345/
    match = re.search(r"/opinions/(\d+)", url)
    if match:
        return match.group(1)
    return ""


def download_one(
    client: httpx.Client,
    info: dict,
    output_dir: Path,
    idx: int,
    total: int,
    delay: float = 0.75,
    max_retries: int = 3,
) -> tuple[str, str, str | None]:
    """Download all opinions for a cluster and combine into one .md file.

    Returns (status, filename, error).
    """
    court = info["court"]
    cluster_id = info["cluster_id"]
    case_name = info["case_name"]
    filename = _make_filename(court, cluster_id, case_name)

    court_dir = output_dir / court
    court_dir.mkdir(parents=True, exist_ok=True)
    filepath = court_dir / filename

    if filepath.exists():
        print(f"  [{idx}/{total}] {case_name[:50]} (exists, skipped)")
        return "skipped", filename, None

    print(f"  [{idx}/{total}] {case_name[:50]}...", end=" ", flush=True)

    try:
        # Fetch each sub-opinion
        opinion_sections = []
        opinion_urls = info.get("opinion_urls", [])

        # If we don't have opinion URLs (e.g. old state), fetch cluster detail
        if not opinion_urls:
            _throttle(delay)
            cluster_url = f"https://www.courtlistener.com/api/rest/v4/clusters/{cluster_id}/"

            def fetch_cluster():
                return _api_get(client, cluster_url)

            cluster_data = _retry(f"cluster {cluster_id}", fetch_cluster, max_retries)

            # Populate any missing fields from detail response
            for field in ("syllabus", "headnotes", "procedural_history", "posture", "attorneys",
                          "disposition", "nature_of_suit", "docket_number"):
                val = str(cluster_data.get(field) or "").strip()
                if val and not info.get(field):
                    info[field] = val

            sub_ops = cluster_data.get("sub_opinions", [])
            for op in sub_ops:
                if isinstance(op, str):
                    opinion_urls.append(op)

        for op_url in opinion_urls:
            if _shutdown_requested:
                return "failed", filename, "shutdown requested"

            op_id = _opinion_id_from_url(op_url)
            if not op_id:
                continue

            def fetch_opinion(url=op_url):
                _throttle(delay)
                # Ensure we use the full API URL
                if url.startswith("/"):
                    url_full = f"https://www.courtlistener.com{url}"
                elif not url.startswith("http"):
                    url_full = f"https://www.courtlistener.com/api/rest/v4/opinions/{url}/"
                else:
                    url_full = url
                return _api_get(client, url_full)

            try:
                op_data = _retry(f"opinion {op_id}", fetch_opinion, max_retries)
                text, op_type = _extract_opinion_text(op_data)
                if text:
                    label = _opinion_type_label(op_type)
                    opinion_sections.append((label, text))
            except Exception as e:
                print(f"(opinion {op_id} failed: {e}) ", end="")

        if not opinion_sections:
            print("empty (no text)")
            return "failed", filename, "no opinion text found"

        # Build markdown
        frontmatter = _build_frontmatter(info)
        body_parts = [f"# {case_name}", ""]

        # Include court-provided summary sections before the opinion text
        if info.get("syllabus"):
            body_parts.append("## Syllabus")
            body_parts.append("")
            body_parts.append(info["syllabus"])
            body_parts.append("")

        if info.get("headnotes"):
            body_parts.append("## Headnotes")
            body_parts.append("")
            body_parts.append(info["headnotes"])
            body_parts.append("")

        if info.get("procedural_history"):
            body_parts.append("## Procedural History")
            body_parts.append("")
            body_parts.append(info["procedural_history"])
            body_parts.append("")

        if len(opinion_sections) == 1:
            body_parts.append(opinion_sections[0][1])
        else:
            for label, text in opinion_sections:
                body_parts.append(f"## {label}")
                body_parts.append("")
                body_parts.append(text)
                body_parts.append("")

        content = frontmatter + "\n".join(body_parts)
        filepath.write_text(content, encoding="utf-8")
        print(f"OK ({len(content):,} chars, {len(opinion_sections)} opinion(s))")
        return "success", filename, None

    except InterruptedError:
        return "failed", filename, "shutdown requested"
    except Exception as e:
        print(f"error: {e}")
        return "failed", filename, str(e)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Download court opinions from CourtListener")
    parser.add_argument("--courts", type=str, default=None,
                        help="Comma-separated court codes (default: all). Use --list-courts to see options.")
    parser.add_argument("--list-courts", action="store_true", help="List available courts and exit")
    parser.add_argument("--months", type=int, default=12, help="Months back to scan (default: 12)")
    parser.add_argument("--limit", type=int, default=0, help="Max clusters per court (0 = all)")
    parser.add_argument("--output", default="data/court_opinions", help="Output directory")
    parser.add_argument("--update", action="store_true", help="Resume from saved cursors")
    parser.add_argument("--retry", action="store_true", help="Retry failed downloads only")
    parser.add_argument("--reset", action="store_true", help="Delete state and start fresh")
    parser.add_argument("--delay", type=float, default=0.75, help="Delay between API calls (s)")
    parser.add_argument("--max-retries", type=int, default=3, help="Max retries per call")
    args = parser.parse_args()

    if args.list_courts:
        print("Available courts:\n")
        for code, name in COURTS.items():
            category = "SCOTUS" if code == "scotus" else "Federal Appellate"
            print(f"  {code:8s}  {name:55s}  [{category}]")
        print(f"\nUsage: python download_legal.py --courts scotus,ca9,ca5")
        return

    # Auth
    token = os.environ.get("COURTLISTENER_TOKEN")
    if not token:
        print("Error: COURTLISTENER_TOKEN environment variable is required")
        print("Get a free token at https://www.courtlistener.com/help/api/rest/")
        sys.exit(1)

    signal.signal(signal.SIGINT, _handle_sigint)

    # Resolve courts
    if args.courts:
        selected_courts = [c.strip().lower() for c in args.courts.split(",")]
        for c in selected_courts:
            if c not in COURTS:
                print(f"Unknown court: {c}")
                print(f"Available: {', '.join(COURTS.keys())}")
                sys.exit(1)
    else:
        selected_courts = ALL_COURT_CODES

    # Date filter
    date_gte = (datetime.now() - timedelta(days=args.months * 30)).strftime("%Y-%m-%d")
    print(f"Courts: {', '.join(selected_courts)}")
    print(f"Date filter: >= {date_gte}")

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {output_dir.resolve()}")

    # State
    state_path = output_dir / ".download_state.json"
    if args.reset:
        if state_path.exists():
            state_path.unlink()
            print("State file deleted.")
    state = load_state(state_path)
    if state["clusters"]:
        print(f"Loaded state: {len(state['clusters'])} cluster(s) tracked")

    # HTTP client
    headers = {
        "Authorization": f"Token {token}",
        "User-Agent": "ragora-demo-legal/1.0 (contact@ragora.app)",
    }
    client = httpx.Client(
        headers=headers,
        timeout=httpx.Timeout(connect=30.0, read=120.0, write=30.0, pool=30.0),
        follow_redirects=True,
    )

    try:
        # --- Phase 1: Discover clusters via Search API ---
        if not args.retry:
            print(f"\nPhase 1: Discovering opinion clusters (search API)...")

            total_new = discover_clusters_search(
                client, selected_courts, date_gte, state,
                delay=args.delay, max_retries=args.max_retries,
                limit=args.limit, update=args.update,
            )
            save_state(state, state_path)

            total = len(state["clusters"])
            print(f"\n  Total: {total} cluster(s) tracked ({total_new} new)")
        else:
            print("\nPhase 1: Skipped (retry mode)")

        # --- Mark files already on disk ---
        existing_files = {f.name for f in output_dir.rglob("*.md")} if output_dir.exists() else set()
        marked = 0
        for info in state["clusters"].values():
            if info["status"] == "pending" and info.get("filename") and info["filename"] in existing_files:
                info["status"] = "success"
                marked += 1
        if marked:
            print(f"  Found {marked} opinion(s) already on disk, marked as done.")
            save_state(state, state_path)

        # --- Phase 2: Download opinions ---
        if args.retry:
            to_process = {
                cid: info for cid, info in state["clusters"].items()
                if info["status"] == "failed"
                and info["court"] in selected_courts
            }
            print(f"\nRetry mode: {len(to_process)} failed cluster(s)")
        else:
            to_process = {
                cid: info for cid, info in state["clusters"].items()
                if info["status"] in ("pending", "failed")
                and info["court"] in selected_courts
            }

        if args.limit:
            to_process = dict(list(to_process.items())[:args.limit])

        total = len(to_process)
        if total == 0:
            done = sum(1 for i in state["clusters"].values() if i["status"] == "success")
            print(f"\nNothing to download. {done} opinion(s) already done.")
            return

        print(f"\nPhase 2: Downloading {total} opinion(s)...")
        ok = skip = fail = 0
        t0 = time.time()

        for i, (cid, info) in enumerate(to_process.items(), 1):
            if _shutdown_requested:
                print(f"\nStopping early ({i-1}/{total} processed).")
                break

            result, filename, error = download_one(
                client, info, output_dir, i, total,
                delay=args.delay, max_retries=args.max_retries,
            )
            info["filename"] = filename
            info["error"] = error
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

            if i % 10 == 0:
                save_state(state, state_path)

        save_state(state, state_path)

        elapsed = time.time() - t0
        s = sum(1 for i in state["clusters"].values() if i["status"] == "success")
        f = sum(1 for i in state["clusters"].values() if i["status"] == "failed")
        p = sum(1 for i in state["clusters"].values() if i["status"] == "pending")
        print(f"\nDone in {elapsed:.0f}s. This run: {ok} downloaded, {skip} skipped, {fail} failed")
        print(f"Overall: {s} success, {f} failed, {p} pending")

        # Per-court breakdown
        court_stats: dict[str, dict[str, int]] = {}
        for info in state["clusters"].values():
            c = info["court"]
            if c not in court_stats:
                court_stats[c] = {"success": 0, "failed": 0, "pending": 0}
            court_stats[c][info["status"]] = court_stats[c].get(info["status"], 0) + 1
        for c in selected_courts:
            if c in court_stats:
                cs = court_stats[c]
                print(f"  {c:8s}: {cs.get('success', 0)} success, {cs.get('failed', 0)} failed, {cs.get('pending', 0)} pending")

        print(f"State: {state_path.resolve()}")
        print(f"Files: {output_dir.resolve()}")

    finally:
        client.close()


if __name__ == "__main__":
    main()
