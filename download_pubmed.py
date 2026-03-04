#!/usr/bin/env python3
"""Download PubMed articles as clean markdown from NCBI.

Usage:
    python download_pubmed.py                                    # All categories
    python download_pubmed.py --category glp1                    # Single category
    python download_pubmed.py --category glp1 --category ai_ml   # Multiple categories
    python download_pubmed.py --list-categories                  # Show available categories
    python download_pubmed.py --query '"mRNA Vaccines"[mesh]'    # Custom query (ignores categories)
    python download_pubmed.py --limit 50                         # Override per-category limit
    python download_pubmed.py --abstract-only                    # Skip full-text (faster)
    python download_pubmed.py --retry                            # Retry failed downloads
    python download_pubmed.py --reset                            # Start fresh

Requires: pip install biopython requests
Optional: set NCBI_API_KEY env var for faster rate limits (10 req/s vs 3 req/s).
"""

import argparse
import json
import math
import os
import signal
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from Bio import Entrez

_shutdown_requested = False


def _handle_sigint(signum, frame):
    global _shutdown_requested
    if _shutdown_requested:
        print("\nForce exit.")
        sys.exit(1)
    _shutdown_requested = True
    print("\nInterrupt received — finishing current item and saving state...")


# ---------------------------------------------------------------------------
# Categories — each is a named PubMed query with a default article limit.
# Add new categories here. All queries filter to open-access 2025+ articles.
# ---------------------------------------------------------------------------

_OA_2025 = 'AND pubmed pmc open access[filter] AND "2025":"3000"[dp]'

CATEGORIES: dict[str, dict] = {
    "glp1": {
        "name": "GLP-1 / Semaglutide",
        "query": (
            '("Glucagon-Like Peptide-1 Receptor"[mesh] OR "semaglutide"[tiab] '
            f'OR "GLP-1 receptor agonist"[tiab]) {_OA_2025}'
        ),
        "limit": 1000,
    },
    "ai_ml": {
        "name": "AI/ML in Clinical Medicine",
        "query": (
            '("Artificial Intelligence"[mesh] OR "Machine Learning"[mesh] '
            'OR "Deep Learning"[mesh] OR "large language model"[tiab] '
            'OR "clinical decision support"[tiab]) '
            'AND ("diagnosis"[mesh] OR "prognosis"[mesh] OR "therapeutics"[mesh] '
            f'OR "clinical trial"[tiab] OR "patient outcome"[tiab]) {_OA_2025}'
        ),
        "limit": 1000,
    },
    "immunotherapy": {
        "name": "Cancer Immunotherapy",
        "query": (
            '("Immune Checkpoint Inhibitors"[mesh] OR "Receptors, Chimeric Antigen"[mesh] '
            'OR "CAR-T"[tiab] OR "checkpoint inhibitor"[tiab] '
            'OR "pembrolizumab"[tiab] OR "nivolumab"[tiab] '
            f'OR "immune checkpoint blockade"[tiab]) {_OA_2025}'
        ),
        "limit": 1000,
    },
    "mrna_vaccines": {
        "name": "mRNA Vaccines & Therapeutics",
        "query": (
            '("mRNA Vaccines"[mesh] OR "mRNA vaccine"[tiab] OR "mRNA therapeutic"[tiab] '
            'OR "lipid nanoparticle"[tiab] OR "BNT162"[tiab] OR "mRNA-1273"[tiab] '
            f'OR "self-amplifying RNA"[tiab]) {_OA_2025}'
        ),
        "limit": 1000,
    },
    "obesity": {
        "name": "Obesity & Metabolic Syndrome",
        "query": (
            '("Obesity"[mesh] OR "Metabolic Syndrome"[mesh] '
            'OR "anti-obesity"[tiab] OR "bariatric"[tiab] '
            'OR "tirzepatide"[tiab] OR "weight management"[tiab]) '
            'AND ("clinical trial"[pt] OR "meta-analysis"[pt] OR "randomized controlled trial"[pt] '
            f'OR "systematic review"[pt] OR "review"[pt]) {_OA_2025}'
        ),
        "limit": 1000,
    },
    "alzheimers": {
        "name": "Alzheimer's Disease & Neurodegeneration",
        "query": (
            '("Alzheimer Disease"[mesh] OR "Neurodegenerative Diseases"[mesh] '
            'OR "lecanemab"[tiab] OR "donanemab"[tiab] '
            'OR "amyloid beta"[tiab] OR "tau protein"[tiab] '
            f'OR "cognitive decline"[tiab]) {_OA_2025}'
        ),
        "limit": 1000,
    },
}


# ---------------------------------------------------------------------------
# Impact scoring constants
# ---------------------------------------------------------------------------

PUB_TYPE_SCORES: dict[str, float] = {
    "Meta-Analysis": 1.0,
    "Network Meta-Analysis": 1.0,
    "Systematic Review": 0.85,
    "Guideline": 0.85,
    "Practice Guideline": 0.85,
    "Randomized Controlled Trial": 0.70,
    "Clinical Trial, Phase III": 0.65,
    "Clinical Trial, Phase II": 0.55,
    "Clinical Trial, Phase I": 0.45,
    "Clinical Trial": 0.50,
    "Multicenter Study": 0.40,
    "Observational Study": 0.30,
    "Comparative Study": 0.30,
    "Validation Study": 0.30,
    "Evaluation Study": 0.20,
    "Review": 0.35,
    "Journal Article": 0.10,
    # Negative signals — low-evidence types
    "Case Reports": -0.30,
    "Letter": -0.20,
    "Editorial": -0.20,
    "Comment": -0.25,
    "Preprint": -0.10,
}

TOP_JOURNALS: set[str] = {
    "The New England journal of medicine",
    "The Lancet",
    "JAMA",
    "BMJ (Clinical research ed.)",
    "Nature medicine",
    "Nature",
    "Science",
    "Cell",
    "The Lancet. Oncology",
    "The Lancet. Infectious diseases",
    "The Lancet. Neurology",
    "The Lancet. Respiratory medicine",
    "The Lancet. Diabetes & endocrinology",
    "JAMA internal medicine",
    "JAMA oncology",
    "JAMA neurology",
    "Annals of internal medicine",
    "Circulation",
    "The Journal of clinical investigation",
    "Nature biotechnology",
    "Nature genetics",
    "PLOS medicine",
}

# Lowercase lookup for case-insensitive journal matching
_TOP_JOURNALS_LOWER: set[str] = {j.lower() for j in TOP_JOURNALS}


# ---------------------------------------------------------------------------
# State persistence (same pattern as download_edgar.py)
# ---------------------------------------------------------------------------

def load_state(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    state = {"categories": {}, "articles": {}}
    return state


def save_state(state: dict, path: Path):
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)


# ---------------------------------------------------------------------------
# PubMed search + metadata fetch via Biopython Entrez
# ---------------------------------------------------------------------------

MONTH_MAP = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}


def search_pubmed(query: str, max_results: int, email: str, api_key: str | None) -> list[str]:
    """Search PubMed and return PMIDs."""
    Entrez.email = email
    if api_key:
        Entrez.api_key = api_key

    print(f"  Query: {query[:100]}{'...' if len(query) > 100 else ''}")
    handle = Entrez.esearch(
        db="pubmed", term=query, retmax=max_results, usehistory="y", sort="pub_date",
    )
    results = Entrez.read(handle)
    handle.close()

    pmids = results["IdList"]
    total = int(results["Count"])
    print(f"  Found {total} matches, fetching top {len(pmids)}")
    return pmids


def _extract_electronic_date(article: dict) -> str:
    """Extract the electronic publication date (ArticleDate) — the actual date it went online."""
    for ad in article.get("ArticleDate", []):
        if hasattr(ad, "attributes") and ad.attributes.get("DateType") == "Electronic":
            y = str(ad.get("Year", ""))
            m = str(ad.get("Month", "")).zfill(2)
            d = str(ad.get("Day", "")).zfill(2)
            if y:
                return f"{y}-{m}-{d}"
    return ""


def _extract_journal_date(journal: dict) -> str:
    """Extract the journal issue date (can be a future issue date)."""
    pd = journal.get("JournalIssue", {}).get("PubDate", {})
    year = str(pd.get("Year", ""))
    month = str(pd.get("Month", ""))
    day = str(pd.get("Day", ""))
    if not year:
        return ""
    if not month:
        return year
    m = MONTH_MAP.get(month.lower()[:3], month.zfill(2) if month.isdigit() else "01")
    if not day:
        return f"{year}-{m}"
    return f"{year}-{m}-{day.zfill(2)}"


def fetch_metadata(pmids: list[str], email: str, api_key: str | None) -> list[dict]:
    """Fetch article metadata from PubMed in batches."""
    Entrez.email = email
    if api_key:
        Entrez.api_key = api_key

    articles = []
    batch_size = 50

    for start in range(0, len(pmids), batch_size):
        batch = pmids[start:start + batch_size]
        print(f"  Fetching metadata {start + 1}–{start + len(batch)} of {len(pmids)}...")

        handle = Entrez.efetch(db="pubmed", id=",".join(batch), retmode="xml")
        records = Entrez.read(handle)
        handle.close()

        for record in records.get("PubmedArticle", []):
            medline = record["MedlineCitation"]
            art = medline["Article"]

            # Authors
            authors = []
            for au in art.get("AuthorList", []):
                last = str(au.get("LastName", ""))
                first = str(au.get("ForeName", ""))
                if last:
                    authors.append(f"{last} {first}".strip())

            # Abstract (may be structured)
            abstract_parts = art.get("Abstract", {}).get("AbstractText", [])
            abstract = ""
            if abstract_parts:
                sections = []
                for part in abstract_parts:
                    label = part.attributes.get("Label", "") if hasattr(part, "attributes") else ""
                    text = str(part)
                    sections.append(f"**{label}**: {text}" if label else text)
                abstract = "\n\n".join(sections)

            # Publication types
            pub_types = [str(pt) for pt in art.get("PublicationTypeList", [])]

            # MeSH terms
            mesh_terms = [str(h["DescriptorName"]) for h in medline.get("MeshHeadingList", [])]

            # Publication date — prefer electronic date (actual online date)
            # over journal issue date (can be months in the future)
            journal = art.get("Journal", {})
            pub_date = _extract_electronic_date(art) or _extract_journal_date(journal)

            # DOI
            doi = ""
            for eid in art.get("ELocationID", []):
                if hasattr(eid, "attributes") and eid.attributes.get("EIdType") == "doi":
                    doi = str(eid)

            # PMC ID
            pmc_id = ""
            for aid in record.get("PubmedData", {}).get("ArticleIdList", []):
                if hasattr(aid, "attributes") and aid.attributes.get("IdType") == "pmc":
                    pmc_id = str(aid)

            articles.append({
                "pmid": str(medline["PMID"]),
                "pmc_id": pmc_id,
                "title": str(art.get("ArticleTitle", "")),
                "abstract": abstract,
                "authors": authors,
                "journal": str(journal.get("Title", "")),
                "pub_date": pub_date,
                "doi": doi,
                "mesh_terms": mesh_terms,
                "pub_types": pub_types,
            })

        time.sleep(0.4)

    return articles


# ---------------------------------------------------------------------------
# iCite impact data + scoring
# ---------------------------------------------------------------------------

def fetch_icite(pmids: list[str]) -> dict[str, dict]:
    """Fetch citation metrics from the NIH iCite API.

    Returns {pmid_str: {rcr, citation_count, is_clinical}} for each PMID found.
    Missing PMIDs are silently omitted.
    """
    result: dict[str, dict] = {}
    batch_size = 1000

    for start in range(0, len(pmids), batch_size):
        batch = pmids[start:start + batch_size]
        try:
            resp = requests.get(
                "https://icite.od.nih.gov/api/pubs",
                params={"pmids": ",".join(batch), "fl": "pmid,relative_citation_ratio,citation_count,is_clinical"},
                timeout=30,
            )
            if resp.status_code != 200:
                print(f"  Warning: iCite returned {resp.status_code}, skipping batch")
                continue
            data = resp.json()
            for entry in data.get("data", []):
                pid = str(entry.get("pmid", ""))
                if pid:
                    result[pid] = {
                        "rcr": entry.get("relative_citation_ratio"),
                        "citation_count": entry.get("citation_count"),
                        "is_clinical": entry.get("is_clinical"),
                    }
        except Exception as e:
            print(f"  Warning: iCite request failed ({e}), skipping batch")
            continue

    return result


def compute_impact_score(article: dict, icite: dict) -> dict:
    """Compute a weighted impact score for an article.

    Returns dict with 'total' and individual component scores.
    """
    icite_data = icite.get(article["pmid"], {})

    # --- RCR (weight 0.30) ---
    rcr_raw = icite_data.get("rcr")
    if rcr_raw is not None:
        rcr_score = rcr_raw / (rcr_raw + 1.0)  # sigmoid 0-1
    else:
        rcr_score = 0.5  # neutral default for new papers

    # --- Citation count, log-scaled (weight 0.15) ---
    cites = icite_data.get("citation_count")
    if cites is not None and cites > 0:
        cite_score = math.log2(1 + cites) / math.log2(101)
    else:
        cite_score = 0.3  # neutral default

    # --- Publication type (weight 0.30) ---
    # Use the best (highest) matching type. Negative scores (case reports, etc.)
    # only apply when no positive type is present.
    matched_scores = [PUB_TYPE_SCORES[pt] for pt in article.get("pub_types", []) if pt in PUB_TYPE_SCORES]
    best_pub_type_score = max(matched_scores) if matched_scores else 0.0
    # Clamp to [-1, 1] then map to [0, 1]: -1 -> 0.0, 0 -> 0.5, 1 -> 1.0
    pub_type_score = max(0.0, min(1.0, (best_pub_type_score + 1.0) / 2.0))

    # --- Clinical citation flag (weight 0.10) ---
    is_clinical = icite_data.get("is_clinical")
    clinical_score = 1.0 if is_clinical else (0.5 if is_clinical is None else 0.0)

    # --- Top-tier journal bonus (weight 0.15) ---
    journal = article.get("journal", "")
    journal_score = 1.0 if journal.lower() in _TOP_JOURNALS_LOWER else 0.0

    total = (
        0.30 * rcr_score
        + 0.15 * cite_score
        + 0.30 * pub_type_score
        + 0.10 * clinical_score
        + 0.15 * journal_score
    )

    return {
        "total": round(total, 4),
        "rcr_score": round(rcr_score, 4),
        "cite_score": round(cite_score, 4),
        "pub_type_score": round(pub_type_score, 4),
        "clinical_score": round(clinical_score, 4),
        "journal_score": round(journal_score, 4),
    }


# ---------------------------------------------------------------------------
# Full-text fetch from PMC via BioC API
# ---------------------------------------------------------------------------

def fetch_full_text(pmid: str, pmc_id: str = "") -> list[dict] | None:
    """Fetch full text from PMC BioC API. Returns passage list or None.

    Tries PMID first, then PMC ID as fallback (BioC often needs the PMC ID
    for newer articles).
    """
    ids_to_try = [pmid]
    if pmc_id:
        ids_to_try.append(pmc_id)

    bioc = None
    for article_id in ids_to_try:
        url = f"https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/pmcoa.cgi/BioC_json/{article_id}/unicode"
        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code != 200:
                continue
            candidate = resp.json()
            # Check if this response actually has content (>3 passages)
            colls = candidate if isinstance(candidate, list) else [candidate]
            has_content = any(
                len(d.get("passages", [])) > 3
                for c in colls for d in c.get("documents", [])
            )
            if has_content:
                bioc = candidate
                break
        except Exception:
            continue

    if bioc is None:
        return None

    try:
        # BioC response is a list of collections; each has "documents"
        collections = bioc if isinstance(bioc, list) else [bioc]
        passages = []
        for collection in collections:
            for doc in collection.get("documents", []):
                for passage in doc.get("passages", []):
                    infons = passage.get("infons", {})
                    section = infons.get("section_type", "")
                    ptype = infons.get("type", "")
                    text = passage.get("text", "").strip()
                    if text:
                        entry = {
                            "section": section or ptype,
                            "type": ptype,
                            "text": text,
                        }
                        # Preserve XML for tables (enables proper markdown conversion)
                        if ptype == "table" and infons.get("xml"):
                            entry["xml"] = infons["xml"]
                        passages.append(entry)
        return passages if passages else None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Markdown conversion
# ---------------------------------------------------------------------------

def _xml_table_to_markdown(xml: str) -> str | None:
    """Parse HTML/XML table into markdown. Handles multi-level headers (thead)."""
    try:
        from xml.etree import ElementTree as ET

        root = ET.fromstring(xml)
    except Exception:
        return None

    def _cell_text(cell) -> str:
        return "".join(cell.itertext()).replace("\xa0", " ").replace("\u2009", " ").strip()

    def _parse_rows(parent) -> list[list[str]]:
        rows = []
        for tr in parent.findall("tr"):
            cells = []
            for cell in tr:
                if cell.tag in ("th", "td"):
                    cells.append(_cell_text(cell))
            if cells:
                rows.append(cells)
        return rows

    # Collect header and body rows
    header_rows = []
    thead = root.find("thead")
    if thead is not None:
        header_rows = _parse_rows(thead)

    body_rows = []
    tbody = root.find("tbody")
    if tbody is not None:
        body_rows = _parse_rows(tbody)
    else:
        # Rows directly under <table>
        body_rows = _parse_rows(root)

    all_rows = header_rows + body_rows
    if not all_rows:
        return None

    # Normalize column count
    ncols = max(len(r) for r in all_rows)
    for r in all_rows:
        while len(r) < ncols:
            r.append("")

    lines = []
    # If we have explicit header rows, use first as header
    hdr = all_rows[0]
    lines.append("| " + " | ".join(hdr) + " |")
    lines.append("|" + "|".join("---" for _ in range(ncols)) + "|")
    for row in all_rows[1:]:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def _tsv_to_markdown_table(text: str) -> str:
    """Fallback: convert BioC tab-separated table text to a markdown table.

    BioC tables use \\t between columns and '\\t \\t' between rows.
    """
    text = text.replace("\xa0", " ").replace("\u2009", " ")
    rows = [row.strip() for row in text.split("\t \t") if row.strip()]
    if not rows:
        return text

    parsed = [row.split("\t") for row in rows]
    ncols = max(len(r) for r in parsed)
    for r in parsed:
        while len(r) < ncols:
            r.append("")

    lines = []
    lines.append("| " + " | ".join(c.strip() for c in parsed[0]) + " |")
    lines.append("|" + "|".join("---" for _ in range(ncols)) + "|")
    for row in parsed[1:]:
        lines.append("| " + " | ".join(c.strip() for c in row) + " |")
    return "\n".join(lines)


def article_to_markdown(article: dict, full_text: list[dict] | None, category: str = "") -> str:
    """Convert article metadata + optional full text to markdown with YAML frontmatter."""
    esc = lambda v: json.dumps(str(v), ensure_ascii=False)

    lines = [
        "---",
        f"pmid: {esc(article['pmid'])}",
        f"title: {esc(article['title'])}",
        f"journal: {esc(article['journal'])}",
        f"published: {esc(article['pub_date'])}",
        f"authors: {esc(', '.join(article['authors'][:5]))}",
    ]
    if article["doi"]:
        lines.append(f"doi: {esc(article['doi'])}")
    if article["pmc_id"]:
        lines.append(f"pmc_id: {esc(article['pmc_id'])}")
    if article["mesh_terms"]:
        lines.append(f"mesh_terms: {esc(', '.join(article['mesh_terms'][:10]))}")
    if article.get("pub_types"):
        lines.append(f"pub_types: {esc(', '.join(article['pub_types']))}")
    if article.get("impact_score"):
        score = article["impact_score"]
        lines.append(f"impact_score: {score['total']}")
        lines.append(f"impact_score_breakdown: {json.dumps(score, ensure_ascii=False)}")
    pubmed_url = f"https://pubmed.ncbi.nlm.nih.gov/{article['pmid']}/"
    lines.append(f"source_url: {esc(pubmed_url)}")
    lines.append('domain: "medical"')
    if category:
        lines.append(f"category: {esc(category)}")
    lines.extend(["---", ""])

    # Title
    lines.append(f"# {article['title']}")
    lines.append("")

    if full_text:
        current_section = ""
        for passage in full_text:
            section = passage["section"]
            ptype = passage.get("type", "")
            text = passage["text"]
            # Skip front-matter / title passages (redundant)
            if section.lower() in ("front", "title_1", "title"):
                continue
            if section and section != current_section:
                current_section = section
                lines.append(f"## {section.replace('_', ' ').title()}")
                lines.append("")
            # Convert table data to markdown tables (prefer XML for accuracy)
            if ptype == "table" and ("\t" in text or passage.get("xml")):
                xml = passage.get("xml", "")
                md_table = _xml_table_to_markdown(xml) if xml else None
                if md_table:
                    lines.append(md_table)
                else:
                    lines.append(_tsv_to_markdown_table(text))
            else:
                lines.append(text)
            lines.append("")
    else:
        # Abstract-only fallback
        if article["abstract"]:
            lines.append("## Abstract")
            lines.append("")
            lines.append(article["abstract"])
            lines.append("")
        if article["mesh_terms"]:
            lines.append("## MeSH Terms")
            lines.append("")
            for term in article["mesh_terms"]:
                lines.append(f"- {term}")
            lines.append("")

    return "\n".join(lines)


def _safe_filename(title: str, pmid: str) -> str:
    slug = title[:60].lower()
    slug = "".join(c if c.isalnum() or c in " -" else "" for c in slug)
    slug = slug.strip().replace(" ", "_")[:50]
    return f"PMID{pmid}_{slug}.md"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _resolve_queries(args) -> list[tuple[str, str, int]]:
    """Return list of (category_key, query, limit) tuples to run."""
    if args.query:
        return [("custom", args.query, args.limit or 1000)]

    selected = args.category if args.category else list(CATEGORIES.keys())
    queries = []
    for key in selected:
        if key not in CATEGORIES:
            print(f"Unknown category: {key}")
            print(f"Available: {', '.join(CATEGORIES.keys())}")
            sys.exit(1)
        cat = CATEGORIES[key]
        limit = args.limit if args.limit else cat["limit"]
        queries.append((key, cat["query"], limit))
    return queries


def _download_articles(
    to_process: dict, output_dir: Path, state: dict, state_path: Path,
    abstract_only: bool, api_key: str | None, category: str,
) -> tuple[int, int]:
    """Download full text + save markdown for pending articles. Returns (ok, fail)."""
    total = len(to_process)
    if total == 0:
        return 0, 0

    mode_label = " (abstract only)" if abstract_only else ""
    print(f"\n  Downloading {total} article(s){mode_label}...")
    ok = fail = 0
    delay = 0.35 if api_key else 0.5

    for i, (pmid, info) in enumerate(to_process.items(), 1):
        if _shutdown_requested:
            print(f"\n  Stopping early ({i - 1}/{total} processed).")
            break

        title = info.get("title", "untitled")
        print(f"    [{i}/{total}] PMID {pmid}: {title[:60]}...", end=" ", flush=True)

        try:
            full_text = None
            if not abstract_only:
                full_text = fetch_full_text(pmid, pmc_id=info.get("pmc_id", ""))

            source = "full-text" if full_text else "abstract"
            md = article_to_markdown(info, full_text, category=category)

            filename = _safe_filename(title, pmid)
            (output_dir / filename).write_text(md, encoding="utf-8")

            info["status"] = "success"
            info["filename"] = filename
            info["source"] = source
            info["error"] = None
            ok += 1
            print(f"OK ({source}, {len(md):,} chars)")

        except Exception as e:
            info["status"] = "failed"
            info["error"] = str(e)
            fail += 1
            print(f"error: {e}")

        if i % 10 == 0:
            save_state(state, state_path)
        time.sleep(delay)

    save_state(state, state_path)
    return ok, fail


def main():
    parser = argparse.ArgumentParser(description="Download PubMed articles as markdown")
    parser.add_argument("--category", action="append",
                        help="Category to download (repeatable; default: all). "
                             "Use --list-categories to see options.")
    parser.add_argument("--list-categories", action="store_true",
                        help="List available categories and exit")
    parser.add_argument("--query", type=str, default=None,
                        help="Custom PubMed query (bypasses categories)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Override per-category article limit")
    parser.add_argument("--output", default="data/pubmed_articles", help="Output directory")
    parser.add_argument("--email", default="demo@example.com",
                        help="Email for NCBI Entrez (required by NCBI policy)")
    parser.add_argument("--api-key", default=None,
                        help="NCBI API key (or set NCBI_API_KEY env var)")
    parser.add_argument("--retry", action="store_true", help="Retry failed downloads only")
    parser.add_argument("--reset", action="store_true", help="Delete state and start fresh")
    parser.add_argument("--abstract-only", action="store_true",
                        help="Skip full-text fetch, abstracts only (faster)")
    args = parser.parse_args()

    if args.list_categories:
        print("Available categories:\n")
        for key, cat in CATEGORIES.items():
            print(f"  {key:20s} {cat['name']:40s} (default limit: {cat['limit']})")
        print(f"\nRun all: python download_pubmed.py")
        print(f"Run one: python download_pubmed.py --category {next(iter(CATEGORIES))}")
        return

    signal.signal(signal.SIGINT, _handle_sigint)

    api_key = args.api_key or os.environ.get("NCBI_API_KEY")

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {output_dir.resolve()}")

    state_path = output_dir / ".download_state.json"
    if args.reset:
        if state_path.exists():
            state_path.unlink()
            print("State file deleted.")
    state = load_state(state_path)

    queries = _resolve_queries(args)
    total_ok = total_fail = 0
    t0 = time.time()

    for cat_key, query, limit in queries:
        if _shutdown_requested:
            break

        cat_name = CATEGORIES[cat_key]["name"] if cat_key in CATEGORIES else "Custom"
        print(f"\n{'=' * 60}")
        print(f"Category: {cat_name} [{cat_key}] (limit: {limit})")
        print(f"{'=' * 60}")

        # --- Search & fetch metadata (overfetch 3x, then score & keep top N) ---
        # Skip the expensive search/metadata/scoring phase if this category
        # already has enough articles downloaded (or pending) in state.
        if not args.retry:
            existing_cat = [
                v for v in state["articles"].values()
                if v.get("category") == cat_key and v["status"] in ("success", "pending")
            ]
            if len(existing_cat) >= limit:
                print(f"\n  Already have {len(existing_cat)} articles in state (limit {limit}), skipping search.")
            else:

                overfetch_limit = limit * 3
                print(f"\n  Searching PubMed (overfetch {overfetch_limit} to score top {limit})...")
                pmids = search_pubmed(query, overfetch_limit, args.email, api_key)
                if not pmids:
                    print("  No articles found, skipping.")
                    continue

                print(f"\n  Fetching metadata...")
                articles = fetch_metadata(pmids, args.email, api_key)
                print(f"  Got metadata for {len(articles)} articles")

                # Fetch iCite impact data
                all_pmids = [a["pmid"] for a in articles]
                print(f"\n  Fetching iCite impact data for {len(all_pmids)} articles...")
                icite_data = fetch_icite(all_pmids)
                print(f"  Got iCite data for {len(icite_data)} articles")

                # Score each article
                for art in articles:
                    art["impact_score"] = compute_impact_score(art, icite_data)

                # Sort by impact score descending, keep top N
                articles.sort(key=lambda a: a["impact_score"]["total"], reverse=True)
                articles = articles[:limit]
                print(f"  Kept top {len(articles)} articles by impact score")
                if articles:
                    top = articles[0]["impact_score"]["total"]
                    bottom = articles[-1]["impact_score"]["total"]
                    print(f"  Score range: {top:.4f} (best) — {bottom:.4f} (cutoff)")

                new = 0
                for art in articles:
                    pmid = art["pmid"]
                    if pmid not in state["articles"]:
                        state["articles"][pmid] = {
                            **art, "status": "pending", "error": None, "category": cat_key,
                        }
                        new += 1
                state["categories"][cat_key] = {"query": query, "limit": limit}
                save_state(state, state_path)
                print(f"  {new} new articles added to state")

        # --- Download ---
        if args.retry:
            to_process = {
                k: v for k, v in state["articles"].items()
                if v["status"] == "failed" and v.get("category") == cat_key
            }
            print(f"\n  Retry mode: {len(to_process)} failed article(s)")
        else:
            to_process = {
                k: v for k, v in state["articles"].items()
                if v["status"] in ("pending", "failed") and v.get("category") == cat_key
            }

        if not to_process:
            done = sum(
                1 for v in state["articles"].values()
                if v["status"] == "success" and v.get("category") == cat_key
            )
            print(f"\n  Nothing to download. {done} article(s) already done.")
            continue

        ok, fail = _download_articles(
            to_process, output_dir, state, state_path,
            args.abstract_only, api_key, cat_key,
        )
        total_ok += ok
        total_fail += fail

    elapsed = time.time() - t0
    s = sum(1 for v in state["articles"].values() if v["status"] == "success")
    f = sum(1 for v in state["articles"].values() if v["status"] == "failed")
    p = sum(1 for v in state["articles"].values() if v["status"] == "pending")
    ft = sum(1 for v in state["articles"].values() if v.get("source") == "full-text")
    ab = sum(1 for v in state["articles"].values() if v.get("source") == "abstract")

    print(f"\n{'=' * 60}")
    print(f"Done in {elapsed:.0f}s. This run: {total_ok} downloaded, {total_fail} failed")
    print(f"Overall: {s} success ({ft} full-text, {ab} abstract-only), {f} failed, {p} pending")

    # Per-category breakdown
    for cat_key in {v.get("category", "unknown") for v in state["articles"].values()}:
        cat_arts = [v for v in state["articles"].values() if v.get("category") == cat_key]
        cs = sum(1 for v in cat_arts if v["status"] == "success")
        cf = sum(1 for v in cat_arts if v["status"] == "failed")
        cp = sum(1 for v in cat_arts if v["status"] == "pending")
        label = CATEGORIES[cat_key]["name"] if cat_key in CATEGORIES else cat_key
        print(f"  {label}: {cs} success, {cf} failed, {cp} pending")

    print(f"Files: {output_dir.resolve()}")


if __name__ == "__main__":
    main()
