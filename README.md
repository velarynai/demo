# Ragora Demos

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Example projects showing how to use [Ragora](https://ragora.app) for real-world RAG applications.

## Demos

### [Chat with SEC 10-K Filings](./ragora-chat/)

A full-stack example that downloads SEC 10-K filings from EDGAR, ingests them into Ragora, and provides a chat interface to ask questions about any company’s annual report.

**Stack:** Next.js 16 - Tailwind CSS 4 - Ragora Node SDK - Python

| File | Description |
|------|-------------|
| `download_edgar.py` | Downloads SEC filings (10-K, 10-Q) with index filtering (S&P 500 / Dow 30) |
| `ingest_edgar.py` | Ingests filings with rich metadata/tags/versioning for high-precision retrieval |
| `ragora-chat/` | Next.js chat app with multi-collection sidebar and isolated chat sessions |

## Quick Start

```bash
# 1) Download filings
pip install edgartools
python download_edgar.py --index-filter dow --limit 10

# 2) Ingest into Ragora
pip install ragora httpx
export RAGORA_API_KEY="sk_live_..."
export RAGORA_COLLECTION="sec-filings"
python ingest_edgar.py --wait

# 3) Run the chat app
cd ragora-chat
npm install
cp .env.example .env   # add API key and one or more collection IDs/slugs
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) and ask questions like:
- "What were Apple's revenue segments last year?"
- "Compare Tesla and Ford risk factors."

## Ingestion Quality Defaults

`ingest_edgar.py` optimizes uploads for Ragora retrieval quality by default:

- Strips YAML frontmatter from chunk text (cleaner semantic retrieval)
- Uses stable `document_key` per company (e.g. `sec/0000320193/10-k`)
- Sets filing-based `version` (e.g. `2024` -> `v2024`) for version-aware retrieval
- Sets `effective_at` and `document_time` from filing date
- Sets `domain="financial"` for domain-aware chunking/retrieval
- Sends structured `metadata` (ticker, cik, accession, filing year/quarter/date, form type, etc.)
- Sends high-signal `custom_tags` (`ticker:...`, `cik:...`, `year:...`, `quarter:...`, `form:10-k`, etc.)

This enables precise filtering and much better retrieval targeting.

## Retrieval Best Practices (Ragora)

Use reranking + metadata filters + tags whenever possible:

```python
import asyncio
from ragora import RagoraClient

async def main():
    async with RagoraClient(api_key="...") as client:
        results = await client.search(
            collection_id="sec-filings",
            query="revenue growth in cloud segments",
            top_k=12,
            enable_reranker=True,
            domain=["financial"],
            domain_filter_mode="strict",
            custom_tags=["ticker:aapl", "10-k"],
            filters={
                "filing_year": {"$gte": 2022},
                "sec_form": "10-K",
            },
            version_mode="all",  # use "latest" for newest filing per company
            temporal_filter={"since": "2022-01-01T00:00:00Z"},
        )

        for r in results.results:
            print(r.score, r.metadata.get("ticker"), r.metadata.get("filing_year"))

asyncio.run(main())
```

For chat calls, apply the same `enable_reranker`, `custom_tags`, and `filters` inputs.

## Prerequisites

- [Node.js](https://nodejs.org/) >= 18
- [Python](https://python.org/) >= 3.10
- A [Ragora](https://ragora.app) account and API key

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Links

- [Ragora Documentation](https://ragora.app/docs)
- [Ragora Node SDK](https://github.com/velarynai/ragora-node)
- [Ragora Python SDK](https://pypi.org/project/ragora/)
