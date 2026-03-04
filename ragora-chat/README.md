# Ragora Next.js Chat

A stylish multi-collection chat demo powered by [Ragora](https://ragora.app) RAG and the official [`ragora`](https://www.npmjs.com/package/ragora) Node.js SDK.

![Next.js](https://img.shields.io/badge/Next.js-16-black)
![TypeScript](https://img.shields.io/badge/TypeScript-5-blue)
![Tailwind CSS](https://img.shields.io/badge/Tailwind-4-38bdf8)

## Features

- Streaming responses via Server-Sent Events
- Collection sidebar with names and descriptions
- Comma-separated collection config from env
- Per-collection chat sessions when switching between collections
- Redesigned UI with responsive desktop/mobile layout
- Markdown rendering for assistant messages

## Quick Start

### 1. Install dependencies

```bash
npm install
```

### 2. Configure environment

```bash
cp .env.example .env
```

Set your API key and collection list:

```env
RAGORA_API_KEY=sk_live_...
RAGORA_COLLECTION_IDS=collection-one,collection-two
```

`RAGORA_COLLECTION_IDS` can contain IDs or slugs.

### 3. Run the dev server

```bash
npm run dev
```

Open [http://localhost:3000](http://localhost:3000).

## How It Works

```
Browser
  |- GET /api/collections -> configured collection metadata (name/description/status)
  `- POST /api/chat       -> stream response for active collection session

Next.js API Routes
  `- Ragora SDK (create/cache one agent per configured collection)
```

## Project Structure

```
nextjs-chat/
|-- app/
|   |-- api/chat/route.ts         # Streaming chat API route
|   |-- api/collections/route.ts  # Configured collection metadata route
|   |-- globals.css               # Theme + markdown + animations
|   |-- layout.tsx                # Fonts + metadata
|   `-- page.tsx                  # Multi-collection chat UI
|-- lib/
|   `-- ragora.ts                 # Client singleton + collection/agent helpers
|-- .env.example
`-- package.json
```

## Configuration

| Variable | Required | Description |
|----------|----------|-------------|
| `RAGORA_API_KEY` | Yes | Your Ragora API key |
| `RAGORA_COLLECTION_IDS` | Yes | Comma-separated collection IDs/slugs shown in sidebar |
| `RAGORA_BASE_URL` | No | API base URL (defaults to `https://api.ragora.app`) |

## Learn More

- [Ragora Documentation](https://ragora.app/docs)
- [Ragora Node.js SDK](https://github.com/velarynai/ragora-node)
- [Next.js Documentation](https://nextjs.org/docs)
