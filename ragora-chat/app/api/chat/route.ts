import { getClient, SYSTEM_PROMPT } from '@/lib/ragora';
import type { SearchResult } from 'ragora';

function mapSources(results: SearchResult[]): { title: string; url: string }[] {
  const seen = new Set<string>();
  const sources: { title: string; url: string }[] = [];

  for (const result of results) {
    const meta = result.metadata ?? {};
    const url =
      result.sourceUrl ||
      (meta.source_url as string) ||
      (meta.sourceUrl as string) ||
      (meta.url as string) ||
      '';
    const title =
      (meta.title as string) ||
      (meta.filename as string) ||
      (meta.source as string) ||
      (meta.source_name as string) ||
      url ||
      '';

    if (!title && !url) continue;

    const key = result.documentId || url || title;
    if (seen.has(key)) continue;
    seen.add(key);

    sources.push({ title: title || url, url });
  }

  return sources;
}

export async function POST(request: Request) {
  const { message, sessionId, collectionId } = (await request.json()) as {
    message: string;
    sessionId?: string;
    collectionId?: string;
  };

  if (!message?.trim()) {
    return Response.json({ error: 'Message is required' }, { status: 400 });
  }

  if (!collectionId?.trim()) {
    return Response.json({ error: 'Collection ID is required' }, { status: 400 });
  }

  const client = getClient();
  const encoder = new TextEncoder();

  const stream = new ReadableStream({
    async start(controller) {
      const sendData = (payload: Record<string, unknown>) => {
        controller.enqueue(encoder.encode(`data: ${JSON.stringify(payload)}\n\n`));
      };

      try {
        const chunks = client.chatStream({
          messages: [{ role: 'user', content: message.trim() }],
          retrieval: { collectionId: collectionId.trim() },
          generation: { temperature: 0 },
          agentic: {
            mode: 'agentic',
            session: true,
            ...(sessionId ? { sessionId } : {}),
            systemPrompt: SYSTEM_PROMPT,
          },
        });

        for await (const chunk of chunks) {
          if (chunk.thinking) {
            sendData({
              eventType: 'ragora_status',
              thinking: chunk.thinking,
            });
          }

          if (chunk.content) {
            sendData({
              eventType: 'message',
              content: chunk.content,
            });
          }

          if (chunk.sessionId) {
            sendData({ sessionId: chunk.sessionId });
          }

          if (chunk.sources.length > 0) {
            const sources = mapSources(chunk.sources);
            if (sources.length > 0) {
              sendData({ sources });
            }
          }

          if (chunk.stats) {
            sendData({ done: true });
          }
        }
      } catch (error) {
        const errMsg = error instanceof Error ? error.message : 'Unknown error';
        sendData({ eventType: 'error', error: errMsg });
      }

      controller.enqueue(encoder.encode('data: [DONE]\n\n'));
      controller.close();
    },
  });

  return new Response(stream, {
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
    },
  });
}
