import { getConfiguredCollections } from '@/lib/ragora';

export async function GET() {
  try {
    const collections = await getConfiguredCollections();
    return Response.json({ collections });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Failed to load collections';
    return Response.json({ error: message }, { status: 500 });
  }
}
