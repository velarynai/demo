import { RagoraClient, type Collection } from 'ragora';

let clientInstance: RagoraClient | null = null;

export function getClient(): RagoraClient {
  if (!clientInstance) {
    const apiKey = process.env.RAGORA_API_KEY;
    if (!apiKey) {
      throw new Error('RAGORA_API_KEY environment variable is required');
    }
    clientInstance = new RagoraClient({
      apiKey,
      baseUrl: process.env.RAGORA_BASE_URL ?? 'https://api.ragora.app',
      timeout: 300_000, // 5 minutes - agent streams can be long
    });
  }
  return clientInstance;
}

export interface DemoCollection {
  id: string;
  ref: string;
  name: string;
  slug?: string;
  description?: string;
  totalDocuments: number;
}

function parseCsv(value?: string): string[] {
  if (!value) return [];
  return [...new Set(value.split(',').map((item) => item.trim()).filter(Boolean))];
}

function getConfiguredCollectionRefs(): string[] {
  return parseCsv(process.env.RAGORA_COLLECTION_IDS);
}

function toDemoCollection(collection: Collection, ref: string): DemoCollection {
  return {
    id: collection.id,
    ref,
    name: collection.name,
    slug: collection.slug,
    description: collection.description,
    totalDocuments: collection.totalDocuments,
  };
}

let configuredCollectionsCache: DemoCollection[] | null = null;
let configuredCollectionsPromise: Promise<DemoCollection[]> | null = null;

async function loadConfiguredCollections(): Promise<DemoCollection[]> {
  const refs = getConfiguredCollectionRefs();
  if (refs.length === 0) {
    throw new Error('Set RAGORA_COLLECTION_IDS (comma-separated) in your .env file');
  }

  const client = getClient();

  return Promise.all(
    refs.map(async (ref) => {
      const collection = await client.getCollection(ref);
      return toDemoCollection(collection, ref);
    }),
  );
}

export async function getConfiguredCollections(): Promise<DemoCollection[]> {
  if (configuredCollectionsCache) {
    return configuredCollectionsCache;
  }

  if (!configuredCollectionsPromise) {
    configuredCollectionsPromise = loadConfiguredCollections()
      .then((collections) => {
        configuredCollectionsCache = collections;
        return collections;
      })
      .finally(() => {
        configuredCollectionsPromise = null;
      });
  }

  return configuredCollectionsPromise;
}

export const SYSTEM_PROMPT = [
  // 1. Unit Scale — prevent misreading "in billions/millions" table headers
  'When answering questions involving numeric data from tables or reports, check if the source specifies a unit scale (e.g., "in millions", "in billions", "in thousands"). If so, apply that scale to all numeric values. Treat commas in numbers as thousands separators (e.g., 13,894 means thirteen thousand eight hundred ninety-four), not decimal points. State final numbers unambiguously (e.g., "$13,894 billion" or "$13.9 trillion").',
  // 2. GAAP vs. Non-GAAP — never silently substitute adjusted metrics
  'You must strictly distinguish between GAAP and Non-GAAP financial measures. If a user asks for standard metrics like "Net Income", "Earnings", or "Operating Margin", you must extract the GAAP metric from the Consolidated Statement of Income. If you extract a Non-GAAP metric (e.g., Adjusted EBITDA, Free Cash Flow, Non-GAAP EPS), you must explicitly label it as "Non-GAAP" in your response. Never substitute a Non-GAAP metric for a GAAP metric without explicitly warning the user.',
  // 3. Temporal Isolation — never confuse quarterly vs. annual columns
  'Strictly isolate time periods. Before extracting a number from a table, verify whether the column header specifies "Three Months Ended" (Quarterly data) or "Year Ended / Twelve Months Ended" (Annual data). If the user asks for annual results, you are forbidden from extracting data from a quarterly column, and vice versa. Always state the exact time period associated with your extracted number.',
  // 4. Consolidated vs. Segment — never present a segment sub-total as consolidated
  'Maintain structural hierarchy between Consolidated totals and Segment results. If a query asks for the company\'s total revenue or total operating income, you must extract the "Consolidated" metric. If extracting a segment or business unit metric, you must explicitly name the segment. Never present a segment sub-total as the consolidated total for the entire company.',
  // 5. Forward-Looking vs. Historical — never present guidance as realized data
  'Differentiate historical facts from management projections. Before extracting an expected metric, check if the text falls under "Outlook", "Guidance", or "Forward-Looking Statements". If the data is a projection, you must prepend your answer with "Management estimates" or "Projected". Never present future guidance as realized historical data.',
].join('\n\n');
