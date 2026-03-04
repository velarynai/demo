'use client';

import React, { useCallback, useEffect, useMemo, useRef, useState, type FormEvent } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import remarkMath from 'remark-math';
import rehypeKatex from 'rehype-katex';
import rehypeRaw from 'rehype-raw';
import 'katex/dist/katex.min.css';

interface Source {
  title: string;
  url?: string;
}

interface ThinkingStep {
  type: string;
  message: string;
  timestamp: number;
}

interface Message {
  role: 'user' | 'assistant';
  content: string;
  sources?: Source[];
  thinkingSteps?: ThinkingStep[];
}

interface CollectionSummary {
  id: string;
  ref: string;
  name: string;
  slug?: string;
  description?: string;
  totalDocuments: number;
}

interface CollectionChatState {
  messages: Message[];
  sessionId?: string;
  draft: string;
}

const SUGGESTIONS = [
  'What is this knowledgebase about?',
  'What are the key topics covered here?',
  'Give me a summary of the most important points.',
  'What can I ask about this collection?',
];

function SparklesIcon() {
  return (
    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round">
      <path d="M12 3l1.9 5.8a2 2 0 001.3 1.3L21 12l-5.8 1.9a2 2 0 00-1.3 1.3L12 21l-1.9-5.8a2 2 0 00-1.3-1.3L3 12l5.8-1.9a2 2 0 001.3-1.3L12 3z" />
    </svg>
  );
}

function SendIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <line x1="22" y1="2" x2="11" y2="13" />
      <polygon points="22 2 15 22 11 13 2 9 22 2" />
    </svg>
  );
}

function PlusIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <line x1="12" y1="5" x2="12" y2="19" />
      <line x1="5" y1="12" x2="19" y2="12" />
    </svg>
  );
}

function MenuIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <line x1="3" y1="6" x2="21" y2="6" />
      <line x1="3" y1="12" x2="21" y2="12" />
      <line x1="3" y1="18" x2="21" y2="18" />
    </svg>
  );
}

function CollectionIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <ellipse cx="12" cy="5" rx="8" ry="3" />
      <path d="M4 5v6c0 1.7 3.6 3 8 3s8-1.3 8-3V5" />
      <path d="M4 11v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6" />
    </svg>
  );
}

function LinkIcon() {
  return (
    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="shrink-0">
      <path d="M18 13v6a2 2 0 01-2 2H5a2 2 0 01-2-2V8a2 2 0 012-2h6" />
      <polyline points="15 3 21 3 21 9" />
      <line x1="10" y1="14" x2="21" y2="3" />
    </svg>
  );
}

function ChevronRightIcon() {
  return (
    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="9 18 15 12 9 6" />
    </svg>
  );
}

function ChevronDownIcon() {
  return (
    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="6 9 12 15 18 9" />
    </svg>
  );
}

function SpinnerIcon() {
  return (
    <svg
      width="12"
      height="12"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      className="animate-spin"
    >
      <line x1="12" y1="2" x2="12" y2="6" />
      <line x1="12" y1="18" x2="12" y2="22" />
      <line x1="4.93" y1="4.93" x2="7.76" y2="7.76" />
      <line x1="16.24" y1="16.24" x2="19.07" y2="19.07" />
      <line x1="2" y1="12" x2="6" y2="12" />
      <line x1="18" y1="12" x2="22" y2="12" />
      <line x1="4.93" y1="19.07" x2="7.76" y2="16.24" />
      <line x1="16.24" y1="7.76" x2="19.07" y2="4.93" />
    </svg>
  );
}

// ── Thinking step icons ───────────────────────────────────────────────

function BrainIcon({ className }: { className?: string }) {
  return (
    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className={className}>
      <path d="M12 5a3 3 0 1 0-5.997.125 4 4 0 0 0-2.526 5.77 4 4 0 0 0 .556 6.588A4 4 0 1 0 12 18Z" />
      <path d="M12 5a3 3 0 1 1 5.997.125 4 4 0 0 1 2.526 5.77 4 4 0 0 1-.556 6.588A4 4 0 1 1 12 18Z" />
      <path d="M15 13a4.5 4.5 0 0 1-3-4 4.5 4.5 0 0 1-3 4" />
      <path d="M17.599 6.5a3 3 0 0 0 .399-1.375" />
      <path d="M6.003 5.125A3 3 0 0 0 6.401 6.5" />
      <path d="M3.477 10.896a4 4 0 0 1 .585-.396" />
      <path d="M19.938 10.5a4 4 0 0 1 .585.396" />
      <path d="M6 18a4 4 0 0 1-1.967-.516" />
      <path d="M19.967 17.484A4 4 0 0 1 18 18" />
    </svg>
  );
}

function SearchStepIcon({ className }: { className?: string }) {
  return (
    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className={className}>
      <circle cx="11" cy="11" r="8" />
      <path d="m21 21-4.3-4.3" />
    </svg>
  );
}

function CheckCircleIcon({ className }: { className?: string }) {
  return (
    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className={className}>
      <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14" />
      <path d="m9 11 3 3L22 4" />
    </svg>
  );
}

function SparklesStepIcon({ className }: { className?: string }) {
  return (
    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className={className}>
      <path d="M12 3l1.9 5.8a2 2 0 001.3 1.3L21 12l-5.8 1.9a2 2 0 00-1.3 1.3L12 21l-1.9-5.8a2 2 0 00-1.3-1.3L3 12l5.8-1.9a2 2 0 001.3-1.3L12 3z" />
    </svg>
  );
}

function WrenchIcon({ className }: { className?: string }) {
  return (
    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className={className}>
      <path d="M14.7 6.3a1 1 0 0 0 0 1.4l1.6 1.6a1 1 0 0 0 1.4 0l3.77-3.77a6 6 0 0 1-7.94 7.94l-6.91 6.91a2.12 2.12 0 0 1-3-3l6.91-6.91a6 6 0 0 1 7.94-7.94l-3.76 3.76z" />
    </svg>
  );
}

// ── Thinking step condensing ──────────────────────────────────────────

type IconComponent = ({ className }: { className?: string }) => React.JSX.Element;

interface DisplayStep {
  icon: IconComponent;
  color: string;
  label: string;
  detail?: string;
}

function truncateQuery(query: string, max = 50): string {
  if (query.length <= max) return query;
  return query.slice(0, max).trimEnd() + '...';
}

function findPairedFound(
  steps: ThinkingStep[],
  fromIndex: number,
): { index: number; count: number } | null {
  for (let j = fromIndex; j < Math.min(fromIndex + 3, steps.length); j++) {
    if (steps[j].type === 'found') {
      const m = steps[j].message.match(/Found (\d+)/);
      if (m) return { index: j, count: parseInt(m[1]) };
    }
    if (
      j > fromIndex &&
      (steps[j].type === 'searching' || steps[j].type === 'generating')
    )
      break;
  }
  return null;
}

function condenseSteps(steps: ThinkingStep[]): DisplayStep[] {
  const display: DisplayStep[] = [];
  const consumed = new Set<number>();
  let i = 0;

  while (i < steps.length) {
    if (consumed.has(i)) {
      i++;
      continue;
    }
    const step = steps[i];

    if (step.type === 'thinking') {
      if (display.length === 0) {
        display.push({
          icon: BrainIcon,
          color: 'text-blue-500',
          label: 'Analyzing question',
        });
      }
      i++;
      continue;
    }

    if (step.type === 'searching') {
      const queryMatch = step.message.match(/^Searching:\s*(.+)$/i);
      const query = queryMatch?.[1];
      const paired = findPairedFound(steps, i + 1);

      if (query) {
        const truncated = truncateQuery(query);
        if (paired) {
          consumed.add(paired.index);
          display.push({
            icon: SearchStepIcon,
            color: 'text-slate-500',
            label: `Searched: ${truncated}`,
            detail: `${paired.count} result${paired.count !== 1 ? 's' : ''}`,
          });
        } else {
          display.push({
            icon: SearchStepIcon,
            color: 'text-slate-500',
            label: `Searching: ${truncated}`,
          });
        }
      } else {
        display.push({
          icon: SearchStepIcon,
          color: 'text-slate-500',
          label: step.message,
        });
      }
      i++;
      continue;
    }

    if (step.type === 'found') {
      const m = step.message.match(/Found (\d+)/);
      const count = m ? parseInt(m[1]) : null;
      display.push({
        icon: CheckCircleIcon,
        color: 'text-emerald-500',
        label:
          count !== null
            ? `Found ${count} result${count !== 1 ? 's' : ''}`
            : step.message,
      });
      i++;
      continue;
    }

    if (step.type === 'generating') {
      display.push({
        icon: SparklesStepIcon,
        color: 'text-violet-500',
        label: 'Generating response',
      });
      i++;
      continue;
    }

    if (step.type === 'warning') {
      i++;
      continue;
    }

    if (step.type === 'working') {
      display.push({
        icon: WrenchIcon,
        color: 'text-slate-500',
        label: step.message,
      });
      i++;
      continue;
    }

    i++;
  }

  return display;
}

// ── ThinkingProcess ───────────────────────────────────────────────────

function ThinkingProcess({
  steps,
  isStreaming,
}: {
  steps: ThinkingStep[];
  isStreaming: boolean;
}) {
  const [isOpen, setIsOpen] = useState(false);
  const displaySteps = useMemo(() => condenseSteps(steps), [steps]);

  useEffect(() => {
    if (isStreaming && steps.length > 0) {
      setIsOpen(true);
    } else if (!isStreaming) {
      setIsOpen(false);
    }
  }, [isStreaming, steps.length > 0]);

  if (displaySteps.length === 0) return null;

  return (
    <div className="mb-2">
      <button
        type="button"
        onClick={() => setIsOpen((prev) => !prev)}
        className="inline-flex items-center gap-1.5 rounded-md px-1 py-0.5 text-[11px] text-slate-500 transition hover:bg-slate-100 hover:text-slate-700"
      >
        {isOpen ? <ChevronDownIcon /> : <ChevronRightIcon />}
        {isStreaming ? (
          <span className="inline-flex items-center gap-1.5">
            <SpinnerIcon />
            {displaySteps[displaySteps.length - 1]?.label ?? 'Working...'}
          </span>
        ) : (
          <span>
            Thought for {displaySteps.length} step
            {displaySteps.length !== 1 ? 's' : ''}
          </span>
        )}
      </button>

      {isOpen ? (
        <div className="ml-1.5 mt-1.5 space-y-0.5 border-l border-slate-200 pl-3">
          {displaySteps.map((step, i) => {
            const Icon = step.icon;
            const isLast = i === displaySteps.length - 1;
            const isAnimating = isStreaming && isLast;

            return (
              <div
                key={i}
                className="flex items-center gap-1.5 py-0.5 text-[11px]"
              >
                <Icon
                  className={`shrink-0 ${step.color}${isAnimating ? ' animate-pulse' : ''}`}
                />
                <span className="text-slate-600">{step.label}</span>
                {step.detail ? (
                  <span className="text-[10px] font-medium text-emerald-600">
                    {step.detail}
                  </span>
                ) : null}
              </div>
            );
          })}
        </div>
      ) : null}
    </div>
  );
}

// ── Citation normalization ────────────────────────────────────────────

function normalizeTableLineBreaks(text: string): string {
  let result = text.replace(
    /(\|[^\n|]+(?:\|[^\n|]+)*\|)[ \t]+(\|)/g,
    '$1\n$2',
  );
  let prev = '';
  while (prev !== result) {
    prev = result;
    result = result.replace(
      /(\|[^\n|]+(?:\|[^\n|]+)*\|)[ \t]+(\|)/g,
      '$1\n$2',
    );
  }
  result = result.replace(
    /([^\n])\n(\|[^\n]+\|\s*\n\|[-| :]+\|)/g,
    '$1\n\n$2',
  );
  return result;
}

function normalizeCitations(text: string): string {
  // Convert OpenAI-style 【N†...】 to [N]
  let normalized = text.replace(/【(\d+)[†\u2020\u2021][^】]*】/g, '[$1]');
  // Strip agent-style 【ref=N】 or [ref=N]
  normalized = normalized.replace(/\s*【ref=\d+】/g, '');
  normalized = normalized.replace(/\s*\[ref=\d+\]/g, '');
  // Strip any remaining fullwidth bracket citations
  normalized = normalized.replace(/\s*【[^】]*】/g, '');
  // Convert [Source N: ...] or [Source N] to [N]
  normalized = normalized.replace(
    /\[Source\s+(\d+)(?::\s*[^\]]+)?\]/gi,
    '[$1]',
  );
  // Escape $ signs to prevent remarkMath from treating currency as LaTeX
  // (e.g. "$383.3 billion" would otherwise be parsed as inline math)
  normalized = normalized.replace(/\$/g, '\\$');
  // Strip LLM-generated sources sections at the end
  normalized = normalized.replace(
    /\n*📚\s*\*{0,2}Sources:?\*{0,2}\s*[\s\S]*$/i,
    '',
  );
  normalized = normalized.replace(
    /\n{2,}\*{0,2}Sources:?\*{0,2}\s*\n\s*(?:\[?\d+\]?[^\n]*\n?)+$/i,
    '',
  );
  return normalized.trimEnd();
}

function processChildrenForCitations(
  children: React.ReactNode,
): React.ReactNode {
  if (typeof children === 'string') {
    const parts = children.split(/(\[\d+\]|\$)/g);
    if (parts.length === 1) return children;

    return parts.map((part, i) => {
      const match = part.match(/^\[(\d+)\]$/);
      if (match) {
        return (
          <span
            key={i}
            className="ml-0.5 inline-flex h-4 w-4 cursor-default items-center justify-center rounded-full bg-[#0f766e] align-super text-[9px] font-bold text-white"
            title={`Source ${match[1]}`}
          >
            {match[1]}
          </span>
        );
      }
      // Wrap $ in a span to prevent cross-DOM-node math pairing
      if (part === '$') {
        return <span key={i}>$</span>;
      }
      return part;
    });
  }

  if (Array.isArray(children)) {
    return children.map((child, i) => (
      <React.Fragment key={i}>
        {processChildrenForCitations(child)}
      </React.Fragment>
    ));
  }

  if (
    React.isValidElement(children) &&
    (children.props as Record<string, unknown>)?.children
  ) {
    return React.cloneElement(children, {
      ...(children.props as Record<string, unknown>),
      children: processChildrenForCitations(
        (children.props as Record<string, unknown>).children as React.ReactNode,
      ),
    } as Record<string, unknown>);
  }

  return children;
}

// ── SourcesList ───────────────────────────────────────────────────────

function SourcesList({
  sources,
  citedNumbers,
}: {
  sources: Source[];
  citedNumbers?: Set<number>;
}) {
  if (sources.length === 0) return null;

  const grouped = new Map<
    string,
    { label: string; url?: string; indices: number[] }
  >();

  sources.forEach((src, i) => {
    const num = i + 1;
    if (citedNumbers && citedNumbers.size > 0 && !citedNumbers.has(num)) return;
    const dedupKey = src.url || src.title;
    const existing = grouped.get(dedupKey);
    if (existing) {
      existing.indices.push(num);
    } else {
      grouped.set(dedupKey, {
        label: src.title,
        url: src.url,
        indices: [num],
      });
    }
  });

  const dedupedSources = Array.from(grouped.values());
  if (dedupedSources.length === 0) return null;

  return (
    <div className="mt-3 border-t border-slate-100 pt-3">
      <p className="mb-1.5 text-[11px] font-semibold uppercase tracking-wider text-slate-400">
        Sources
      </p>
      <div className="flex flex-wrap gap-1.5">
        {dedupedSources.map((group, i) => {
          const chip = (
            <>
              <span className="inline-flex h-4 w-4 shrink-0 items-center justify-center rounded-full bg-[#0f766e] text-[10px] font-bold text-white">
                {group.indices[0]}
              </span>
              <LinkIcon />
              <span className="max-w-[220px] truncate">{group.label}</span>
              {group.indices.length > 1 ? (
                <span className="text-[9px] text-slate-400">
                  ({group.indices.length} chunks)
                </span>
              ) : null}
            </>
          );

          if (group.url) {
            return (
              <a
                key={`${group.url}-${i}`}
                href={group.url}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-1 rounded-lg border border-slate-200 bg-slate-50 px-2 py-1 text-[11px] text-slate-600 transition hover:border-[#0ea5a8]/40 hover:text-[#0f766e]"
                title={group.url}
              >
                {chip}
              </a>
            );
          }

          return (
            <span
              key={`${group.label}-${i}`}
              className="inline-flex items-center gap-1 rounded-lg border border-slate-200 bg-slate-50 px-2 py-1 text-[11px] text-slate-600"
              title={group.label}
            >
              {chip}
            </span>
          );
        })}
      </div>
    </div>
  );
}

function TypingIndicator() {
  return (
    <div className="flex items-center gap-1 px-1 py-1">
      <span className="typing-dot h-2 w-2 rounded-full bg-[#0ea5a8]/70" />
      <span className="typing-dot h-2 w-2 rounded-full bg-[#0ea5a8]/70" />
      <span className="typing-dot h-2 w-2 rounded-full bg-[#0ea5a8]/70" />
    </div>
  );
}

function WelcomeScreen({
  collectionName,
  onSend,
}: {
  collectionName: string;
  onSend: (msg: string) => void;
}) {
  return (
    <div className="flex h-full flex-col items-center justify-center px-6 text-center">
      <div className="reveal rounded-full border border-[#0ea5a8]/30 bg-[#0ea5a8]/10 px-3 py-1 text-[10px] uppercase tracking-[0.2em] text-[#0f766e]">
        Active collection
      </div>
      <h2 className="reveal reveal-delay-1 mt-4 font-display text-3xl text-slate-900 md:text-4xl">
        {collectionName}
      </h2>
      <p className="reveal reveal-delay-2 mt-3 max-w-xl text-sm text-slate-600 md:text-base">
        Ask focused questions and keep a dedicated conversation timeline per collection.
      </p>
      <div className="reveal reveal-delay-3 mt-8 flex max-w-2xl flex-wrap justify-center gap-2">
        {SUGGESTIONS.map((suggestion) => (
          <button
            key={suggestion}
            onClick={() => onSend(suggestion)}
            className="rounded-full border border-slate-200 bg-white px-4 py-2 text-sm text-slate-700 shadow-sm transition hover:-translate-y-0.5 hover:border-[#0ea5a8]/40 hover:text-[#0f766e]"
          >
            {suggestion}
          </button>
        ))}
      </div>
    </div>
  );
}

function formatDocCount(totalDocuments: number) {
  if (totalDocuments === 1) return '1 doc';
  return `${totalDocuments} docs`;
}

function buildEmptyCollectionChatState(): CollectionChatState {
  return { messages: [], sessionId: undefined, draft: '' };
}

export default function ChatPage() {
  const [collections, setCollections] = useState<CollectionSummary[]>([]);
  const [activeCollectionId, setActiveCollectionId] = useState('');
  const [chatStateByCollection, setChatStateByCollection] = useState<Record<string, CollectionChatState>>({});
  const [isStreaming, setIsStreaming] = useState(false);
  const [loadingCollections, setLoadingCollections] = useState(true);
  const [collectionsError, setCollectionsError] = useState<string | null>(null);
  const [mobileSidebarOpen, setMobileSidebarOpen] = useState(false);

  const messagesSectionRef = useRef<HTMLElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  const activeCollection = useMemo(
    () => collections.find((collection) => collection.id === activeCollectionId),
    [collections, activeCollectionId],
  );

  const activeChatState = activeCollectionId
    ? chatStateByCollection[activeCollectionId]
    : undefined;

  const activeMessages = activeChatState?.messages ?? [];
  const activeDraft = activeChatState?.draft ?? '';
  const isInputDisabled = isStreaming || !activeCollection;

  const hydrateChatState = useCallback((incomingCollections: CollectionSummary[]) => {
    setChatStateByCollection((previous) => {
      let changed = false;
      const next: Record<string, CollectionChatState> = { ...previous };

      for (const collection of incomingCollections) {
        if (!next[collection.id]) {
          next[collection.id] = buildEmptyCollectionChatState();
          changed = true;
        }
      }

      return changed ? next : previous;
    });
  }, []);

  const loadCollections = useCallback(async () => {
    setLoadingCollections(true);
    setCollectionsError(null);

    try {
      const response = await fetch('/api/collections');
      const payload = (await response.json()) as {
        collections?: CollectionSummary[];
        error?: string;
      };

      if (!response.ok) {
        throw new Error(payload.error ?? `Request failed: ${response.status}`);
      }

      const nextCollections = Array.isArray(payload.collections)
        ? payload.collections
        : [];

      setCollections(nextCollections);
      hydrateChatState(nextCollections);

      setActiveCollectionId((previousActiveId) => {
        if (
          previousActiveId &&
          nextCollections.some((collection) => collection.id === previousActiveId)
        ) {
          return previousActiveId;
        }

        return nextCollections[0]?.id ?? '';
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to load collections';
      setCollections([]);
      setCollectionsError(message);
      setActiveCollectionId('');
    } finally {
      setLoadingCollections(false);
    }
  }, [hydrateChatState]);

  useEffect(() => {
    void loadCollections();
  }, [loadCollections]);

  useEffect(() => {
    const section = messagesSectionRef.current;
    if (section) {
      section.scrollTo({ top: section.scrollHeight, behavior: 'smooth' });
    }
  }, [activeMessages, isStreaming, activeCollectionId]);

  useEffect(() => {
    if (!inputRef.current) return;
    inputRef.current.style.height = 'auto';
    inputRef.current.style.height = `${Math.min(inputRef.current.scrollHeight, 180)}px`;
  }, [activeDraft, activeCollectionId]);

  const setCollectionDraft = useCallback((collectionId: string, value: string) => {
    setChatStateByCollection((previous) => {
      const current = previous[collectionId] ?? buildEmptyCollectionChatState();
      return {
        ...previous,
        [collectionId]: {
          ...current,
          draft: value,
        },
      };
    });
  }, []);

  const appendAssistantDelta = useCallback((collectionId: string, delta: string) => {
    setChatStateByCollection((previous) => {
      const state = previous[collectionId];
      if (!state || state.messages.length === 0) return previous;

      const updatedMessages = [...state.messages];
      const lastMessage = updatedMessages[updatedMessages.length - 1];
      if (lastMessage.role !== 'assistant') return previous;

      updatedMessages[updatedMessages.length - 1] = {
        ...lastMessage,
        content: lastMessage.content + delta,
      };

      return {
        ...previous,
        [collectionId]: {
          ...state,
          messages: updatedMessages,
        },
      };
    });
  }, []);

  const replaceLastAssistantMessage = useCallback((collectionId: string, content: string) => {
    setChatStateByCollection((previous) => {
      const state = previous[collectionId];
      if (!state || state.messages.length === 0) return previous;

      const updatedMessages = [...state.messages];
      const lastMessage = updatedMessages[updatedMessages.length - 1];
      if (lastMessage.role !== 'assistant') return previous;

      updatedMessages[updatedMessages.length - 1] = {
        ...lastMessage,
        content,
      };

      return {
        ...previous,
        [collectionId]: {
          ...state,
          messages: updatedMessages,
        },
      };
    });
  }, []);

  const appendThinkingStep = useCallback((collectionId: string, step: ThinkingStep) => {
    setChatStateByCollection((previous) => {
      const state = previous[collectionId];
      if (!state || state.messages.length === 0) return previous;

      const updatedMessages = [...state.messages];
      const lastMessage = updatedMessages[updatedMessages.length - 1];
      if (lastMessage.role !== 'assistant') return previous;

      updatedMessages[updatedMessages.length - 1] = {
        ...lastMessage,
        thinkingSteps: [...(lastMessage.thinkingSteps ?? []), step],
      };

      return {
        ...previous,
        [collectionId]: { ...state, messages: updatedMessages },
      };
    });
  }, []);

  const setLastAssistantSources = useCallback((collectionId: string, sources: Source[]) => {
    setChatStateByCollection((previous) => {
      const state = previous[collectionId];
      if (!state || state.messages.length === 0) return previous;

      const updatedMessages = [...state.messages];
      const lastMessage = updatedMessages[updatedMessages.length - 1];
      if (lastMessage.role !== 'assistant') return previous;

      updatedMessages[updatedMessages.length - 1] = {
        ...lastMessage,
        sources,
      };

      return {
        ...previous,
        [collectionId]: { ...state, messages: updatedMessages },
      };
    });
  }, []);

  const setCollectionSessionId = useCallback((collectionId: string, sessionId: string) => {
    setChatStateByCollection((previous) => {
      const state = previous[collectionId] ?? buildEmptyCollectionChatState();
      return {
        ...previous,
        [collectionId]: {
          ...state,
          sessionId,
        },
      };
    });
  }, []);

  const sendMessage = useCallback(
    async (content: string) => {
      const collectionId = activeCollectionId;
      if (!collectionId || isStreaming) return;

      const trimmed = content.trim();
      if (!trimmed) return;

      const sessionId = chatStateByCollection[collectionId]?.sessionId;

      setChatStateByCollection((previous) => {
        const state = previous[collectionId] ?? buildEmptyCollectionChatState();
        return {
          ...previous,
          [collectionId]: {
            ...state,
            draft: '',
            messages: [
              ...state.messages,
              { role: 'user', content: trimmed },
              { role: 'assistant', content: '', thinkingSteps: [] },
            ],
          },
        };
      });

      if (inputRef.current) {
        inputRef.current.style.height = 'auto';
      }

      setIsStreaming(true);

      try {
        const response = await fetch('/api/chat', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            message: trimmed,
            sessionId,
            collectionId,
          }),
        });

        if (!response.ok) {
          let errorMessage = `Request failed: ${response.status}`;
          try {
            const payload = (await response.json()) as { error?: string };
            if (payload.error) errorMessage = payload.error;
          } catch {
            // no-op
          }
          throw new Error(errorMessage);
        }

        const reader = response.body?.getReader();
        if (!reader) {
          throw new Error('No response body');
        }

        const decoder = new TextDecoder();
        let buffer = '';

        let shouldStopStream = false;
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;

          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split('\n');
          buffer = lines.pop() ?? '';

          for (const line of lines) {
            if (!line.startsWith('data: ')) continue;

            const data = line.slice(6);
            if (data === '[DONE]') {
              shouldStopStream = true;
              break;
            }

            try {
              const chunk = JSON.parse(data) as {
                eventType?: string;
                content?: string;
                sessionId?: string;
                sources?: Source[];
                thinking?: ThinkingStep;
                error?: string;
                done?: boolean;
              };

              if (chunk.error) {
                replaceLastAssistantMessage(
                  collectionId,
                  `Sorry, something went wrong: ${chunk.error}`,
                );
                shouldStopStream = true;
                break;
              }

              if (chunk.sessionId) {
                setCollectionSessionId(collectionId, chunk.sessionId);
              }

              if (chunk.sources && chunk.sources.length > 0) {
                setLastAssistantSources(collectionId, chunk.sources);
              }

              if (
                chunk.eventType === 'ragora_status' &&
                chunk.thinking
              ) {
                appendThinkingStep(collectionId, {
                  ...chunk.thinking,
                  timestamp:
                    typeof chunk.thinking.timestamp === 'number'
                      ? chunk.thinking.timestamp
                      : Date.now(),
                });
                continue;
              }

              if (chunk.content) {
                appendAssistantDelta(collectionId, chunk.content);
              }

              if (chunk.done) {
                shouldStopStream = true;
                break;
              }
            } catch {
              // ignore malformed SSE chunks
            }
          }

          if (shouldStopStream) break;
        }
      } catch (error) {
        const errorMessage =
          error instanceof Error ? error.message : 'Unknown error';
        replaceLastAssistantMessage(
          collectionId,
          `Sorry, something went wrong: ${errorMessage}`,
        );
      } finally {
        setIsStreaming(false);
      }
    },
    [
      activeCollectionId,
      appendAssistantDelta,
      chatStateByCollection,
      isStreaming,
      replaceLastAssistantMessage,
      setCollectionSessionId,
      setLastAssistantSources,
      appendThinkingStep,
    ],
  );

  const handleSubmit = (event: FormEvent) => {
    event.preventDefault();
    void sendMessage(activeDraft);
  };

  const handleInputChange = (event: React.ChangeEvent<HTMLTextAreaElement>) => {
    if (!activeCollectionId) return;
    setCollectionDraft(activeCollectionId, event.target.value);
    event.target.style.height = 'auto';
    event.target.style.height = `${Math.min(event.target.scrollHeight, 180)}px`;
  };

  const handleKeyDown = (event: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (event.key === 'Enter' && !event.shiftKey) {
      event.preventDefault();
      void sendMessage(activeDraft);
    }
  };

  const startNewChat = () => {
    if (!activeCollectionId || isStreaming) return;
    setChatStateByCollection((previous) => ({
      ...previous,
      [activeCollectionId]: buildEmptyCollectionChatState(),
    }));
    inputRef.current?.focus();
  };

  const switchCollection = (collectionId: string) => {
    if (isStreaming) return;
    setActiveCollectionId(collectionId);
    setMobileSidebarOpen(false);
    inputRef.current?.focus();
  };

  return (
    <div className="relative min-h-dvh overflow-hidden bg-[#061722] text-slate-100">
      <div className="pointer-events-none absolute inset-0">
        <div className="absolute -left-20 top-0 h-80 w-80 rounded-full bg-[#14b8a6]/25 blur-3xl" />
        <div className="absolute right-[-140px] top-[15%] h-[26rem] w-[26rem] rounded-full bg-[#fb923c]/20 blur-3xl" />
        <div className="absolute bottom-[-180px] left-1/3 h-[30rem] w-[30rem] rounded-full bg-[#22d3ee]/20 blur-3xl" />
      </div>

      <div className="relative mx-auto flex h-dvh max-w-[1700px] gap-3 p-3 md:gap-5 md:p-5">
        <button
          type="button"
          aria-label="Close collections"
          onClick={() => setMobileSidebarOpen(false)}
          className={`fixed inset-0 z-30 bg-black/45 transition md:hidden ${mobileSidebarOpen ? 'opacity-100' : 'pointer-events-none opacity-0'}`}
        />

        <aside
          className={`fixed bottom-3 left-3 top-3 z-40 flex w-[290px] flex-col rounded-[28px] border border-white/15 bg-[#0a2434]/85 shadow-2xl backdrop-blur-xl transition-transform duration-300 md:static md:translate-x-0 ${
            mobileSidebarOpen ? 'translate-x-0' : '-translate-x-[115%]'
          }`}
        >
          <div className="reveal border-b border-white/10 px-5 py-5">
            <div className="flex items-start justify-between gap-3">
              <div>
                <p className="text-[10px] uppercase tracking-[0.22em] text-[#67e8f9]/85">
                  Ragora Demo
                </p>
                <h1 className="mt-2 font-display text-3xl leading-none text-white">
                  Collection Studio
                </h1>
                <p className="mt-2 text-xs text-slate-300">
                  Switch collections and keep separate sessions.
                </p>
              </div>
              <button
                type="button"
                onClick={() => void loadCollections()}
                disabled={loadingCollections}
                className="rounded-lg border border-white/15 bg-white/5 px-2 py-1 text-[11px] text-slate-200 transition hover:bg-white/10 disabled:cursor-not-allowed disabled:opacity-60"
              >
                Refresh
              </button>
            </div>
          </div>

          <div className="scrollbar-thin flex-1 space-y-3 overflow-y-auto px-4 py-4">
            {loadingCollections ? (
              <>
                <div className="h-24 animate-pulse rounded-2xl border border-white/10 bg-white/5" />
                <div className="h-24 animate-pulse rounded-2xl border border-white/10 bg-white/5" />
                <div className="h-24 animate-pulse rounded-2xl border border-white/10 bg-white/5" />
              </>
            ) : null}

            {collectionsError ? (
              <div className="rounded-2xl border border-rose-300/35 bg-rose-500/10 p-3 text-xs text-rose-200">
                {collectionsError}
              </div>
            ) : null}

            {!loadingCollections && !collectionsError && collections.length === 0 ? (
              <div className="rounded-2xl border border-white/10 bg-white/5 p-4 text-xs text-slate-300">
                No collections configured. Set <code>RAGORA_COLLECTION_IDS</code> in your <code>.env</code>.
              </div>
            ) : null}

            {!loadingCollections &&
              collections.map((collection, index) => {
                const isActive = collection.id === activeCollectionId;
                const isDisabled = isStreaming;

                return (
                  <button
                    key={collection.id}
                    type="button"
                    onClick={() => switchCollection(collection.id)}
                    disabled={isDisabled}
                    className={`reveal w-full rounded-2xl border p-3 text-left transition ${
                      isActive
                        ? 'border-[#67e8f9]/40 bg-white text-slate-900 shadow-lg shadow-cyan-900/20'
                        : 'border-white/10 bg-white/5 text-slate-100 hover:bg-white/10'
                    } ${isDisabled ? 'cursor-not-allowed opacity-70' : ''}`}
                    style={{ animationDelay: `${index * 80}ms` }}
                  >
                    <div className="flex items-start justify-between gap-2">
                      <div className="min-w-0">
                        <p className="truncate text-sm font-semibold">{collection.name}</p>
                        <p className={`mt-1 text-[11px] ${isActive ? 'text-slate-500' : 'text-slate-300'}`}>
                          {collection.slug ?? collection.ref}
                        </p>
                      </div>
                    </div>
                    <p className={`mt-2 text-xs leading-relaxed ${isActive ? 'text-slate-600' : 'text-slate-300'}`}>
                      {collection.description ?? 'No description provided for this collection.'}
                    </p>
                    <div className={`mt-3 text-[11px] ${isActive ? 'text-slate-500' : 'text-slate-400'}`}>
                      {formatDocCount(collection.totalDocuments)}
                    </div>
                  </button>
                );
              })}
          </div>

          <div className="border-t border-white/10 px-5 py-3 text-[11px] text-slate-300">
            Powered by{' '}
            <a
              href="https://ragora.app"
              target="_blank"
              rel="noopener noreferrer"
              className="text-[#67e8f9] hover:underline"
            >
              Ragora
            </a>
          </div>
        </aside>

        <main className="flex min-w-0 flex-1 flex-col rounded-[28px] border border-white/35 bg-white/88 shadow-2xl backdrop-blur-xl">
          <header className="border-b border-slate-200/80 px-4 py-3 md:px-7 md:py-4">
            <div className="flex items-center justify-between gap-3">
              <div className="flex min-w-0 items-center gap-3">
                <button
                  type="button"
                  onClick={() => setMobileSidebarOpen(true)}
                  className="inline-flex h-10 w-10 items-center justify-center rounded-xl border border-slate-200 bg-white text-slate-600 md:hidden"
                >
                  <MenuIcon />
                </button>
                <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-gradient-to-br from-[#0f766e] to-[#14b8a6] text-white shadow-sm">
                  <CollectionIcon />
                </div>
                <div className="min-w-0">
                  <p className="text-[11px] uppercase tracking-[0.18em] text-slate-500">
                    Active collection
                  </p>
                  <p className="truncate text-sm font-semibold text-slate-900 md:text-base">
                    {activeCollection?.name ?? 'No collection selected'}
                  </p>
                </div>
              </div>

              <button
                type="button"
                onClick={startNewChat}
                disabled={!activeCollection || isStreaming}
                className="inline-flex items-center gap-1.5 rounded-xl border border-slate-200 bg-white px-3 py-2 text-xs font-semibold text-slate-700 transition hover:border-[#0ea5a8]/40 hover:text-[#0f766e] disabled:cursor-not-allowed disabled:opacity-50"
              >
                <PlusIcon />
                New chat
              </button>
            </div>
          </header>

          <section ref={messagesSectionRef} className="scrollbar-thin flex-1 overflow-y-auto px-4 py-5 md:px-8 md:py-6">
            {!activeCollection && !loadingCollections ? (
              <div className="flex h-full items-center justify-center">
                <div className="rounded-2xl border border-slate-200 bg-white p-6 text-center shadow-sm">
                  <h3 className="font-display text-2xl text-slate-900">No collection selected</h3>
                  <p className="mt-2 text-sm text-slate-600">
                    Configure <code>RAGORA_COLLECTION_IDS</code> to start chatting.
                  </p>
                </div>
              </div>
            ) : null}

            {activeCollection && activeMessages.length === 0 ? (
              <WelcomeScreen
                collectionName={activeCollection.name}
                onSend={(message) => {
                  void sendMessage(message);
                }}
              />
            ) : null}

            {activeCollection && activeMessages.length > 0 ? (
              <div className="mx-auto max-w-4xl space-y-6">
                {activeMessages.map((message, index) => {
                  const normalizedContent =
                    message.role === 'assistant' && message.content
                      ? normalizeCitations(normalizeTableLineBreaks(message.content))
                      : '';
                  const citedNumbers = new Set<number>();
                  if (normalizedContent) {
                    for (const m of normalizedContent.matchAll(/\[(\d+)\]/g)) {
                      citedNumbers.add(parseInt(m[1], 10));
                    }
                  }

                  return (
                    <div key={`${activeCollection.id}-${index}`}>
                      {message.role === 'user' ? (
                        <div className="flex justify-end">
                          <div className="max-w-[86%] rounded-3xl rounded-br-md bg-gradient-to-br from-[#0f766e] to-[#14b8a6] px-4 py-3 text-sm text-white shadow-md md:max-w-[75%]">
                            {message.content}
                          </div>
                        </div>
                      ) : (
                        <div className="flex items-start gap-3">
                          <div className="mt-0.5 flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-[#0ea5a8]/15 text-[#0f766e]">
                            <SparklesIcon />
                          </div>
                          <div className="min-w-0 flex-1 rounded-2xl border border-slate-200 bg-white px-4 py-3 shadow-sm">
                            {message.thinkingSteps && message.thinkingSteps.length > 0 ? (
                              <ThinkingProcess
                                steps={message.thinkingSteps}
                                isStreaming={isStreaming && index === activeMessages.length - 1}
                              />
                            ) : null}
                            {normalizedContent ? (
                              <div className="prose-chat text-sm text-slate-800">
                                <ReactMarkdown
                                  remarkPlugins={[remarkGfm, [remarkMath, { singleDollarTextMath: true }]]}
                                  rehypePlugins={[rehypeKatex, rehypeRaw]}
                                  components={{
                                    p: ({ children, ...props }) => (
                                      <p {...props}>{processChildrenForCitations(children)}</p>
                                    ),
                                    li: ({ children, ...props }) => (
                                      <li {...props}>{processChildrenForCitations(children)}</li>
                                    ),
                                  }}
                                >
                                  {normalizedContent}
                                </ReactMarkdown>
                              </div>
                            ) : isStreaming &&
                              index === activeMessages.length - 1 &&
                              (!message.thinkingSteps || message.thinkingSteps.length === 0) ? (
                              <TypingIndicator />
                            ) : null}
                            {message.sources && message.sources.length > 0 ? (
                              <SourcesList sources={message.sources} citedNumbers={citedNumbers} />
                            ) : null}
                          </div>
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            ) : null}
          </section>

          <footer className="border-t border-slate-200/80 bg-white/95 px-4 py-3 md:px-7 md:py-4">
            <form onSubmit={handleSubmit} className="mx-auto max-w-4xl">
              <div className="rounded-2xl border border-slate-200 bg-white shadow-sm transition focus-within:border-[#0ea5a8]/50 focus-within:ring-4 focus-within:ring-[#14b8a6]/15">
                <textarea
                  ref={inputRef}
                  rows={1}
                  value={activeDraft}
                  onChange={handleInputChange}
                  onKeyDown={handleKeyDown}
                  disabled={isInputDisabled}
                  placeholder={
                    activeCollection
                      ? `Ask about ${activeCollection.name}...`
                      : 'Select a collection to begin'
                  }
                  className="max-h-[180px] min-h-[56px] w-full resize-none border-0 bg-transparent px-4 py-3 text-sm text-slate-800 placeholder:text-slate-400 focus:outline-none disabled:cursor-not-allowed disabled:opacity-60"
                />
                <div className="flex items-center justify-between border-t border-slate-100 px-3 py-2">
                  <p className="text-xs text-slate-500">
                    {activeCollection
                      ? `Session is isolated to ${activeCollection.name}`
                      : 'Pick a collection from the sidebar'}
                  </p>
                  <button
                    type="submit"
                    disabled={!activeDraft.trim() || isInputDisabled}
                    className="inline-flex h-9 w-9 items-center justify-center rounded-xl bg-gradient-to-br from-[#0f766e] to-[#14b8a6] text-white shadow-sm transition hover:brightness-110 disabled:cursor-not-allowed disabled:opacity-45"
                  >
                    <SendIcon />
                  </button>
                </div>
              </div>
            </form>
          </footer>
        </main>
      </div>
    </div>
  );
}
