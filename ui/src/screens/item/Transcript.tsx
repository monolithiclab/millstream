import { useTranslation } from 'react-i18next';
import { QueryView } from '../../common/States';
import { useResource } from '../../common/useResource';
import { useFormat } from '../../common/useFormat';

// An attempt's transcript.json (internal/agent.Transcript, spec section 7.2).
interface Turn {
  role: string;
  text?: string;
  data?: unknown;
}

export interface TranscriptFile {
  harness?: string;
  model?: string;
  prompt?: string;
  turns?: Turn[];
  output?: unknown;
  error?: string;
  tokensIn?: number;
  tokensOut?: number;
  costUsd?: number;
  durationMs?: number;
}

export type Message =
  | { kind: 'input' | 'user' | 'response' | 'system'; text: string }
  | { kind: 'toolCall'; name: string; input: string }
  | { kind: 'toolResult'; text: string };

interface Block {
  type?: string;
  text?: string;
  name?: string;
  input?: unknown;
  content?: unknown;
}

function blocksOf(data: unknown): Block[] {
  if (typeof data !== 'object' || data === null) return [];
  const record = data as { content?: unknown; message?: { content?: unknown } };
  const content = record.message?.content ?? record.content;
  return Array.isArray(content) ? (content as Block[]) : [];
}

function resultText(content: unknown): string {
  if (typeof content === 'string') return content;
  if (Array.isArray(content))
    return content.map((c: Block) => (typeof c === 'string' ? c : (c.text ?? JSON.stringify(c)))).join('\n');
  return JSON.stringify(content);
}

// The conversation of a transcript: the prompt, the model's answers, its tool calls and their results. A turn's
// text wins over its harness record (the api harness keeps both); claude-code stream-json records carry tool calls
// and results as content blocks; system and result records are the harness's own and stay out.
export function messagesOf(transcript: TranscriptFile): Message[] {
  const out: Message[] = [];
  // The first person's message is the input; later ones are messages.
  const said = () => (out.some((m) => m.kind === 'input') ? 'user' : 'input');
  for (const turn of transcript.turns ?? []) {
    const blocks = blocksOf(turn.data);
    if (turn.role === 'system') {
      if (turn.text) out.push({ kind: 'system', text: turn.text });
      continue;
    }
    if (turn.role === 'result') continue;
    if (turn.text) {
      out.push({ kind: turn.role === 'assistant' ? 'response' : said(), text: turn.text });
    }
    for (const block of blocks) {
      if (block.type === 'text' && !turn.text && block.text) {
        out.push({ kind: turn.role === 'assistant' ? 'response' : said(), text: block.text });
      } else if (block.type === 'tool_use') {
        out.push({ kind: 'toolCall', name: block.name ?? '?', input: JSON.stringify(block.input ?? {}) });
      } else if (block.type === 'tool_result') {
        out.push({ kind: 'toolResult', text: resultText(block.content) });
      }
    }
  }
  if (out.length === 0 && transcript.prompt) out.push({ kind: 'input', text: transcript.prompt });
  return out;
}

const labelBase = 'text-[11px] font-bold tracking-[0.05em] uppercase';
const labelClass = `${labelBase} text-ink-3`;

// The transcript as a conversation (canvas "Transcript view"): input, tool calls dashed in mono, responses, then
// the final output tinted accent when valid or red with the error when not; tokens, cost and duration on top.
export function TranscriptView({ transcript }: { transcript: TranscriptFile }) {
  const { t } = useTranslation();
  const f = useFormat();
  const messages = messagesOf(transcript);
  return (
    <div className="flex flex-col gap-2.5">
      <div className="flex flex-wrap gap-x-3 gap-y-1 font-mono text-tiny text-ink-2">
        {transcript.model !== undefined && <span>{transcript.model}</span>}
        <span>
          {t('item.transcript.tokens', { in: f.num(transcript.tokensIn ?? 0), out: f.num(transcript.tokensOut ?? 0) })}
        </span>
        {transcript.costUsd !== undefined && <span>{f.money(transcript.costUsd)}</span>}
        {transcript.durationMs !== undefined && <span>{f.duration(transcript.durationMs)}</span>}
      </div>
      <ol className="m-0 flex list-none flex-col gap-2.5 p-0">
        {messages.map((m, i) => (
          <li key={i} className="flex max-w-[620px] flex-col gap-1">
            {m.kind === 'system' ? (
              <details className="rounded-2 border border-line bg-surface-2 px-3 py-2">
                <summary className="cursor-pointer text-tiny font-bold text-ink-2">
                  {t('item.transcript.system')}
                </summary>
                <p className="m-0 mt-2 text-small whitespace-pre-wrap">{m.text}</p>
              </details>
            ) : m.kind === 'toolCall' ? (
              <>
                <span className={labelClass}>{t('item.transcript.toolCall')}</span>
                <div className="rounded-2 border border-dashed border-control-line px-3 py-2.5 font-mono text-tiny leading-[18px] break-words text-ink-2">
                  <span className="font-bold text-accent">{m.name}</span>({m.input})
                </div>
              </>
            ) : m.kind === 'toolResult' ? (
              <>
                <span className={labelClass}>{t('item.transcript.toolResult')}</span>
                <div className="rounded-2 border border-dashed border-control-line px-3 py-2.5 font-mono text-tiny leading-[18px] break-words whitespace-pre-wrap text-ink-2">
                  → {m.text}
                </div>
              </>
            ) : (
              <>
                <span className={labelClass}>
                  {m.kind === 'input'
                    ? t('item.transcript.input')
                    : m.kind === 'user'
                      ? t('item.transcript.user')
                      : t('item.transcript.response')}
                </span>
                <div
                  className={`rounded-2 px-3 py-2.5 text-small break-words whitespace-pre-wrap ${m.kind === 'response' ? 'bg-surface-2' : 'border border-line'}`}
                >
                  {m.text}
                </div>
              </>
            )}
          </li>
        ))}
        {transcript.error !== undefined ? (
          <li className="flex max-w-[620px] flex-col gap-1">
            <span className={`${labelBase} text-bad`}>{t('item.transcript.invalid')}</span>
            <div className="rounded-2 bg-bad-soft px-3 py-2.5 font-mono text-tiny leading-[18px] break-words text-bad">
              {transcript.error}
            </div>
          </li>
        ) : (
          transcript.output !== undefined && (
            <li className="flex max-w-[620px] flex-col gap-1">
              <span className={labelClass}>{t('item.transcript.valid')}</span>
              <pre className="m-0 rounded-2 bg-accent-soft px-3 py-2.5 font-mono text-tiny leading-[18px] break-words whitespace-pre-wrap text-ink">
                {JSON.stringify(transcript.output, null, 2)}
              </pre>
            </li>
          )
        )}
      </ol>
    </div>
  );
}

// Loads an attempt's transcript artifact and shows it.
export function AttemptTranscript({
  pipeline,
  delivery,
  attempt,
}: {
  pipeline: string;
  delivery: string;
  attempt: number;
}) {
  const { t } = useTranslation();
  const read = useResource(`transcript:${pipeline}:${delivery}:${attempt}`, async (api) => {
    const text = await api.readArtifact({ pipeline, delivery, attempt, name: 'transcript.json' });
    return JSON.parse(text) as TranscriptFile;
  });
  return (
    <QueryView query={read} title={t('item.transcript.error')} rows={2} className="h-12">
      {(transcript) => <TranscriptView transcript={transcript} />}
    </QueryView>
  );
}
