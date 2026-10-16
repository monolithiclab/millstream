import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import type { ArtifactRef, Item, Lineage, LineageNode } from '../../api/schema';
import { useApi } from '../../api/context';
import { buttons, card } from '../../common/styles';
import { QueryView } from '../../common/States';
import { useResource } from '../../common/useResource';
import { shortId } from '../../common/Id';
import { formatBytes, useFormat } from '../../common/useFormat';
import { itemPath } from '../../routes';

interface Row {
  node: LineageNode | null;
  // Levels from the item: negative upstream, positive downstream, 0 the item.
  level: number;
}

// Each node's distance from the start ids, breadth first along next.
function distances(nodes: LineageNode[], start: string[], next: (id: string) => string[]): Map<string, number> {
  const known = new Set(nodes.map((n) => n.id));
  const seen = new Map<string, number>();
  let level = start;
  for (let d = 1; level.length > 0; d++) {
    const fresh = level.filter((id) => known.has(id) && !seen.has(id));
    fresh.forEach((id) => seen.set(id, d));
    level = fresh.flatMap(next);
  }
  return seen;
}

// The lineage as rows: ancestors farthest first, the item, then descendants nearest first, each at its distance.
export function lineageRows(lineage: Lineage, parents: string[]): Row[] {
  const byId = new Map(lineage.up.map((n) => [n.id, n]));
  const up = distances(lineage.up, parents, (id) => byId.get(id)?.parents ?? []);
  const children = (id: string) => lineage.down.filter((n) => n.parents.includes(id)).map((n) => n.id);
  const down = distances(lineage.down, children(lineage.item), children);
  return [
    ...lineage.up.map((n) => ({ node: n, level: -(up.get(n.id) ?? 1) })).sort((a, b) => a.level - b.level),
    { node: null, level: 0 },
    ...lineage.down.map((n) => ({ node: n, level: down.get(n.id) ?? 1 })).sort((a, b) => a.level - b.level),
  ];
}

// Lineage up and down (canvas): a tree indented by distance, each node a door to its item, the item itself in bold,
// and a marker when it goes deeper than the server walked.
export function LineageTree({ pipeline, item, lineage }: { pipeline: string; item: Item; lineage: Lineage }) {
  const { t } = useTranslation();
  const f = useFormat();
  const rows = lineageRows(lineage, item.parents);
  const minLevel = Math.min(0, ...rows.map((r) => r.level));
  return (
    <section aria-labelledby="lineage-title" className={`${card} flex flex-col gap-2.5 px-3.5 py-3`}>
      <h2 id="lineage-title" className="m-0 text-ui font-bold">
        {t('item.lineage.title')}
      </h2>
      {lineage.up.length === 0 && lineage.down.length === 0 && (
        <p className="m-0 text-small text-ink-2">{t('item.lineage.alone')}</p>
      )}
      <ol className="m-0 flex list-none flex-col p-0 text-tiny">
        {rows.map(({ node, level }) => (
          <li
            key={node?.id ?? 'self'}
            className="ml-1.5 flex flex-wrap items-center gap-x-2 border-l border-line py-[5px]"
            style={{ paddingLeft: 4 + (level - minLevel) * 16 }}
          >
            <span className="-ml-px h-px w-2.5 bg-line-strong" aria-hidden="true" />
            {node === null ? (
              <>
                <span className="font-mono font-bold text-ink" title={item.id}>
                  {shortId(item.id)}
                </span>
                <span className="text-ink-3">{t('item.lineage.self', { step: item.step, stream: item.stream })}</span>
              </>
            ) : (
              <>
                <span className="sr-only">
                  {level < 0
                    ? t('item.lineage.upLevel', { n: f.num(-level) })
                    : t('item.lineage.downLevel', { n: f.num(level) })}
                </span>
                <Link to={itemPath(node.pipeline, node.id)} className="font-mono text-ink-2" title={node.id}>
                  {shortId(node.id)}
                </Link>
                <span className="text-ink-3">
                  {node.step} · {node.stream}
                </span>
                {node.pipeline !== pipeline && (
                  <span className="text-ink-2">{t('item.lineage.inPipeline', { pipeline: node.pipeline })}</span>
                )}
              </>
            )}
          </li>
        ))}
      </ol>
      {lineage.truncated && <p className="m-0 text-tiny text-ink-3">{t('item.lineage.truncated')}</p>}
    </section>
  );
}

function TextPreview({ pipeline, item, artifact }: { pipeline: string; item: string; artifact: ArtifactRef }) {
  const { t } = useTranslation();
  const read = useResource(`artifact:${pipeline}:${item}:${artifact.name}`, (api) =>
    api.readArtifact({ pipeline, item, name: artifact.name }),
  );
  return (
    <QueryView query={read} title={t('item.artifacts.error')} rows={1} className="h-10">
      {(text) => (
        <pre className="m-0 max-h-64 overflow-auto rounded-1 bg-surface-2 px-3 py-2 font-mono text-tiny break-words whitespace-pre-wrap text-ink-2">
          {artifact.type.includes('json') ? indented(text) : text}
        </pre>
      )}
    </QueryView>
  );
}

// JSON indented, or the text as it is when it is not JSON after all.
function indented(text: string): string {
  try {
    return JSON.stringify(JSON.parse(text), null, 2);
  } catch {
    return text;
  }
}

const textual = (type: string) => type.startsWith('text/') || type.includes('json');

// One artifact, previewed by its type: images inline, PDFs in an object (the server serves artifacts under a sandbox
// policy), text and JSON on demand, anything else as a link.
function ArtifactPreview({ pipeline, item, artifact }: { pipeline: string; item: string; artifact: ArtifactRef }) {
  const { t } = useTranslation();
  const f = useFormat();
  const api = useApi();
  const [shown, setShown] = useState(false);
  const url = api.artifactUrl({ pipeline, item, name: artifact.name });
  const meta = `${artifact.name} · ${formatBytes(artifact.size, f)}`;
  const link = (
    <a href={url} target="_blank" rel="noopener noreferrer" className="font-mono text-tiny break-all">
      {t('item.artifacts.open', { name: artifact.name })}
    </a>
  );
  if (artifact.type.startsWith('image/')) {
    return (
      <figure className="m-0 flex flex-col gap-1">
        <img
          src={url}
          alt={artifact.name}
          loading="lazy"
          className="aspect-[4/3] w-full rounded-1 border border-line bg-surface-2 object-cover"
        />
        <figcaption className="font-mono text-[11px] break-all text-ink-3">{meta}</figcaption>
      </figure>
    );
  }
  return (
    <div className="col-span-full flex flex-col gap-1.5">
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-small text-ink-2">
        <svg width="14" height="14" viewBox="0 0 16 16" aria-hidden="true" className="flex-none text-ink-3">
          <path d="M3.5 1.5H10L13 4.5V14.5H3.5ZM10 1.5V4.5H13" fill="none" stroke="currentColor" strokeWidth="1.4" />
        </svg>
        <span className="font-mono text-tiny break-all">{meta}</span>
        {textual(artifact.type) && (
          <button type="button" className={buttons.link} aria-expanded={shown} onClick={() => setShown(!shown)}>
            {shown ? t('item.artifacts.hide') : t('item.artifacts.preview')}
          </button>
        )}
        {link}
      </div>
      {artifact.type === 'application/pdf' && (
        <object
          data={url}
          type="application/pdf"
          aria-label={artifact.name}
          className="h-72 w-full rounded-1 border border-line"
        >
          <p className="m-2 text-small text-ink-2">{t('item.artifacts.noPdf')}</p>
        </object>
      )}
      {shown && <TextPreview pipeline={pipeline} item={item} artifact={artifact} />}
    </div>
  );
}

export function Artifacts({ pipeline, item }: { pipeline: string; item: Item }) {
  const { t } = useTranslation();
  return (
    <section aria-labelledby="artifacts-title" className={`${card} flex flex-col gap-2.5 px-3.5 py-3`}>
      <h2 id="artifacts-title" className="m-0 text-ui font-bold">
        {t('item.artifacts.title')}
      </h2>
      {item.artifacts.length === 0 ? (
        <p className="m-0 text-small text-ink-2">{t('item.artifacts.none')}</p>
      ) : (
        <div className="grid grid-cols-2 gap-2 sm:grid-cols-3">
          {item.artifacts.map((a) => (
            <ArtifactPreview key={a.name} pipeline={pipeline} item={item.id} artifact={a} />
          ))}
        </div>
      )}
    </section>
  );
}
