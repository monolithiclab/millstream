import { useTranslation } from 'react-i18next';
import { Link, useParams } from 'react-router-dom';
import { enc, withQuery } from '../../api/api';
import type { StepLessons } from '../../api/schema';
import { pipelinePath } from '../../routes';
import { ScreenPage } from '../ScreenPage';
import { EmptyState, QueryView } from '../../common/States';
import { card, cardTitle } from '../../common/styles';
import { useResource } from '../../common/useResource';
import { ReviewSource } from './ReviewSource';

// A lesson's source (spec 11.4) is "kind/id": an insight links to its card, a review opens its decision in a panel; a
// proposal has no page until the Improve view (M5), so it is named only.
function Source({ pipeline, source }: { pipeline: string; source: string }) {
  const { t } = useTranslation();
  const [kind = '', ...rest] = source.split('/');
  const id = rest.join('/');
  if (kind === 'insight' && id) {
    return <Link to={`${pipelinePath(pipeline, 'insights')}#${enc(id)}`}>{t('lessons.fromInsight', { id })}</Link>;
  }
  if (kind === 'review' && id) return <ReviewSource pipeline={pipeline} delivery={id} />;
  if (kind === 'proposal' && id) return <span>{t('lessons.fromProposal', { id })}</span>;
  return <span className="font-mono">{source}</span>;
}

const json = (value: unknown) => JSON.stringify(value, null, 2);

// What each step has learned (lessons/<step>.yaml of the active revision): rules and examples, read-only, each with
// the source it cites. Lessons change through the package (or, from M5, accepted proposals).
export function LessonsScreen() {
  const { t } = useTranslation();
  const { pipeline = '' } = useParams();
  const lessons = useResource(`lessons:${pipeline}`, (api) => api.listLessons(pipeline));

  return (
    <ScreenPage
      title={t('nav.lessons')}
      eyebrow={<p className="m-0 font-mono text-small text-ink-2">{pipeline}</p>}
      header={<p className="m-0 text-tiny text-ink-3 md:ml-auto">{t('lessons.explain')}</p>}
    >
      <QueryView query={lessons}>
        {(steps) =>
          steps.length === 0 ? (
            <EmptyState title={t('lessons.empty')}>
              {t('lessons.emptyBody')}
              <code className="mt-2 block rounded-1 bg-surface-2 px-2.5 py-2 font-mono text-tiny">
                lessons/score.yaml
              </code>
            </EmptyState>
          ) : (
            <div className="flex flex-col gap-4">
              {steps.map((s) => (
                <StepCard key={s.step} pipeline={pipeline} lessons={s} />
              ))}
            </div>
          )
        }
      </QueryView>
    </ScreenPage>
  );
}

function StepCard({ pipeline, lessons }: { pipeline: string; lessons: StepLessons }) {
  const { t } = useTranslation();
  return (
    <section aria-labelledby={`lessons-${lessons.step}`} className={`${card} flex flex-col gap-3 px-5 py-[18px]`}>
      <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
        <h2 id={`lessons-${lessons.step}`} className={`${cardTitle} font-mono text-[20px]`}>
          {lessons.step}
        </h2>
        <Link to={withQuery(pipelinePath(pipeline, 'graph'), { step: lessons.step })} className="text-tiny">
          {t('lessons.openStep')}
        </Link>
      </div>
      <h3 className="m-0 text-tiny font-bold tracking-wide text-ink-3 uppercase">{t('lessons.rules')}</h3>
      {lessons.rules.length === 0 ? (
        <p className="m-0 text-small text-ink-2">{t('lessons.noRules')}</p>
      ) : (
        <ol className="m-0 flex flex-col gap-2.5 pl-5">
          {lessons.rules.map((rule) => (
            <li key={rule.text} className="text-ui">
              <span className="break-words">{rule.text}</span>
              {(rule.source !== undefined || rule.added !== undefined) && (
                <span className="mt-0.5 flex flex-wrap gap-x-3 text-tiny text-ink-3">
                  {rule.source !== undefined && <Source pipeline={pipeline} source={rule.source} />}
                  {rule.added !== undefined && <span>{t('lessons.added', { date: rule.added })}</span>}
                </span>
              )}
            </li>
          ))}
        </ol>
      )}
      <h3 className="m-0 text-tiny font-bold tracking-wide text-ink-3 uppercase">{t('lessons.examples')}</h3>
      {lessons.examples.length === 0 ? (
        <p className="m-0 text-small text-ink-2">{t('lessons.noExamples')}</p>
      ) : (
        lessons.examples.map((example, i) => (
          <figure key={i} className="m-0 flex flex-col gap-2 rounded-2 border border-line p-3">
            <div className="grid gap-2 md:grid-cols-2">
              {[
                [t('lessons.input'), example.input],
                [t('lessons.output'), example.output],
              ].map(([label, value]) => (
                <div key={String(label)} className="flex min-w-0 flex-col gap-1">
                  <span className="text-[11px] font-bold tracking-wide text-ink-3 uppercase">{String(label)}</span>
                  <pre className="m-0 overflow-x-auto rounded-1 bg-surface-2 p-2 font-mono text-tiny break-words whitespace-pre-wrap">
                    {json(value)}
                  </pre>
                </div>
              ))}
            </div>
            {(example.why !== undefined || example.source !== undefined) && (
              <figcaption className="flex flex-wrap gap-x-3 text-small text-ink-2">
                {example.why !== undefined && <span className="break-words">{example.why}</span>}
                {example.source !== undefined && (
                  <span className="text-tiny">
                    <Source pipeline={pipeline} source={example.source} />
                  </span>
                )}
              </figcaption>
            )}
          </figure>
        ))
      )}
    </section>
  );
}
