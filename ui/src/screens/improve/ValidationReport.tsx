import { useTranslation } from 'react-i18next';
import type {
  BacktestMetrics,
  BacktestReport,
  EvalReport,
  EvalScore,
  ProposalState,
  ValidationReport as Report,
} from '../../api/schema';
import { ErrorText } from '../../common/Copy';
import { ScrollRegion } from '../../common/ScrollRegion';
import { StatusText, Tag } from '../../common/Status';
import { useFormat, type Format } from '../../common/useFormat';

// The backtest's thresholds (spec 11.3): an agreement may fall by at most two points, the failure rate not at all.
const MAX_AGREEMENT_DROP = 0.02;

const subheading = 'm-0 text-small font-bold';
const table = 'w-full border-collapse text-small';
const th = 'border-b border-line px-3 py-2 text-left text-tiny font-bold text-ink-3';
const td = 'border-b border-line px-3 py-2 align-top';

function Verdict({ passed, label }: { passed: boolean; label?: string | undefined }) {
  const { t } = useTranslation();
  return (
    <StatusText
      status={passed ? 'passed' : 'failed'}
      label={label ?? (passed ? t('improve.validation.passed') : t('improve.validation.failed'))}
    />
  );
}

// The validation the engine ran on the proposal's patch (spec 11.3): check, the evals before and after, and the
// backtest's before and after; or why there is none yet.
export function ValidationReport({ report, state }: { report: Report | null; state: ProposalState }) {
  const { t } = useTranslation();
  if (report === null) {
    return (
      <p className="m-0 text-small text-ink-2">
        {state === 'validating' ? t('improve.validation.running') : t('improve.validation.none')}
      </p>
    );
  }
  return (
    <div className="flex flex-col gap-5">
      <p className="m-0 flex flex-wrap items-center gap-x-3 gap-y-1 text-ui">
        <Verdict passed={report.passed} label={report.passed ? t('improve.validation.allPassed') : undefined} />
        {report.error !== undefined && <ErrorText text={report.error} />}
      </p>
      <section className="flex flex-col gap-2" aria-labelledby="validation-check">
        <h3 id="validation-check" className={subheading}>
          {t('improve.validation.check')}
        </h3>
        {report.check === undefined ? (
          <p className="m-0 text-small text-ink-2">{t('improve.validation.notRun')}</p>
        ) : (
          <>
            <Verdict passed={report.check.passed} />
            {report.check.diagnostics.length > 0 && (
              <ul className="m-0 flex list-none flex-col gap-1 p-0 text-small">
                {report.check.diagnostics.map((d, i) => (
                  <li key={i} className="flex flex-wrap items-baseline gap-x-2 break-words">
                    <Tag tone={d.severity === 'error' ? 'bad' : 'warn'}>
                      {d.severity === 'error' ? t('improve.validation.error') : t('improve.validation.warning')}
                    </Tag>
                    <span className="font-mono text-tiny">{d.path}</span>
                    <span lang="en">{d.message}</span>
                  </li>
                ))}
              </ul>
            )}
          </>
        )}
      </section>
      <Evals report={report} />
      <Backtest backtest={report.backtest} />
    </div>
  );
}

function Score({ score }: { score: EvalScore | undefined }) {
  const { t } = useTranslation();
  const f = useFormat();
  if (score === undefined) return <span className="text-ink-3">—</span>;
  return (
    <span className="flex flex-col gap-0.5">
      <Verdict
        passed={score.passed}
        label={t(score.passed ? 'improve.evals.casePassed' : 'improve.evals.caseFailed', {
          score: f.num(score.score, { maximumFractionDigits: 2 }),
        })}
      />
      {score.error !== undefined && <ErrorText text={score.error} />}
    </span>
  );
}

function EvalTable({ report, afterOnly = false }: { report: EvalReport; afterOnly?: boolean }) {
  const { t } = useTranslation();
  const name = report.after === undefined ? report.action : `${report.action} → ${report.after}`;
  return (
    <ScrollRegion label={t('improve.evals.tableLabel', { step: report.step })} className="overflow-x-auto">
      <table className={table}>
        <caption className="pb-1.5 text-left text-small">
          <span className="font-mono font-bold">{report.step}</span> · <span className="font-mono">{name}</span>{' '}
          {!afterOnly && <Verdict passed={report.passed} />}
          {report.error !== undefined && <ErrorText text={report.error} />}
        </caption>
        <thead>
          <tr>
            <th scope="col" className={th}>
              {t('improve.evals.case')}
            </th>
            {!afterOnly && (
              <th scope="col" className={th}>
                {t('improve.before')}
              </th>
            )}
            <th scope="col" className={th}>
              {t('improve.after')}
            </th>
          </tr>
        </thead>
        <tbody>
          {report.cases.map((c) => (
            <tr key={c.case}>
              <th scope="row" className={`${td} font-mono font-normal`}>
                {c.case}
              </th>
              {!afterOnly && (
                <td className={td}>
                  <Score score={c.before} />
                </td>
              )}
              <td className={td}>
                <Score score={c.after} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </ScrollRegion>
  );
}

function Evals({ report }: { report: Report }) {
  const { t } = useTranslation();
  const evals = report.evals ?? [];
  const added = report.newEvals ?? [];
  return (
    <section className="flex flex-col gap-2" aria-labelledby="validation-evals">
      <h3 id="validation-evals" className={subheading}>
        {t('improve.evals.title')}
      </h3>
      <p className="m-0 max-w-[760px] text-small text-ink-2">{t('improve.evals.rule')}</p>
      {evals.length === 0 ? (
        <p className="m-0 text-small text-ink-2">{t('improve.evals.none')}</p>
      ) : (
        evals.map((e) => <EvalTable key={`${e.step}:${e.action}`} report={e} />)
      )}
      {added.length > 0 && (
        <>
          <h4 className="m-0 mt-2 text-small font-bold">{t('improve.evals.newTitle')}</h4>
          <p className="m-0 max-w-[760px] text-small text-ink-2">{t('improve.evals.newRule')}</p>
          {added.map((e) => (
            <EvalTable key={`new:${e.step}:${e.action}`} report={e} afterOnly />
          ))}
        </>
      )}
    </section>
  );
}

interface Row {
  label: string;
  value: (m: BacktestMetrics) => number;
  format: (v: number) => string;
  change: (d: number) => string;
  // Whether the change from before to after fails the backtest.
  regressed?: (before: number, after: number) => boolean;
  // What an agreement is over: none, and there is nothing to compare.
  over?: (m: BacktestMetrics) => number;
}

function useRows(f: Format): Row[] {
  const { t } = useTranslation();
  // A change with its sign: + or the typographic minus.
  const signed = (v: number, format: (x: number) => string) =>
    `${v > 0 ? '+' : v < 0 ? '−' : ''}${format(Math.abs(v))}`;
  const points = (d: number) =>
    t('improve.backtest.points', { value: signed(d * 100, (x) => f.num(x, { maximumFractionDigits: 1 })) });
  // An agreement fails the backtest when it falls by more than two points; with nothing compared, it does not count.
  const agreement = (label: string, value: Row['value'], over: Row['value']): Row => ({
    label,
    value,
    format: f.percent,
    change: points,
    regressed: (b, a) => b - a > MAX_AGREEMENT_DROP + 1e-9,
    over,
  });
  return [
    agreement(
      t('improve.backtest.agreement'),
      (m) => m.agreement,
      (m) => m.compared,
    ),
    agreement(
      t('improve.backtest.reviewAgreement'),
      (m) => m.reviewAgreement,
      (m) => m.reviewed,
    ),
    {
      label: t('improve.backtest.failureRate'),
      value: (m) => m.failureRate,
      format: f.percent,
      change: points,
      regressed: (b, a) => a > b,
    },
    {
      label: t('improve.backtest.approvalsDropped'),
      value: (m) => m.approvalsDropped,
      format: (v) => f.num(v),
      change: (d) => signed(d, (x) => f.num(x)),
    },
    {
      label: t('improve.backtest.cost'),
      value: (m) => m.costUsd,
      format: f.money,
      change: (d) => signed(d, f.money),
    },
    {
      label: t('improve.backtest.latency'),
      value: (m) => m.latencyMs,
      format: f.duration,
      change: (d) => signed(d, f.duration),
    },
  ];
}

// Before and after, one row per measure; a regression is said in words, not only colour. An agreement with nothing
// to compare does not count.
function MetricsTable({ label, before, after }: { label: string; before: BacktestMetrics; after: BacktestMetrics }) {
  const { t } = useTranslation();
  const f = useFormat();
  const rows = useRows(f);
  const empty = (row: Row, m: BacktestMetrics) => row.over?.(m) === 0;
  return (
    <ScrollRegion label={label} className="overflow-x-auto">
      <table className={table}>
        <caption className="sr-only">{label}</caption>
        <thead>
          <tr>
            <th scope="col" className={th}>
              {t('improve.backtest.measure')}
            </th>
            <th scope="col" className={`${th} text-right`}>
              {t('improve.before')}
            </th>
            <th scope="col" className={`${th} text-right`}>
              {t('improve.after')}
            </th>
            <th scope="col" className={th}>
              {t('improve.backtest.change')}
            </th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => {
            const [b, a] = [row.value(before), row.value(after)];
            const none = empty(row, before) || empty(row, after);
            const regressed = !none && row.regressed?.(b, a) === true;
            return (
              <tr key={row.label}>
                <th scope="row" className={`${td} font-normal`}>
                  {row.label}
                </th>
                <td className={`${td} text-right tabular-nums`}>{empty(row, before) ? '—' : row.format(b)}</td>
                <td className={`${td} text-right tabular-nums`}>{empty(row, after) ? '—' : row.format(a)}</td>
                <td className={`${td} tabular-nums`}>
                  {none ? (
                    <span className="text-ink-3">{t('improve.backtest.nothingToCompare')}</span>
                  ) : regressed ? (
                    <StatusText
                      status="failed"
                      label={t('improve.backtest.regressed', { change: row.change(a - b) })}
                    />
                  ) : (
                    row.change(a - b)
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </ScrollRegion>
  );
}

function Backtest({ backtest }: { backtest: Report['backtest'] }) {
  const { t } = useTranslation();
  const f = useFormat();
  const report = backtest?.report as BacktestReport | undefined;
  return (
    <section className="flex flex-col gap-2" aria-labelledby="validation-backtest">
      <h3 id="validation-backtest" className={subheading}>
        {t('improve.backtest.title')}
      </h3>
      <p className="m-0 max-w-[760px] text-small text-ink-2">{t('improve.backtest.rule')}</p>
      {backtest === undefined ? (
        <p className="m-0 text-small text-ink-2">{t('improve.validation.notRun')}</p>
      ) : (
        <Verdict passed={backtest.passed} />
      )}
      {backtest?.skipped !== undefined && (
        <p className="m-0 text-small break-words">{t('improve.backtest.skipped', { reason: backtest.skipped })}</p>
      )}
      {report !== undefined && (
        <>
          <dl className="m-0 grid grid-cols-[minmax(0,auto)_minmax(0,1fr)] gap-x-4 gap-y-1.5 text-small">
            <dt className="text-ink-3">{t('improve.backtest.sample')}</dt>
            <dd className="m-0 break-words">
              {t(report.source === 'evidence' ? 'improve.backtest.fromEvidence' : 'improve.backtest.fromLocal', {
                count: f.num(report.sampled),
                runs: f.num(report.runs),
              })}
              {Object.keys(report.strata).length > 0 &&
                ` · ${Object.entries(report.strata)
                  .map(([k, v]) => `${k} ${f.num(v)}`)
                  .join(', ')}`}
            </dd>
            <dt className="text-ink-3">{t('improve.backtest.changedSteps')}</dt>
            <dd className="m-0 font-mono break-words">{report.changedSteps.join(', ') || '—'}</dd>
            <dt className="text-ink-3">{t('improve.backtest.reach')}</dt>
            <dd className="m-0">
              {t('improve.backtest.reachValue', {
                added: f.num(report.after.newlyReached),
                dropped: f.num(report.after.noLongerReached),
              })}
            </dd>
            <dt className="text-ink-3">{t('improve.backtest.spent')}</dt>
            <dd className="m-0">{f.money(report.costUsd)}</dd>
          </dl>
          <MetricsTable label={t('improve.backtest.tableLabel')} before={report.before} after={report.after} />
          {report.steps.length > 0 && (
            <details className="rounded-1 border border-line px-3 py-2">
              <summary className="cursor-pointer text-small font-bold">{t('improve.backtest.byStep')}</summary>
              <div className="mt-2 flex flex-col gap-3">
                {report.steps.map((s) => (
                  <MetricsTable
                    key={s.step}
                    label={t('improve.backtest.stepTableLabel', { step: s.step })}
                    before={s.before}
                    after={s.after}
                  />
                ))}
              </div>
            </details>
          )}
          {((report.divergence?.length ?? 0) > 0 || (report.warnings?.length ?? 0) > 0) && (
            <div
              role="note"
              className="flex flex-col gap-1.5 rounded-1 border border-warn bg-warn-soft px-3 py-2.5 text-small"
            >
              <strong>{t('improve.backtest.warnings')}</strong>
              <ul className="m-0 flex flex-col gap-1 pl-[18px]">
                {(report.divergence ?? []).map((d) => (
                  <li key={d.packageHash} className="break-words">
                    {t('improve.backtest.divergence', { revision: d.packageHash, count: f.num(d.sampled) })}
                  </li>
                ))}
                {(report.warnings ?? []).map((w) => (
                  <li key={w} lang="en" className="break-words">
                    {w}
                  </li>
                ))}
              </ul>
            </div>
          )}
        </>
      )}
    </section>
  );
}
