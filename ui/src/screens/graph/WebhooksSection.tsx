import { useId, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useApi } from '../../api/context';
import type { Trigger, TriggerToken } from '../../api/schema';
import { ConfirmDialog, Consequences } from '../../common/ConfirmDialog';
import { CopyButton } from '../../common/Copy';
import { errorMessage, type Notice } from '../../common/Notice';
import { buttons, card, cardTitle } from '../../common/styles';
import { Dialog } from '../../shell/Dialog';

// Where a webhook trigger is served: the server's address, which is the UI's, and the trigger's path.
const hookUrl = (path: string) => window.location.origin + path;

const codeBlock = 'm-0 block rounded-1 bg-surface-2 px-3 py-2 font-mono break-all text-ink';

// The pipeline's webhook triggers: each one's URL to copy, its rate, and its token to rotate. The new token is shown
// once, in a dialog; the old one stops working at once. Nothing shows for a pipeline without webhook triggers.
export function WebhooksSection({
  pipeline,
  triggers,
  setNotice,
}: {
  pipeline: string;
  triggers: Trigger[];
  setNotice: (notice: Notice | null) => void;
}) {
  const { t } = useTranslation();
  const api = useApi();
  const heading = useId();
  const [rotating, setRotating] = useState<Trigger | null>(null);
  const [issued, setIssued] = useState<TriggerToken | null>(null);
  const hooks = triggers.filter((tr) => tr.kind === 'webhook');
  if (hooks.length === 0) return null;

  // The confirmed rotation; Retry, after a failure, runs it again.
  const rotate = async (trigger: string) => {
    try {
      const token = await api.rotateTriggerToken(pipeline, trigger);
      setNotice(null);
      setIssued(token);
    } catch (error) {
      const message = t('graph.webhooks.rotateFailed', { trigger, message: errorMessage(error) });
      setNotice({ kind: 'error', message, retry: () => void rotate(trigger) });
    } finally {
      setRotating(null);
    }
  };

  return (
    <section aria-labelledby={heading} className={`${card} flex flex-col gap-3 px-5 py-4`}>
      <h2 id={heading} className={cardTitle}>
        {t('graph.webhooks.title')}
      </h2>
      <p className="m-0 text-small text-ink-2">
        {t('graph.webhooks.intro', { header: 'Authorization', param: '?token=' })}
      </p>
      <ul className="m-0 flex list-none flex-col gap-4 p-0">
        {hooks.map((hook) => {
          const url = hookUrl(hook.path ?? `/hooks/${pipeline}/${hook.name}`);
          return (
            <li key={hook.name} className="flex min-w-0 flex-col gap-2 border-t border-line pt-3">
              <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
                <h3 className="m-0 font-mono text-small font-bold">{hook.name}</h3>
                {hook.rate !== undefined && (
                  <span className="text-tiny text-ink-2">{t('graph.webhooks.rate', { rate: hook.rate })}</span>
                )}
              </div>
              <code className={`${codeBlock} text-tiny`}>{url}</code>
              <div className="flex flex-wrap gap-2">
                <CopyButton text={url} label={t('graph.webhooks.copyUrl')} className={buttons.smallSecondary} />
                <button
                  type="button"
                  className={buttons.smallSecondary}
                  aria-label={t('graph.webhooks.rotateName', { trigger: hook.name })}
                  onClick={() => setRotating(hook)}
                >
                  {t('graph.webhooks.rotate')}
                </button>
              </div>
            </li>
          );
        })}
      </ul>
      {rotating !== null && (
        <ConfirmDialog
          title={t('graph.webhooks.rotateTitle', { trigger: rotating.name })}
          confirmLabel={t('graph.webhooks.rotate')}
          destructive
          onConfirm={() => rotate(rotating.name)}
          onCancel={() => setRotating(null)}
        >
          <Consequences>
            <li>{t('graph.webhooks.rotateNew', { trigger: rotating.name })}</li>
            <li>{t('graph.webhooks.rotateOld')}</li>
            <li>{t('graph.webhooks.rotateSenders')}</li>
            <li>{t('graph.webhooks.rotateOnce')}</li>
          </Consequences>
        </ConfirmDialog>
      )}
      {issued !== null && <TokenDialog issued={issued} onClose={() => setIssued(null)} />}
    </section>
  );
}

// The new token, the only time it is shown: copy it, and how to send it.
function TokenDialog({ issued, onClose }: { issued: TriggerToken; onClose: () => void }) {
  const { t } = useTranslation();
  const title = useId();
  const { trigger, token } = issued;
  return (
    <Dialog labelledBy={title} onClose={onClose}>
      <div className="flex flex-col gap-3.5 px-6 pt-[22px] pb-5">
        <h2 id={title} className="m-0 font-display text-[32px] leading-9 font-normal text-balance">
          {t('graph.webhooks.tokenTitle', { trigger })}
        </h2>
        <p className="m-0 rounded-2 border border-warn bg-warn-soft px-3.5 py-3 text-small text-ink">
          <strong>{t('graph.webhooks.tokenOnce')}</strong> {t('graph.webhooks.tokenLost')}
        </p>
        <code className={`${codeBlock} text-small`} data-testid="trigger-token">
          {token}
        </code>
        <div>
          <CopyButton text={token} label={t('graph.webhooks.copyToken')} className={buttons.secondary} />
        </div>
        <p className="m-0 text-small text-ink-2">{t('graph.webhooks.usage')}</p>
        <pre className={`${codeBlock} text-tiny whitespace-pre-wrap text-ink-2`}>
          {`POST ${hookUrl(issued.path)}\nAuthorization: Bearer ${token}`}
        </pre>
        <div className="mt-1 flex justify-end">
          <button type="button" className={buttons.primary} onClick={onClose}>
            {t('graph.webhooks.done')}
          </button>
        </div>
      </div>
    </Dialog>
  );
}
