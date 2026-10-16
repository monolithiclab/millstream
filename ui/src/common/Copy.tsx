import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { byteSize, formatBytes, useFormat } from './useFormat';
import { buttons } from './styles';

function copy(text: string): Promise<void> {
  return navigator.clipboard?.writeText(text) ?? Promise.reject(new Error('clipboard unavailable'));
}

// Copies text; the label turns to "Copied" until the next press or a new text. icon draws the 28 px square of the canvas;
// className replaces the link style.
export function CopyButton({
  text,
  label,
  icon = false,
  className = buttons.link,
}: {
  text: string;
  label: string;
  icon?: boolean;
  className?: string;
}) {
  const { t } = useTranslation();
  // The text last copied: a new text is not copied yet.
  const [copiedText, setCopiedText] = useState<string | null>(null);
  const copied = copiedText === text;
  const onClick = () => {
    copy(text).then(
      () => setCopiedText(text),
      () => setCopiedText(null),
    );
  };
  if (icon) {
    return (
      <button
        type="button"
        onClick={onClick}
        aria-label={copied ? t('common.copied') : label}
        title={label}
        className="inline-flex size-7 flex-none items-center justify-center rounded-1 border border-line bg-surface text-ink-2"
      >
        <svg width="13" height="13" viewBox="0 0 14 14" aria-hidden="true">
          <path d="M4.5 4.5H12.5V12.5H4.5ZM1.5 9.5V1.5H9.5" fill="none" stroke="currentColor" strokeWidth="1.4" />
        </svg>
      </button>
    );
  }
  return (
    <button type="button" onClick={onClick} className={className}>
      {copied ? t('common.copied') : label}
    </button>
  );
}

// Errors up to 4 KB show three lines, then "Show full error · 3.6 KB" and Copy (interaction notes).
export function ErrorText({ text }: { text: string }) {
  const { t } = useTranslation();
  const f = useFormat();
  const [full, setFull] = useState(false);
  const long = text.length > 240 || text.split('\n').length > 3;
  return (
    <div className="flex min-w-0 flex-col gap-1.5">
      <pre
        className={`m-0 rounded-1 bg-surface-2 px-3 py-2.5 font-mono text-tiny leading-[18px] break-words whitespace-pre-wrap text-ink-2 ${full || !long ? '' : 'line-clamp-3'}`}
      >
        {text}
      </pre>
      <div className="flex flex-wrap gap-3.5">
        {long && (
          <button type="button" className={buttons.link} onClick={() => setFull(!full)} aria-expanded={full}>
            {full ? t('common.showLess') : t('common.showFullError', { size: formatBytes(byteSize(text), f) })}
          </button>
        )}
        <CopyButton text={text} label={t('common.copyError')} />
      </div>
    </div>
  );
}
