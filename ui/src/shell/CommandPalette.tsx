import { useId, useState, type KeyboardEvent } from 'react';
import { useTranslation } from 'react-i18next';
import { Dialog } from './Dialog';
import { Kbd } from './Kbd';

export interface Command {
  id: string;
  group: string;
  label: string;
  hint?: string | undefined;
  run: () => void;
}

// Case- and accent-insensitive, so "themes" finds "thème".
function normalise(text: string): string {
  return text
    .normalize('NFD')
    .replace(/\p{Diacritic}/gu, '')
    .toLowerCase();
}

export function filterCommands(commands: Command[], query: string): Command[] {
  const words = normalise(query).split(/\s+/).filter(Boolean);
  return commands.filter((command) => {
    const text = normalise(`${command.group} ${command.label}`);
    return words.every((word) => text.includes(word));
  });
}

// ⌘K: a combobox over every command. Arrows move, Enter runs, Esc closes.
export function CommandPalette({ commands, onClose }: { commands: Command[]; onClose: () => void }) {
  const { t } = useTranslation();
  const [query, setQuery] = useState('');
  const [active, setActive] = useState(0);
  const titleId = useId();
  const listId = useId();
  const matches = filterCommands(commands, query);
  const selected = Math.min(active, matches.length - 1);
  const optionId = (index: number) => `${listId}-${index}`;

  const run = (command: Command | undefined) => {
    if (command === undefined) return;
    onClose();
    command.run();
  };

  const onKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
      event.preventDefault();
      const step = event.key === 'ArrowDown' ? 1 : -1;
      setActive((selected + step + matches.length) % Math.max(matches.length, 1));
    } else if (event.key === 'Enter') {
      event.preventDefault();
      run(matches[selected]);
    }
  };

  return (
    <Dialog labelledBy={titleId} onClose={onClose}>
      <h2 id={titleId} className="sr-only">
        {t('palette.title')}
      </h2>
      <div className="flex items-center gap-2.5 border-b border-line px-3.5 py-3">
        <svg width="14" height="14" viewBox="0 0 14 14" aria-hidden="true">
          <path
            d="M6 10.5a4.5 4.5 0 1 0 0-9a4.5 4.5 0 0 0 0 9ZM9.3 9.3L12.5 12.5"
            fill="none"
            stroke="currentColor"
            strokeWidth="1.5"
            className="text-ink-2"
          />
        </svg>
        <input
          role="combobox"
          aria-expanded="true"
          aria-controls={listId}
          aria-autocomplete="list"
          aria-activedescendant={matches.length > 0 ? optionId(selected) : undefined}
          aria-label={t('palette.input')}
          placeholder={t('palette.placeholder')}
          value={query}
          onChange={(event) => {
            setQuery(event.target.value);
            setActive(0);
          }}
          onKeyDown={onKeyDown}
          className="min-w-0 flex-1 bg-transparent text-body text-ink placeholder:text-ink-3 focus-visible:outline-none"
        />
      </div>
      <div id={listId} role="listbox" aria-label={t('palette.results')} className="max-h-[50vh] overflow-y-auto p-1.5">
        {matches.map((command, index) => (
          <div
            key={command.id}
            id={optionId(index)}
            role="option"
            aria-selected={index === selected}
            tabIndex={-1}
            onClick={() => run(command)}
            onKeyDown={(event) => event.key === 'Enter' && run(command)}
            onMouseMove={() => setActive(index)}
            className={`flex cursor-pointer items-center gap-2.5 rounded-1 px-2.5 py-2 text-small ${index === selected ? 'bg-accent-soft' : ''}`}
          >
            <span className="w-20 shrink-0 text-tiny font-bold text-ink-3">{command.group}</span>
            <span className="flex-1">{command.label}</span>
            {command.hint && <Kbd>{command.hint}</Kbd>}
          </div>
        ))}
      </div>
      {matches.length === 0 && (
        <p role="status" className="m-0 px-4 pb-4 text-small text-ink-2">
          {t('palette.empty')}
        </p>
      )}
    </Dialog>
  );
}
