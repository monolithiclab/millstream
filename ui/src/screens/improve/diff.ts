// A proposal's patch (spec 11.3) is a unified diff of the package as git diff writes it, after a preamble that may
// name its base (Millstream-Base). parsePatch reads its files, hunks and numbered lines for the diff viewer;
// sideBySide pairs removed and added lines into rows; highlight splits a line into syntax tokens.

export type LineKind = 'context' | 'add' | 'del';

export interface DiffLine {
  kind: LineKind;
  text: string;
  // The line's number in the old and new file; a removed line has no new number, an added one no old number.
  old?: number;
  new?: number;
}

export interface Hunk {
  header: string;
  lines: DiffLine[];
}

export type FileOp = 'modify' | 'create' | 'delete';

export interface DiffFile {
  path: string;
  op: FileOp;
  hunks: Hunk[];
  added: number;
  removed: number;
}

const hunkHeader = /^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@/;

// The lines before the first file (the Millstream-Base trailer, a message) are not shown.
export function parsePatch(text: string): DiffFile[] {
  const lines = text.split('\n');
  const files: DiffFile[] = [];
  let file: DiffFile | null = null;
  let i = 0;
  while (i < lines.length) {
    const line = lines[i] ?? '';
    i += 1;
    if (line.startsWith('diff --git ')) {
      // diff --git a/<path> b/<path>: the new path names the file.
      const path = line.slice(line.lastIndexOf(' b/') + 3);
      file = { path, op: 'modify', hunks: [], added: 0, removed: 0 };
      files.push(file);
      continue;
    }
    if (file === null) continue;
    if (line.startsWith('new file mode') || line === '--- /dev/null') file.op = 'create';
    else if (line.startsWith('deleted file mode') || line === '+++ /dev/null') file.op = 'delete';
    const header = hunkHeader.exec(line);
    if (header === null) continue;
    let oldLine = Number(header[1]);
    let newLine = Number(header[3]);
    let oldLeft = header[2] === undefined ? 1 : Number(header[2]);
    let newLeft = header[4] === undefined ? 1 : Number(header[4]);
    const hunk: Hunk = { header: line, lines: [] };
    file.hunks.push(hunk);
    // A hunk holds as many lines as its header counts; "\ No newline at end of file" is not one.
    while ((oldLeft > 0 || newLeft > 0) && i < lines.length) {
      const body = lines[i] ?? '';
      i += 1;
      const mark = body[0];
      const rest = body.slice(1);
      if (mark === '\\') continue;
      if (mark === '+') {
        hunk.lines.push({ kind: 'add', text: rest, new: newLine++ });
        file.added += 1;
        newLeft -= 1;
      } else if (mark === '-') {
        hunk.lines.push({ kind: 'del', text: rest, old: oldLine++ });
        file.removed += 1;
        oldLeft -= 1;
      } else {
        hunk.lines.push({ kind: 'context', text: rest, old: oldLine++, new: newLine++ });
        oldLeft -= 1;
        newLeft -= 1;
      }
    }
  }
  return files;
}

// A row of the side-by-side view: the old line on the left, the new on the right; a change pairs its removed and
// added lines in order, and the longer side runs on alone.
export interface Row {
  left?: DiffLine;
  right?: DiffLine;
}

export function sideBySide(hunk: Hunk): Row[] {
  const rows: Row[] = [];
  let dels: DiffLine[] = [];
  let adds: DiffLine[] = [];
  const flush = () => {
    for (let k = 0; k < Math.max(dels.length, adds.length); k += 1) {
      const row: Row = {};
      const [left, right] = [dels[k], adds[k]];
      if (left !== undefined) row.left = left;
      if (right !== undefined) row.right = right;
      rows.push(row);
    }
    dels = [];
    adds = [];
  };
  for (const line of hunk.lines) {
    if (line.kind === 'del') {
      if (adds.length > 0) flush();
      dels.push(line);
    } else if (line.kind === 'add') {
      adds.push(line);
    } else {
      flush();
      rows.push({ left: line, right: line });
    }
  }
  flush();
  return rows;
}

export type Language = 'yaml' | 'markdown' | 'go' | 'plain';

export function languageOf(path: string): Language {
  if (/\.ya?ml$/i.test(path)) return 'yaml';
  if (/\.(md|markdown)$/i.test(path)) return 'markdown';
  if (/\.go$/i.test(path)) return 'go';
  return 'plain';
}

export type TokenKind = 'key' | 'keyword' | 'string' | 'number' | 'comment' | 'heading' | 'plain';

export interface Token {
  kind: TokenKind;
  text: string;
}

const goKeywords = new Set(
  'break case chan const continue default defer else fallthrough for func go goto if import interface map package range return select struct switch type var nil true false'.split(
    ' ',
  ),
);

// Each language's tokens within a line, tried in order at each position: the first group that matches names it.
const rules: Record<Exclude<Language, 'plain'>, [TokenKind, RegExp][]> = {
  yaml: [
    ['comment', /(?:^|(?<=\s))#.*/y],
    ['string', /"(?:[^"\\]|\\.)*"|'[^']*'/y],
    ['number', /\b(?:-?\d+(?:\.\d+)?|true|false|null|yes|no)\b/y],
  ],
  go: [
    ['comment', /\/\/.*/y],
    ['string', /"(?:[^"\\]|\\.)*"|`[^`]*`|'(?:[^'\\]|\\.)'/y],
    ['number', /\b\d+(?:\.\d+)?\b/y],
    ['keyword', /\b[A-Za-z_]\w*\b/y],
  ],
  markdown: [
    ['string', /`[^`]*`/y],
    ['keyword', /\*\*[^*]+\*\*|__[^_]+__/y],
  ],
};

function scan(text: string, language: Exclude<Language, 'plain'>, out: Token[]): void {
  let plain = '';
  let at = 0;
  const push = (kind: TokenKind, value: string) => {
    if (plain !== '') out.push({ kind: 'plain', text: plain });
    plain = '';
    out.push({ kind, text: value });
  };
  outer: while (at < text.length) {
    for (const [kind, re] of rules[language]) {
      re.lastIndex = at;
      const m = re.exec(text);
      if (m === null || m[0] === '') continue;
      // A Go word is a keyword only when Go reserves it.
      if (language === 'go' && kind === 'keyword' && !goKeywords.has(m[0])) {
        plain += m[0];
      } else {
        push(kind, m[0]);
      }
      at += m[0].length;
      continue outer;
    }
    plain += text[at];
    at += 1;
  }
  if (plain !== '') out.push({ kind: 'plain', text: plain });
}

const yamlKey = /^(\s*(?:-\s+)?)((?:"[^"]*"|'[^']*'|[^\s#'"{[][^:#]*?)):(?=\s|$)/;
const markdownHeading = /^#{1,6}\s/;
const markdownMarker = /^(\s*)([-*+]|\d+\.)(\s)/;

export function highlight(text: string, language: Language): Token[] {
  if (language === 'plain' || text === '') return [{ kind: 'plain', text }];
  const out: Token[] = [];
  let rest = text;
  if (language === 'yaml') {
    const key = yamlKey.exec(text);
    if (key !== null) {
      const [whole, indent = '', name = ''] = key;
      if (indent !== '') out.push({ kind: 'plain', text: indent });
      out.push({ kind: 'key', text: name }, { kind: 'plain', text: ':' });
      rest = text.slice(whole.length);
    }
  } else if (language === 'markdown') {
    if (markdownHeading.test(text)) return [{ kind: 'heading', text }];
    const marker = markdownMarker.exec(text);
    if (marker !== null) {
      const [, indent = '', bullet = ''] = marker;
      if (indent !== '') out.push({ kind: 'plain', text: indent });
      out.push({ kind: 'keyword', text: bullet });
      rest = text.slice(indent.length + bullet.length);
    }
  }
  scan(rest, language, out);
  return out;
}
