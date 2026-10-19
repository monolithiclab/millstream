import { describe, expect, it } from 'vitest';
import { highlight, languageOf, parsePatch, sideBySide, type Token } from './diff';

const patch = `Millstream-Base: sha256:9f2c41d07a

diff --git a/pipeline.yaml b/pipeline.yaml
index 7b8c9d0..a9b8c7d 100644
--- a/pipeline.yaml
+++ b/pipeline.yaml
@@ -30,4 +30,5 @@ steps:
   score:
-    action: agent.score
+    action: score-code
+    fallback: { action: agent.score }
     in: enriched
     concurrency: 2
diff --git a/actions/x/main.go b/actions/x/main.go
new file mode 100644
--- /dev/null
+++ b/actions/x/main.go
@@ -0,0 +1,2 @@
+package main
+func main() {}
\\ No newline at end of file
diff --git a/old.md b/old.md
deleted file mode 100644
--- a/old.md
+++ /dev/null
@@ -1 +0,0 @@
-# Gone
`;

const kinds = (tokens: Token[]) => tokens.map((t) => `${t.kind}:${t.text}`);

describe('parsePatch', () => {
  it('reads the files with their operation and numbered lines, past the preamble', () => {
    const files = parsePatch(patch);
    expect(files.map((f) => [f.path, f.op, f.added, f.removed])).toEqual([
      ['pipeline.yaml', 'modify', 2, 1],
      ['actions/x/main.go', 'create', 2, 0],
      ['old.md', 'delete', 0, 1],
    ]);
    const [hunk] = files[0]!.hunks;
    expect(hunk!.header).toBe('@@ -30,4 +30,5 @@ steps:');
    expect(hunk!.lines).toEqual([
      { kind: 'context', text: '  score:', old: 30, new: 30 },
      { kind: 'del', text: '    action: agent.score', old: 31 },
      { kind: 'add', text: '    action: score-code', new: 31 },
      { kind: 'add', text: '    fallback: { action: agent.score }', new: 32 },
      { kind: 'context', text: '    in: enriched', old: 32, new: 33 },
      { kind: 'context', text: '    concurrency: 2', old: 33, new: 34 },
    ]);
    expect(files[1]!.hunks[0]!.lines.map((l) => l.new)).toEqual([1, 2]);
    expect(files[2]!.hunks[0]!.lines).toEqual([{ kind: 'del', text: '# Gone', old: 1 }]);
  });

  it('reads a patch without a preamble, or empty', () => {
    expect(parsePatch('')).toEqual([]);
    expect(parsePatch(patch.slice(patch.indexOf('diff')))).toHaveLength(3);
  });
});

describe('sideBySide', () => {
  it('pairs removed and added lines, the longer side running on alone', () => {
    const rows = sideBySide(parsePatch(patch)[0]!.hunks[0]!);
    expect(rows.map((r) => [r.left?.old ?? null, r.right?.new ?? null])).toEqual([
      [30, 30],
      [31, 31],
      [null, 32],
      [32, 33],
      [33, 34],
    ]);
  });

  it('starts a new change when a removal follows additions', () => {
    const rows = sideBySide({
      header: '',
      lines: [
        { kind: 'add', text: 'a', new: 1 },
        { kind: 'del', text: 'b', old: 1 },
      ],
    });
    expect(rows).toEqual([{ right: { kind: 'add', text: 'a', new: 1 } }, { left: { kind: 'del', text: 'b', old: 1 } }]);
  });
});

describe('highlight', () => {
  it('knows the languages of a package by extension', () => {
    expect(['a.yaml', 'b.YML', 'prompt.md', 'main.go', 'Dockerfile'].map(languageOf)).toEqual([
      'yaml',
      'yaml',
      'markdown',
      'go',
      'plain',
    ]);
  });

  it('marks YAML keys, strings, scalars and comments', () => {
    expect(kinds(highlight("  - text: 'Cap it' # why", 'yaml'))).toEqual([
      'plain:  - ',
      'key:text',
      'plain::',
      'plain: ',
      "string:'Cap it'",
      'plain: ',
      'comment:# why',
    ]);
    expect(kinds(highlight('    retry: { attempts: 3, ok: true }', 'yaml'))).toContain('number:3');
    expect(kinds(highlight('# a comment', 'yaml'))).toEqual(['comment:# a comment']);
  });

  it('marks Go keywords, strings, numbers and comments, not other words', () => {
    expect(kinds(highlight('if x == 0 { return "no" } // done', 'go'))).toEqual([
      'keyword:if',
      'plain: x == ',
      'number:0',
      'plain: { ',
      'keyword:return',
      'plain: ',
      'string:"no"',
      'plain: } ',
      'comment:// done',
    ]);
  });

  it('marks Markdown headings, list markers, code and strong text', () => {
    expect(kinds(highlight('## Output', 'markdown'))).toEqual(['heading:## Output']);
    expect(kinds(highlight('- use `get_comparables` **only**', 'markdown'))).toEqual([
      'keyword:-',
      'plain: use ',
      'string:`get_comparables`',
      'plain: ',
      'keyword:**only**',
    ]);
  });

  it('leaves plain text and empty lines alone', () => {
    expect(highlight('FROM x', 'plain')).toEqual([{ kind: 'plain', text: 'FROM x' }]);
    expect(highlight('', 'go')).toEqual([{ kind: 'plain', text: '' }]);
  });
});
