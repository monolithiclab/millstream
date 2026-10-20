# Millstream web UI canvas (snapshot)

A read-only snapshot of the Claude Design canvas made from the
[UI design brief](../20261015.ui-brief.md), kept so implementation can cite an exact version.

- Source: the "Millstream web UI" Design artifact on claude.ai, version `1792076103-839e` (2026-10-15, design
  pass 0.2 complete).
- Each `*.dc.html` is one artboard; `canvas.json` is the board index. They render only inside the Design canvas
  (they load its `support.js` runtime); read them for structure, copy and tokens.
- The canvas stays the source of truth while the design evolves. Refresh this snapshot when a design pass is
  accepted, and record the new version id here.

State of this version: design pass 0.2 is complete. The identity board (`Main.dc.html`, "Design system 0.2") defines
the warm palette, the type and the tokens for both themes (light first, dark as `[data-theme="dark"]` and
`prefers-color-scheme`); every screen, including the 375 px French boards, uses them.

## Gaps

- **Improve view (M5).** This version has no proposals board: the Improve screen and the proposal page follow the
  components board (cards, status set, segmented filters, confirmation dialog, the "JSON viewer · diff" lines with
  − and + marks and soft line grounds) and the insights screen (filters aside, evidence cards). The diff viewer adds
  what the board does not draw: side by side from 1024 px, syntax tones (accent, human, warn, ink-2, checked on the
  diff grounds in `tokens.test.ts`), a files list. A design pass should add the board.
