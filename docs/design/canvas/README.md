# Millstream web UI canvas (snapshot)

A read-only snapshot of the Claude Design canvas made from the
[UI design brief](../20261015.ui-brief.md), kept so implementation can cite an exact version.

- Source: the "Millstream web UI" Design artifact on claude.ai, version `1792075239-853a` (2026-10-15).
- Each `*.dc.html` is one artboard; `canvas.json` is the board index. They render only inside the Design canvas
  (they load its `support.js` runtime); read them for structure, copy and tokens.
- The canvas stays the source of truth while the design evolves. Refresh this snapshot when a design pass is
  accepted, and record the new version id here.

State of this version: the identity board (`Main.dc.html`, "Design system 0.2") defines the warm palette, the type
and the tokens. Components, Graph, Inbox and Item follow it; the 375 px French boards still use the earlier palette,
and the dark theme is to be re-derived in the next design pass.
