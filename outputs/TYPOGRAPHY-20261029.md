# Typography correction — 2026-10-29

The user rejected the Chinese redesign: heavy sans-serif headings, loss of the predecessor italic title, and formulaic headings/explanations. This correction restores the existing visual identity rather than introducing another design direction.

- Reference: revision `8e6eca2`, Playfair Display title “Academic influence, measured in movement.”
- Restored regular/italic Playfair, paper/teal palette and title proportions. Chinese section headings use an OFL Noto Serif SC delivery subset at regular weight; body/control/table text stays readable sans serif.
- Replaced rhetorical section headings with “机构与得分”, “人才流动网络”, and “数据与方法”; shortened explanations without changing missing-data, independent-review or score semantics.
- Bundled 138,084 font bytes, source locations, hashes and complete OFL licenses. No browser requests to Google Fonts. Binary font inputs are hashed byte-for-byte; a regression test detects byte changes that UTF-8 replacement would hide.
- 27 Node tests passed. Local production browser passed filtering, sorting, keyboard details, CSV, award attribution, relationship pagination, WebGL and fallback, mobile overflow, hostile numbers/labels, loaded font families and italic styles. Data snapshot remains `6137d26d6e557370d00d2db3dcbf4ebe39aefcb0a4928e25e8dfdf369e703f56`.
- Actual desktop hero/table and mobile hero/table screenshots inspected. No human preference acceptance or causal performance improvement is claimed. Local table readiness was 843 ms; this is one observation, not a controlled benchmark.

Receipts: `outputs/browser-typography-20261029.json`, `outputs/typography-acceptance-20261029.json`. Remote deployment and public browser settlement will be recorded separately after execution. Source scanning and third-party dual-track review remain incomplete; this is an observation release.

Deployment run `40905490029` passed tests/build/browser and published the expected site manifest, but its asset readback encountered HTTP 503. It remains a failed run. Added bounded retries only for transient public GET errors; 403, oversized content and hash mismatches are not accepted. The follow-up run must establish actual recovery. Original log retained in the external work directory.
