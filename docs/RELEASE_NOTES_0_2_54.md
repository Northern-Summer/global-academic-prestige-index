# v0.2.54-observations

The current ORCID 2026 observation and retained 2023 historical view now use the same public vocabulary, source-version explanations and export units. The existing academic title and substantive methodology are preserved. The default page opens the current view; the historical graph loads only when selected. Reader-facing method, source and limitation links open a Chinese HTML guide with return navigation. Technical research materials remain available through an explicitly labelled research link.

## Record reconciliation

The input remains 14,025,102 people and 28,043,519 education/employment records from the same two frozen stores. Original source statuses remain unchanged: 5,605,069 observed and 22,438,450 unresolved. They describe the stored source interpretation, not unread files. New derived disposition is a separate exhaustive partition:

| Primary disposition | Records |
| --- | ---: |
| Conflicting evidence or dates | 12 |
| Explicit exclusions | 206,678 |
| Missing confirmable institution | 5,803,814 |
| Missing explicit qualification or job type | 14,893,739 |
| Missing necessary date | 1,119,357 |
| Meets current identification conditions | 6,019,919 |
| Total | 28,043,519 |

The existing reviewed lexical rules were applied consistently across ORCID snapshots. They changed 588,605 interpretations and newly identified 416,625 records satisfying institution, type and date conditions. This is a derived interpretation; stored source bytes were not rewritten. The difference between the old observed count and the new identified count is not the number repaired, because the disposition additionally applies exclusions and conflict precedence.

Of 2,473,948 people represented in retained historical projections, 2,463,710 also occur in current inputs. There are 3,672,994 exact person/projection matches, including 36,114 stage disagreements whose current interpretation is preserved. The 6,009,263 historical affiliations absent from those current records are historical comparison results, not restored current facts or independently verified evidence. The comparison denominator covers retained projections only.

The governed graph contains 899,624 deduplicated relationships, 586,731 weighted edges and 299,046 institution identifiers; 72,454 institutions have observation scores. The previous release had 806,592 relationships, 532,449 edges and 68,248 scored institutions. A total of 479,926 people contribute qualifying relationships. There is no additional acquisition, annual download, inferred year or independent-evidence promotion.

## Validation and delivery

Independent full-graph replay reproduced reference scores with maximum absolute error 8.674e-19 and training-denominator-adjusted scores with error 2.169e-19. Probability mass is 0.9999999999999988 and stable ordering agrees. Twelve reference and two expanded sensitivity scenarios, including all 72,454 rank ranges, passed independent replay. All 28,043,519 events have exactly one primary disposition; overlapping missing-field flags are separately identified.

Local validation includes the full Python regression suite, 17 affected partition regressions, 81 web regressions and real production-build browser tests. The source-anchor adapter now reads the producer-owned source-manifest.json directly without weakening hash, path or source checks. The public build emits the two small hash-bound guide metadata assets alongside its complete 19,554,521-byte transfer, so the reader guide does not download the complete graph. The latest exact CI, browser and deployment results are bound to this commit and its Release validation attachment.

The frozen person traversal took 3,671.703 seconds. The export process peak observed from the operating system was 505,278,464 bytes, within 512 MiB; process exit leaves a one-second sampling boundary. The prior and current calculations share the original cumulative four-hour allowance, 32 GB storage budget and 256 MiB write reserve. Reliable predecessor and governed checkpoints, source freezes and both private databases remain local.

Global population coverage and independent personal-fact corroboration remain incomplete. These are observation-network indicators, not official global university rankings, employment probabilities, overall university quality or causal training effects. Remaining missing facts and source restrictions are retained explicitly. Individual identifiers, raw roles, private histories and diagnostics are excluded from public artifacts.

[Public site](https://northern-summer.github.io/global-academic-prestige-index/) · [Reader guide](https://northern-summer.github.io/global-academic-prestige-index/guide.html) · [Versioned methodology](https://github.com/Northern-Summer/global-academic-prestige-index/blob/v0.2.54-observations/docs/METHODOLOGY.md).
