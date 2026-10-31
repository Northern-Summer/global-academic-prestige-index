# Wikidata degree interpretation

The authoritative derived vocabulary is `WIKIDATA_DEGREES` in
`src/gapi/career_normalization.py`, under `career-interpretation/4` and its policy
hash. The table below records the official entity definitions reviewed on
2026-10-31. These are interpretation rules, not independent evidence that a person
obtained a degree. Only explicit P512 qualifiers on education statements qualify;
P69 alone, occupation, name similarity and publication affiliations do not.

| Identifier | Definition | Derived stage | Pinned official item |
| --- | --- | --- | --- |
| Q752297 | Doctor of Philosophy | doctorate | [revision 2557359775](https://www.wikidata.org/w/index.php?title=Q752297&oldid=2557359775) |
| Q849697 | doctorate | doctorate | [revision 2555294489](https://www.wikidata.org/w/index.php?title=Q849697&oldid=2555294489) |
| Q183816 | master's degree | master | [revision 2554937153](https://www.wikidata.org/w/index.php?title=Q183816&oldid=2554937153) |
| Q950900 | Master of Science | master | [revision 2549021092](https://www.wikidata.org/w/index.php?title=Q950900&oldid=2549021092) |
| Q2091008 | Master of Arts | master | [revision 2530003554](https://www.wikidata.org/w/index.php?title=Q2091008&oldid=2530003554) |
| Q163727 | bachelor's degree | bachelor | [revision 2553307389](https://www.wikidata.org/w/index.php?title=Q163727&oldid=2553307389) |
| Q1765120 | Bachelor of Arts | bachelor | [revision 2550544271](https://www.wikidata.org/w/index.php?title=Q1765120&oldid=2550544271) |
| Q787674 | Bachelor of Science | bachelor | [revision 2551564279](https://www.wikidata.org/w/index.php?title=Q787674&oldid=2551564279) |
| Q798132 | Bachelor of Engineering | bachelor | [revision 2559107750](https://www.wikidata.org/w/index.php?title=Q798132&oldid=2559107750) |
| Q177232 | son | excluded: not a degree | [revision 2559770251](https://www.wikidata.org/w/index.php?title=Q177232&oldid=2559770251) |
| Q913404 | Doctor of Medicine | unresolved: professional medical qualification | [revision 2544578550](https://www.wikidata.org/w/index.php?title=Q913404&oldid=2544578550) |

All explicit qualifier values must be recognized and agree on one stage. Unknown
or conflicting qualifiers remain unresolved. A missing completion year or
institution prevents admission to the observed network. Rejected evidence stays
rejected. A generic doctorate does not establish that the degree is a research
PhD; the network's doctorate layer retains that limitation.

The frozen acquisition/v1 vocabulary in `global_sources.py` incorrectly mapped
Q177232 to master and omitted several common degree types. It remains solely as
the historical acquisition contract so raw member hashes and old freezes remain
recoverable. Normalization stores separate versioned corrections, including
withdrawal of that wrong classification, and does not mutate raw events. Rank
edges, observed training denominators and exact-fact corroboration consume the
current interpretation policy. Qualifier interpretation never promotes a record
to independent corroboration. Code revisions pinned to earlier releases retain
their earlier policies; current code must not be represented as a byte-identical
replay of a different policy version.

The bounded 2,000-event diagnostic that prompted this change was a source-index
prefix, not a random sample or a global pass-rate estimate. Coverage improvement
must be measured on the actual retained events after applying this policy; it
does not measure the completeness of the world's academic population.
