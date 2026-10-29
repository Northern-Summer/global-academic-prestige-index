# Biases and limitations

GAPI observes published and discoverable records, not the complete academic labor market. Missing profiles and incomplete career histories can produce missing edges. A low observed score is not evidence of low institutional quality.

Likely bias mechanisms include:

- English-language and web-indexing advantages;
- denser public staffing pages in some regions;
- differences in faculty hiring systems and academic job titles;
- discipline-specific postdoctoral norms and mobility;
- institution size and historical reputation;
- incomplete degree dates and undocumented joint appointments;
- ambiguous identity matches, name variants, and institutional mergers;
- correlated errors when one upstream source is reused;
- feedback from PageRank that can reinforce already-visible institutions;
- award concentration and attribution ambiguity.

The current implementation does not yet estimate uncertainty intervals, perform OOD perturbations, balance discipline or geography, or adjust for the size of each graduating cohort. External rankings can be compared descriptively but cannot serve as ground truth. Any real release must publish source coverage, unranked reasons, sensitivity results, and limitations next to the ranking.
