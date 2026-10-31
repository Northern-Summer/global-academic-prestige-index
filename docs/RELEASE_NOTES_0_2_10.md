# 0.2.10 — Reviewed degree interpretation

This code maintenance release adds `career-interpretation/4`. The website's
selected observation scores remain the frozen 101,915-event release; this code
release does not imply that the growing private acquisition has been republished.
No formal ranking or independent external review is claimed.

The derived policy adds six reviewed Wikidata degree identifiers and corrects
the historical Q177232 mapping (son, not a master's degree). Unknown, conflicting
and professional medical degree qualifiers remain unresolved. Original events
and member evidence are immutable. The current interpretation is applied to rank
edges, intensity denominators and fact matching. Public policy counts now exclude
sources whose derivatives are not selected for publication.

All 44,916 retained Wikidata education events were examined: 3,042 additional
events acquired a supported bachelor/master stage and completion year, while 111
previous classifications became unresolved because of conflicting or unsupported
qualifiers. Another 1,121 events acquired a stage but still lack the required
date. No Q177232 education event was present in this retained set; its erroneous
rule was corrected preventively. These are retained-source counts, not global
coverage or accuracy estimates. Raw event hashes before and after were identical,
and repeated normalization left both input and interpretation row counts intact.

The optional one-record ORCID route was tested on four pending candidates.
Extracted affiliation fields agreed, but it sent 152,881 bytes instead of 20,236;
the four request groups took 7.828 seconds instead of 9.047. The sample is small,
and order was alternated. Production retains the two affiliation-only requests;
no universal speedup is claimed. The benchmark saves aggregate diagnostics only,
uses the existing request allowance and writer exclusion, and never saves XML.

Validation: 327 Python tests and 40 frontend tests passed in the final full run.
One initial full run failed the pre-existing temporary-source-pause test; isolated
and subsequent full runs passed. Its root cause is not established and the first
failure remains in the task evidence. Earlier new-test assertion failures were
corrected to use weighted stage amounts and the proper relation field; they are
also retained. No typography or rendering design was changed in this release.

See [the pinned degree definitions](WIKIDATA_DEGREES.md) and
[methodology](METHODOLOGY.md). The task's 2 GiB storage cap, 256 MiB reserve and
512 MiB process cap remain unchanged. Cloud-account routes remain unused.
