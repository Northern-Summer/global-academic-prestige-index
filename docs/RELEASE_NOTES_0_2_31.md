Evidence URL migration now releases reusable SQLite pages, rather than only
shrinking values inside existing pages. New private row archives restore URL
text before encoding, keeping logical inputs independent of this storage codec.

The production migration preserved all restored evidence fields and rowids:

- 82,520 URLs encoded; field payload reduced by 33,962,562 bytes.
- 43,069,440 bytes became reusable database pages. The file did not shrink.
- The full logical evidence hash was unchanged; SQLite checks passed and a
  repeated converted range changed zero rows.
- Reading 1,024 production evidence rows took a median 58.5 ms before and
  109.5 ms after conversion, within the predefined 117.0 ms limit.
- All 425 Python regression tests passed, including rollback, hostile frames,
  dependencies, row identity and identical logical archives before/after encoding.

This is a storage maintenance release. The published dataset remains 282,901
events and 80,668 people; scores, fonts and layout are unchanged. Acquisition
reached 330,504 stored events before maintenance, but their new frozen input hit
the storage limit. That attempt was rejected and its staging file cleaned; these
additional events are not included in this release. Collection has resumed using
the reusable pages and the URL codec.

World coverage is incomplete. Independent external review remains pending.
