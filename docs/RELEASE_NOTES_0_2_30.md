Evidence transport URLs can now use bounded inline compression. The reader
restores the exact original text, including in private input archives; it
requires no additional dictionary table. Short URLs and older TEXT stay valid.

The current published observations, scores, fonts and layout are unchanged.
Newly collected events are not included in this maintenance release.

- A 4,096-row real-evidence replica reduced URL payload from 2,892,403 to
  1,093,991 bytes, with the complete restored row hash unchanged.
- Reading 1,024 shuffled records took a median 3.5 ms before and 10.1 ms after
  compression. This accepted storage tradeoff is a bounded sample measurement.
- Regression checks cover corrupt and oversized frames, rollback, repeatability,
  row identities, archive restoration and unchanged corroboration decisions.
- `repack-evidence-urls` supports a bounded migration with an explicit checkpoint.
  Whole-database migration and its actual space saving have not been measured.

The frozen publication remains 282,901 events and 80,668 people. Global source
enumeration and independent external review remain incomplete.
