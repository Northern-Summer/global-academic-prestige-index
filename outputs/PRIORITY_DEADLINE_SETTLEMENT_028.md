# Bounded optional source priority

The initial 240-second new-scheduler invocation was started as session 90548. It emitted no acquisition batch before its nominal deadline: the optional cross-source priority query was not covered by that deadline. It was interrupted through its actual process handle (exit 1); no replacement writer was started until that handle settled. This is not a successful network throughput acceptance.

QUEUE_SELECTION_BOUND_PROBE_028.json: same live queue without optional priority selected 200 members in 0.438 seconds, read-only with a one-second SQLite execution guard. This distinguished ordinary queue selection from its optional global-priority pass. LIVE_PRIORITY_QUERY_PLAN_028.json retains the inspected plan, including historical evidence fallback.

CareerStore.pending now bounds only its optional priority pass to two seconds using SQLite's progress handler. A deadline interrupt preserves ordinary fresh selection and due retry reservation, clears its handler, and exposes priority_deferred in per-batch collection progress. Other SQLite errors still propagate. It does not change source evidence, person merging, ranking or queue membership. The existing exact optional priority behavior is retained when it fits the window.

PRIORITY_DEADLINE_REAL_028.json: real queue, read-only, 200 candidates selected in 2.421 seconds, priority deferred true, no personal identifiers exported. No source record was downgraded or dropped.

PRIORITY_DEADLINE_TESTS_028.txt: 20 collection tests passed. A real recursive SQLite query is interrupted under an injected monotonic deadline; due retries/fresh work and retry table remain unchanged, a subsequent normal query succeeds, and unrelated operational errors are not masked. PRIORITY_DEADLINE_REGRESSION_028.txt: all 323 Python tests passed.

This bounded fallback prevents priority from blocking productive acquisition; it does not prove fair service among all persons or full global enumeration. A more efficient exact priority index remains a possible later improvement.
