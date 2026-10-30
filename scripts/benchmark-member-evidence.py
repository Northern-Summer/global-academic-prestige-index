"""Same-frozen-input comparison; stores only aggregate hashes and timings.

No raw data download, no public export mutation. Uses the existing private input
and task budget. The legacy path is retained here solely as a benchmark control.
"""
import hashlib
import json
from pathlib import Path
import time

from gapi import corroboration, global_analysis
from gapi.career_store import CareerStore, member_evidence_sql
from gapi.frozen_input import frozen_reader
from gapi.task import Task, canonical


def legacy(*, observed_only=True):
    state = " AND m.status='observed'" if observed_only else ''
    return ("EXISTS(SELECT 1 FROM members m WHERE m.source=e.source" + state +
        " AND (m.locator=e.locator OR e.locator LIKE m.locator||'#%'))")


def main():
    manifest = json.loads(Path('outputs/global/manifest.json').read_text())
    policy = manifest.get('publication_policy')
    task = Task(Path('configs/streaming.json'))
    task.activate_runtime()
    store = None
    try:
        store = CareerStore(task)
        measurements = []
        with frozen_reader(store, manifest['private_normalized_input']['filename']):
            for name, predicate in [('legacy', legacy), ('indexed', member_evidence_sql)]:
                global_analysis.member_evidence_sql = corroboration.member_evidence_sql = predicate
                start = time.perf_counter()
                content = canonical(global_analysis.rank_global(store, public=True, publication_policy=policy))
                measurement = dict(kind=name, seconds=round(time.perf_counter()-start, 6),
                    sha256=hashlib.sha256(content).hexdigest(), bytes=len(content))
                measurements.append(measurement)
                print(json.dumps(measurement), flush=True)
        same = all(item['sha256'] == manifest['output_sha256'] for item in measurements)
        report = dict(schema_version='gapi-membership-benchmark/v1',
            scope='same private frozen input, sequential same-process public graph computation',
            frozen_input_sha256=manifest['private_normalized_input']['sha256'],
            measurements=measurements, same_output_bytes=same,
            network_access=False, raw_input_saved=False,
            limitations='single paired observation; warm caches and machine load may affect timing')
        Path('outputs/membership-benchmark-20261030.json').write_text(json.dumps(report, indent=2)+'\n')
        if not same:
            raise AssertionError('public output changed from the frozen baseline')
    finally:
        global_analysis.member_evidence_sql = corroboration.member_evidence_sql = member_evidence_sql
        if store:
            store.close()
        task.restore_runtime()


if __name__ == '__main__':
    main()
