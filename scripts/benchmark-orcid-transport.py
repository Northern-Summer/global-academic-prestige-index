"""Bounded live transport comparison; never saves XML or personal identifiers.

Uses the ordinary writer lock, rolling request allowance, HTTP guards and SAX
parser. Does not insert career events or change the production transport.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time

from gapi.career_store import CareerStore
from gapi.orcid_stream import CHUNK, SummaryParser, private_person
from gapi.source_http import PublicHTTP, ORCID_ANONYMOUS_DAILY_READS
from gapi.task import Task, canonical


def measure(store, http, person, sections, *, encoding=None):
    events = []
    byte_count = 0
    started = time.monotonic()
    requests_before = http.requests_count
    wire_before = http.bytes_received

    def emit(event):
        events.append(event)
        if len(events) > store.task.config['batch_events']:
            raise ValueError('affiliation comparison exceeds event bound')

    with store.request_budget('orcid-public-api',
            len(sections) * (store.task.config['max_retries'] + 1),
            ORCID_ANONYMOUS_DAILY_READS, http), http.within(60):
        for section in sections:
            url = f'https://pub.orcid.org/v3.0/{person}/{section}'
            parser = SummaryParser(private_person(person), 'orcid-public-api', person,
                emit, store.task.config['batch_events'],
                section=None if section == 'record' else section)
            options = {'headers': {'Accept-Encoding': encoding}} if encoding else {}
            with http.open(url, 'application/vnd.orcid+xml', **options) as response:
                if 'xml' not in response.headers.get('Content-Type', ''):
                    raise ValueError('expected XML representation')
                while chunk := response.read(CHUNK):
                    byte_count += len(chunk)
                    store.task.check_memory()
                    if byte_count > store.task.config['max_member_bytes']:
                        raise ValueError('comparison member exceeds byte bound')
                    parser.feed(chunk)
                parser.feed(b'', final=True)
    # Byte hashes are not comparable across endpoints. Compare every extracted
    # field, including put-code, source assertion, institution and exact dates.
    fingerprint = hashlib.sha256(b'\n'.join(sorted(map(canonical, events)))).hexdigest()
    return {'seconds': round(time.monotonic() - started, 6),
        'bytes': byte_count, 'events': len(events),
        'wire_bytes': http.bytes_received - wire_before,
        'requests': http.requests_count - requests_before}, fingerprint


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--task', type=Path, default=Path('configs/streaming.json'))
    parser.add_argument('--people', type=int, default=4, choices=range(1, 11))
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--comparison', choices=('endpoints', 'encoding'), default='endpoints')
    parser.add_argument('--selection', choices=('pending', 'committed'), default='pending')
    args = parser.parse_args()
    if args.out.exists() or args.out.is_symlink():
        parser.error('output already exists; choose a new evidence filename')
    task = Task(args.task)
    store = CareerStore(task)
    http = PublicHTTP(task, reuse_connections=True)
    report = {'schema_version': 'gapi-orcid-transport-comparison/v1',
        'retrieved_at': datetime.now(timezone.utc).isoformat(),
        'selection': 'first ' + args.selection + ' candidates; bounded diagnostic, not representative',
        'comparison': args.comparison,
        'raw_saved': False, 'career_events_written': False, 'samples': []}
    try:
        if args.selection == 'committed':
            people = [row[0] for row in store.db.execute('''SELECT locator FROM members
                WHERE source='orcid-public-api' AND status='observed' ORDER BY locator LIMIT ?''', (args.people,))]
        else:
            people = store.pending('ingest-orcid-api', args.people)
        for index, person in enumerate(people):
            routes = ([('identity', ('educations', 'employments')), ('gzip', ('educations', 'employments'))]
                      if args.comparison == 'encoding' else
                      [('sections', ('educations', 'employments')), ('record', ('record',))])
            if index % 2:
                routes.reverse()
            row, fingerprints = {}, []
            for name, sections in routes:
                try:
                    row[name], fingerprint = measure(store, http, person, sections,
                        encoding=name if args.comparison == 'encoding' else None)
                    fingerprints.append(fingerprint)
                except (OSError, RuntimeError, ValueError) as exc:
                    # Do not write exception strings: URL-bearing network errors
                    # can disclose identifiers in an otherwise aggregate report.
                    row[name] = {'error_type': type(exc).__name__}
                    fingerprints.append(None)
                    row['equal_extracted_fields'] = False
                    report['samples'].append(row)
                    raise
            row['equal_extracted_fields'] = None not in fingerprints and len(set(fingerprints)) == 1
            report['samples'].append(row)
            print(json.dumps(row), flush=True)
        report['status'] = 'completed'
    finally:
        try:
            http.close()
            report.setdefault('status', 'incomplete')
            report['used_bytes'] = task.used()
            report['headroom_after_reserve'] = task.config['budget_bytes'] - task.config['reserve_bytes'] - task.used()
            report['working_set_bytes'] = task.check_memory()
            encoded = json.dumps(report, indent=2).encode('utf-8') + b'\n'
            task.check(len(encoded))
            # A diagnostic never overwrites an earlier run's evidence.
            with args.out.open('xb') as output:
                output.write(encoded)
        finally:
            store.close()


if __name__ == '__main__':
    main()
