import json
import unittest
from contextlib import nullcontext
from pathlib import Path

from gapi.nobel import ingest_nobel, load_source, normalize_laureate, public_inventory
from gapi.source_http import SourcePause
import test_streaming as fixtures
from test_streaming import archive


def laureate(identifier='1', *, organization=False, affiliations=None):
    return {'id': identifier, 'orgName' if organization else 'knownName': {'en': 'PRIVATE RECIPIENT'},
        'nobelPrizes': [{'awardYear': '2000', 'category': {'en': 'Physics'}, 'portion': '1/2',
            'affiliations': affiliations if affiliations is not None else [
                {'name': {'en': 'Official University'}, 'city': {'en': 'City'}, 'country': {'en': 'Country'}}]}]}


class HTTP:
    def __init__(self, pages): self.pages = iter(pages); self.urls = []
    def within(self, _seconds): return nullcontext()
    def json(self, url, limit):
        self.urls.append(url)
        value = next(self.pages)
        if isinstance(value, Exception): raise value
        return value, 'a'*64


def page(offset, rows, total):
    return {'laureates': rows, 'meta': {'offset': offset, 'limit': 25, 'count': total}}


class NobelTests(unittest.TestCase):
    setUp = fixtures.StreamingTests.setUp
    tearDown = fixtures.StreamingTests.tearDown
    store = fixtures.StreamingTests.store

    def spec(self): return load_source(Path('configs/nobel-20261029.json'))

    def test_complete_inventory_is_deduplicated_private_and_idempotent(self):
        s = self.store(archive([]))
        try:
            item = laureate()
            item['nobelPrizes'][0]['affiliations'] *= 2
            result = ingest_nobel(s, HTTP([page(0, [item, laureate('2', organization=True)], 2)]), self.spec())
            self.assertEqual(result['status'], 'enumerated-live-selection')
            self.assertEqual(result['processed_recipients'], 2)
            self.assertEqual(result['award_events'], 2)
            self.assertEqual(result['affiliations'][0]['award_mentions'], 2)
            self.assertEqual(result['recipient_types'], {'institutional': 1, 'personal': 1})
            self.assertNotIn('PRIVATE RECIPIENT', json.dumps(result))
            self.assertNotIn('recipient_key', json.dumps(result))
            self.assertNotIn('nobel:1', json.dumps(result))
            self.assertEqual(result, ingest_nobel(s, HTTP([]), self.spec()))
            self.assertFalse(any(p.suffix in ('.xml', '.gz') for p in s.task.root.iterdir()))
        finally: s.close()

    def test_failed_page_preserves_previous_cursor_and_resume(self):
        s = self.store(archive([]))
        try:
            # A short page before total is rejected: offset must represent a complete page.
            first = [laureate(str(i)) for i in range(1,26)]
            response = ingest_nobel(s, HTTP([page(0, first, 26), OSError('offline')]), self.spec())
            self.assertEqual(response['status'], 'resumable-source-boundary')
            self.assertEqual(s.db.execute('SELECT sum(rows) FROM award_pages').fetchone()[0], 25)
            http = HTTP([page(25, [laureate('26')], 26)])
            result = ingest_nobel(s, http, self.spec())
            self.assertEqual(result['processed_recipients'],26)
            self.assertIn('offset=25', http.urls[0])
        finally: s.close()

    def test_drift_duplicate_and_malformed_pages_do_not_advance(self):
        for second in (page(25,[laureate('1')],26),page(25,[laureate('26')],27),[],
                       page(24,[laureate('26')],26)):
            with self.subTest(second=type(second).__name__):
                s=self.store(archive([]))
                try:
                    first=[laureate(str(i)) for i in range(1,26)]
                    result=ingest_nobel(s,HTTP([page(0,first,26),second]),self.spec())
                    self.assertEqual(result['status'],'resumable-source-boundary')
                    self.assertEqual(s.db.execute('SELECT count(*) FROM official_awards').fetchone()[0],25)
                    self.assertEqual(s.db.execute('SELECT count(*) FROM award_pages').fetchone()[0],1)
                finally:s.close()

    def test_page_transaction_rolls_back_awards_and_cursor(self):
        import sqlite3
        from gapi.nobel import TABLES
        s=self.store(archive([]))
        try:
            s.db.executescript(TABLES)
            s.db.execute('''CREATE TRIGGER fail_award_page BEFORE INSERT ON award_pages
                BEGIN SELECT RAISE(ABORT,'injected commit failure'); END''')
            with self.assertRaises(sqlite3.IntegrityError):
                ingest_nobel(s,HTTP([page(0,[laureate()],1)]),self.spec())
            self.assertEqual(s.db.execute('SELECT count(*) FROM official_awards').fetchone()[0],0)
            self.assertEqual(s.db.execute('SELECT count(*) FROM award_pages').fetchone()[0],0)
        finally:s.close()

    def test_source_identity_and_rate_limit_are_persistent(self):
        s=self.store(archive([]))
        try:
            ingest_nobel(s,HTTP([SourcePause(429,10**12,'rate limit')]),self.spec())
            http=HTTP([])
            self.assertEqual(ingest_nobel(s,http,self.spec())['status'],'server-backoff-pending')
            self.assertEqual(http.urls,[])
            changed=self.spec();changed['terms_checked_on']='2026-10-30'
            with self.assertRaisesRegex(ValueError,'changed'):ingest_nobel(s,HTTP([]),changed)
        finally:s.close()

    def test_award_types_and_unattributed_awards_are_not_guessed(self):
        item=laureate(affiliations=[])
        recipient, rows=normalize_laureate(item)
        self.assertEqual(json.loads(rows[0][-1]),[])
        self.assertEqual(rows[0][4],'personal')
        item['nobelPrizes'][0]['category']['en']='IEEE Milestone'
        with self.assertRaises(ValueError):normalize_laureate(item)
        item=laureate(); item['nobelPrizes'] *= 2
        with self.assertRaisesRegex(ValueError,'duplicate'):normalize_laureate(item)

    def test_short_nonterminal_page_is_not_accepted_as_complete(self):
        s=self.store(archive([]))
        try:
            result=ingest_nobel(s,HTTP([page(0,[laureate()],26)]),self.spec())
            self.assertEqual(result['status'],'resumable-source-boundary')
            self.assertEqual(s.db.execute('SELECT count(*) FROM award_pages').fetchone()[0],0)
        finally:s.close()

    def test_old_frozen_input_has_no_new_inventory_and_keeps_scores(self):
        s=self.store(archive([]))
        try:self.assertIsNone(public_inventory(s))
        finally:s.close()


if __name__ == '__main__': unittest.main()
