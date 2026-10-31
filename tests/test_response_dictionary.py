import json
import unittest
import zlib
from unittest.mock import patch

import test_streaming as fixtures
import test_global_analysis as analysis
from test_streaming import archive
from gapi.task import canonical, BudgetStop
from gapi.event_storage import (pack, unpack, DICT_MAGIC, MAX_DICTIONARY_BYTES,
    logical_event_hash, repack_event_pages)
from gapi.global_analysis import rank_global
from gapi.frozen_input import freeze_input, frozen_reader
from gapi.row_snapshot import freeze_rows

URL='https://www.wikidata.org/w/api.php?action=wbgetentities&ids='+ '%7C'.join('Q'+str(i) for i in range(200))


class ResponseDictionaryTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def seed(self,s):
        analysis.AnalysisTests.seed(self,s)
        for identifier,text in s.db.execute('SELECT id,payload FROM events').fetchall():
            obj=json.loads(text);obj.update(response_url=URL,transport='wikibase-claims-info-batch/v1')
            with s.db:s.db.execute('UPDATE events SET payload=? WHERE id=?',(pack(canonical(obj).decode()),identifier))

    def test_exact_codec_and_corrupt_or_missing_dictionary_fail_closed(self):
        dictionary=URL.encode();text=canonical({'response_url':URL,'evidence':'真实履历'}).decode()
        encoded=pack(text,dictionary=dictionary)
        self.assertTrue(encoded.startswith(DICT_MAGIC))
        self.assertEqual(unpack(encoded,dictionary_lookup=lambda key:dictionary),text)
        with self.assertRaisesRegex(ValueError,'lookup required'):unpack(encoded)
        with self.assertRaisesRegex(ValueError,'integrity'):unpack(encoded,dictionary_lookup=lambda key:b'wrong')
        for value in [encoded[:-1],encoded+b'extra',DICT_MAGIC+b'\x00'*4]:
            with self.assertRaises(ValueError):unpack(value,dictionary_lookup=lambda key:dictionary)
        with self.assertRaises(ValueError):pack(text,dictionary=b'x'*(MAX_DICTIONARY_BYTES+1))

    def test_migration_preserves_scores_hash_snapshot_udf_and_idempotence(self):
        s=self.store(archive([]))
        try:
            self.seed(s);before=logical_event_hash(s.db);rank=canonical(rank_global(s,public=True))
            original=s.db.execute('SELECT id,payload FROM events ORDER BY id').fetchall()
            result=repack_event_pages(s,share_responses=True)
            self.assertEqual(result['shared_response_events'],4)
            self.assertGreater(result['event_payload_bytes_saved'],0)
            self.assertFalse(result['stored_rows_unchanged'])
            self.assertEqual(s.db.execute('SELECT count(*) FROM event_dictionaries').fetchone()[0],1)
            self.assertEqual(logical_event_hash(s.db),before)
            self.assertEqual(canonical(rank_global(s,public=True)),rank)
            self.assertEqual(s.db.execute('SELECT id,gapi_event_text(payload) FROM events ORDER BY id').fetchall(),original)
            second=repack_event_pages(s,share_responses=True)
            self.assertEqual(second['shared_response_events'],0)
            frozen=freeze_input(s)
            with frozen_reader(s,frozen['filename']):
                self.assertEqual(logical_event_hash(s.db),before)
                self.assertEqual(canonical(rank_global(s,public=True)),rank)
                self.assertEqual(s.db.execute('SELECT count(*) FROM event_dictionaries').fetchone()[0],1)
            scoped=freeze_input(s,scope='global-ranking')
            with frozen_reader(s,scoped['filename']):
                self.assertEqual(logical_event_hash(s.db),before)
                self.assertEqual(canonical(rank_global(s,public=True)),rank)
                self.assertIsNone(s.db.execute("SELECT name FROM sqlite_master WHERE name='event_dictionaries'").fetchone())
            frozen_rows=freeze_rows(s)
            with frozen_reader(s,frozen_rows['filename']):
                self.assertEqual(logical_event_hash(s.db),before)
                self.assertEqual(canonical(rank_global(s,public=True)),rank)
                self.assertIsNone(s.db.execute("SELECT name FROM sqlite_master WHERE name='event_dictionaries'").fetchone())
            # Every read validates dependencies; no cache hides subsequent damage.
            with s.db:s.db.execute('UPDATE event_dictionaries SET compressed_bytes=?',(zlib.compress(b'wrong'),))
            with self.assertRaises(ValueError):s.db.execute('SELECT payload FROM events').fetchall()
        finally:s.close()

    def test_new_ingest_is_atomic_and_missing_dependency_is_rejected(self):
        s=self.store(archive([]))
        obj=dict(person='p',institution='wd:Q1',kind='education',stage='doctorate',year_end=2000,
                 source='wikidata',locator='one',status='observed',response_url=URL,transport='wikibase-claims-info-batch/v1')
        try:
            with self.assertRaisesRegex(RuntimeError,'injected'):
                with s.member('wikidata','one'):
                    s.event(obj);raise RuntimeError('injected')
            self.assertEqual(s.db.execute('SELECT count(*) FROM events').fetchone()[0],0)
            self.assertEqual(s.db.execute('SELECT count(*) FROM event_dictionaries').fetchone()[0],0)
            with s.member('wikidata','one'):s.event(obj)
            self.assertEqual(s.db.execute('SELECT payload FROM events').fetchone()[0],canonical(obj).decode())
            with s.db:s.db.execute('DELETE FROM event_dictionaries')
            with self.assertRaisesRegex(ValueError,'missing'):s.db.execute('SELECT payload FROM events').fetchall()
        finally:s.close()

    def test_migration_fault_rolls_back_dictionary_and_events(self):
        s=self.store(archive([]))
        try:
            self.seed(s);before=logical_event_hash(s.db)
            with patch.object(s.task,'check_memory',side_effect=[0,BudgetStop('injected')]):
                with self.assertRaises(BudgetStop):repack_event_pages(s,share_responses=True)
            self.assertFalse(s.db.in_transaction)
            self.assertEqual(logical_event_hash(s.db),before)
            self.assertEqual(s.db.execute('SELECT count(*) FROM event_dictionaries').fetchone()[0],0)
        finally:s.close()

    def test_source_seed_ingest_rollback_bounds_and_canonical_snapshot(self):
        from gapi.event_storage import source_dictionary
        s=self.store(archive([]),source_compression=True)
        obj=dict(person='p',institution='wd:Q1',kind='education',stage='doctorate',year_end=2000,
                 source='wikidata',locator='one',status='observed',evidence='traceable '*200)
        try:
            with self.assertRaisesRegex(RuntimeError,'injected'):
                with s.member('wikidata','one'):s.event(obj);raise RuntimeError('injected')
            self.assertEqual(s.db.execute('SELECT count(*) FROM event_dictionaries').fetchone()[0],0)
            with s.member('wikidata','one'):s.event(obj)
            before=logical_event_hash(s.db)
            self.assertEqual(s.db.execute('SELECT payload FROM events').fetchone()[0],canonical(obj).decode())
            with s.member('wikidata','one'):s.event(obj)
            self.assertEqual(s.db.execute('SELECT count(*) FROM events').fetchone()[0],1)
            frozen=freeze_rows(s)
            with frozen_reader(s,frozen['filename']):self.assertEqual(logical_event_hash(s.db),before)
            for n in range(30):source_dictionary(s,canonical(obj).decode(),{'source':str(n)})
            self.assertEqual(len(s._event_codec_seeds),16)
            self.assertLessEqual(sum(map(len,s._event_codec_seeds.values())),16*MAX_DICTIONARY_BYTES)
        finally:
            s.close()
            self.assertEqual(s._event_codec_seeds,{})

    def test_source_migration_preserves_hash_rank_resume_and_is_idempotent(self):
        s=self.store(archive([]))
        try:
            self.seed(s);before=logical_event_hash(s.db);rank=canonical(rank_global(s,public=True))
            # Exercise already dictionary-coded payloads as well as ordinary ones.
            repack_event_pages(s,share_responses=True)
            first=repack_event_pages(s,share_sources=True,max_events=2)
            repack_event_pages(s,share_sources=True,after_rowid=first['after_rowid'])
            self.assertEqual(logical_event_hash(s.db),before)
            self.assertEqual(canonical(rank_global(s,public=True)),rank)
            self.assertEqual(repack_event_pages(s,share_sources=True)['shared_response_events'],0)
            with self.assertRaises(ValueError):repack_event_pages(s,share_sources=1)
        finally:s.close()
