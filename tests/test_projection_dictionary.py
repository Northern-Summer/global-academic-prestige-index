"""Independent row decoding, replay and failed-migration recovery contracts."""
import hashlib
import zlib
import unittest
from unittest.mock import patch

import test_projection_storage as fixtures
from gapi import projection_storage as storage
from gapi.easy_orcid_landing import ingest, verified_rows, rank_projection
from gapi.task import BudgetStop


class ProjectionDictionaryTests(unittest.TestCase):
    fixture=fixtures.ProjectionStorageTests.fixture
    store=fixtures.ProjectionStorageTests.store
    source=fixtures.ProjectionStorageTests.source
    record=fixtures.ProjectionStorageTests.record

    def test_stable_automatic_dictionary_and_cli_route(self):
        import argparse
        from gapi.stream_cli import register
        parser=argparse.ArgumentParser();register(parser.add_subparsers(dest='command'))
        self.assertEqual(parser.parse_args(['repack-projection-pages','--codec','dictionary']).codec,'dictionary')
        self.assertEqual(parser.parse_args(['repack-projection-pages']).codec,'blocks')
        s,*_=self.fixture();before=storage.projection_dictionary(s.db)
        self.assertEqual(storage.repack_dictionary(s)['repacked_rows'],128)
        self.assertEqual(storage.projection_dictionary(s.db),before)
        self.assertEqual(storage.repack_dictionary(s)['repacked_rows'],0)
    def dictionary(self,s):
        rows=s.db.execute('SELECT payload AS projected_payload FROM easy_careers ORDER BY row_number LIMIT 128')
        dictionary=b''
        for (value,) in rows:dictionary=(dictionary+storage.plain_payload(s.db,value))[-8192:]
        return dictionary

    def test_dictionary_replay_source_reuse_score_pruning_and_idempotency(self):
        s,spec,http,_=self.fixture();dictionary=self.dictionary(s)
        before=list(verified_rows(s))
        s.db.executescript('CREATE TABLE active_institution_aliases(alias,institution); CREATE TABLE entities(id,name,country,payload);')
        with patch('gapi.global_analysis.resolve_institutions'):score=rank_projection(s)
        result=storage.repack_dictionary(s,dictionary,max_rows=64)
        self.assertEqual(result['repacked_rows'],64)
        self.assertEqual(list(verified_rows(s)),before)
        result=storage.repack_dictionary(s,dictionary,after_row=result['after_row'])
        self.assertEqual(result['repacked_rows'],64)
        self.assertEqual(storage.repack_dictionary(s,dictionary)['repacked_rows'],0)
        self.assertEqual(storage.repack(s)['repacked_rows'],0)
        self.assertTrue(ingest(s,http,spec,max_seconds=5)['source_complete'])
        self.assertEqual(list(verified_rows(s)),before)
        with patch('gapi.global_analysis.resolve_institutions'):self.assertEqual(rank_projection(s),score)
        self.assertEqual(storage.prune_unreferenced(s)['referenced_blocks_preserved'],1)
        self.assertEqual(list(verified_rows(s)),before)

    def test_dictionary_corruption_deletion_and_row_framing_fail_closed(self):
        s,*_=self.fixture();dictionary=self.dictionary(s);storage.repack_dictionary(s,dictionary)
        (value,)=s.db.execute('SELECT payload AS projected_payload FROM easy_careers LIMIT 1').fetchone()
        expected=storage.plain_payload(s.db,value)
        key=hashlib.sha256(dictionary).digest()
        self.assertEqual(zlib.decompress(storage.original_payload(s.db,value)),expected)
        # Cache must bind to bytes actually read, not merely the dictionary key.
        with s.db:s.db.execute(f'UPDATE {storage.TABLE} SET packed=? WHERE digest=?',(zlib.compress(b'wrong'),key))
        with self.assertRaisesRegex(ValueError,'identity'):storage.plain_payload(s.db,value)
        with s.db:s.db.execute(f'UPDATE {storage.TABLE} SET packed=? WHERE digest=?',(zlib.compress(dictionary),key))
        self.assertEqual(storage.plain_payload(s.db,value),expected)
        for bad in (value[:-1],value+b'x',value[:39]+(storage.MAX_ROW+1).to_bytes(4,'big')+value[43:]):
            with self.assertRaises(ValueError):storage.plain_payload(s.db,bad)
        with s.db:s.db.execute(f'DELETE FROM {storage.TABLE} WHERE digest=?',(key,))
        with self.assertRaisesRegex(ValueError,'missing'):storage.plain_payload(s.db,value)

    def test_dictionary_failed_commit_keeps_rows_and_resumes(self):
        s,*_=self.fixture();dictionary=self.dictionary(s)
        query='SELECT row_number,person,row_sha256,payload AS projected_payload FROM easy_careers ORDER BY row_number'
        before=s.db.execute(query).fetchall()
        checks=[0]
        def memory():
            checks[0]+=1
            if checks[0]==2:raise BudgetStop('injected before commit')
        with patch.object(s.task,'check_memory',side_effect=memory):
            with self.assertRaises(BudgetStop):storage.repack_dictionary(s,dictionary)
        self.assertFalse(s.db.in_transaction)
        self.assertEqual(s.db.execute(query).fetchall(),before)
        self.assertEqual(storage.repack_dictionary(s,dictionary)['repacked_rows'],128)
        for bad in (b'',b'x'*8193,'text'):
            with self.assertRaises(ValueError):storage.repack_dictionary(s,bad)

    def test_dictionary_does_not_weaken_original_block_pruning_bounds(self):
        s,*_=self.fixture();storage.repack(s)
        with s.db:s.db.execute('UPDATE easy_careers SET payload=CAST(payload || ? AS BLOB) WHERE row_number=1',(b'x',))
        with self.assertRaisesRegex(ValueError,'framing'):storage.prune_unreferenced(s)
