"""Storage changes must preserve exact source rows, reuse and score bytes."""
from contextlib import contextmanager
import hashlib
import json
import sqlite3
import unittest
import zlib
from unittest.mock import patch

import test_easy_orcid_landing as fixtures
from gapi import projection_storage as storage
from gapi.easy_orcid_landing import ingest,verified_rows,rank_projection
from gapi.easy_orcid_full import ingest_full
from gapi.event_storage import configure_reader
from gapi.task import BudgetStop


class ProjectionStorageTests(unittest.TestCase):
    store=fixtures.LandingTests.store
    source=fixtures.LandingTests.source
    record=fixtures.LandingTests.record

    def test_cli_and_ranking_snapshot_scope(self):
        import argparse
        from gapi.stream_cli import register
        from gapi.ranking_snapshot import EXCLUDED_TABLES
        parser=argparse.ArgumentParser();register(parser.add_subparsers(dest='command'))
        args=parser.parse_args(['repack-projection-pages','--after-row','123','--max-rows','256'])
        self.assertEqual((args.table,args.after_row,args.max_rows),('easy_careers',123,256))
        self.assertIn(storage.TABLE,EXCLUDED_TABLES)
        # Parsing alone missed an execution-dispatch omission. Prove that the
        # real CLI executor reaches Task rather than silently returning False.
        from gapi.stream_cli import execute
        with patch('gapi.stream_cli.Task',side_effect=RuntimeError('dispatch reached task')):
            with self.assertRaisesRegex(RuntimeError,'dispatch reached task'):execute(args)

    def fixture(self):
        s=self.store(batch=1000)
        @contextmanager
        def member(*args):
            s.db.execute('BEGIN IMMEDIATE')
            try:yield;s.db.commit()
            except BaseException:s.db.rollback();raise
        s.member=member
        record=self.record();record['employments'][0]['xrefs']['ror']='03yrm5c26'
        spec,http=self.source([record]*128);spec['derived_from']='fixture'
        ingest(s,http,spec,max_seconds=5);configure_reader(s.db)
        return s,spec,http,record

    def test_repack_replay_full_reuse_and_scores_are_identical(self):
        s,spec,http,record=self.fixture();before=list(verified_rows(s))
        s.db.executescript('CREATE TABLE active_institution_aliases(alias,institution); CREATE TABLE entities(id,name,country,payload);')
        with patch('gapi.global_analysis.resolve_institutions'):
            score=rank_projection(s)
        result=storage.repack(s)
        self.assertEqual(result['repacked_rows'],128)
        self.assertGreater(result['estimated_payload_saving_bytes'],0)
        self.assertEqual(list(verified_rows(s)),before)
        physical=s.db.execute('SELECT row_number,payload AS projected_payload FROM easy_careers ORDER BY row_number').fetchall()
        self.assertTrue(all(value.startswith(storage.MAGIC) for _,value in physical))
        self.assertTrue(ingest(s,http,spec,max_seconds=5)['source_complete'])
        self.assertEqual(physical,s.db.execute('SELECT row_number,payload AS projected_payload FROM easy_careers ORDER BY row_number').fetchall())
        with patch('gapi.global_analysis.resolve_institutions'):
            self.assertEqual(score,rank_projection(s))
        full_spec,full_http=self.source([record]);full_spec.update(id='easy-orcid-2023.6-full',derived_from='fixture')
        result=ingest_full(s,full_http,full_spec,max_seconds=5)
        self.assertEqual(result['reused_hq_source_rows'],1)
        self.assertTrue(result['all_hq_projections_present'])
        self.assertEqual(result['committed_delta_rows'],0)
        self.assertEqual(storage.repack(s)['repacked_rows'],0)
        self.assertEqual(list(verified_rows(s)),before)

    def test_actual_block_bytes_prevent_cache_masking_corruption_or_deletion(self):
        s,_,_,_=self.fixture();storage.repack(s)
        value=s.db.execute('SELECT payload AS projected_payload FROM easy_careers LIMIT 1').fetchone()[0]
        original=storage.original_payload(s.db,value)
        key,packed=s.db.execute(f'SELECT digest,packed FROM {storage.TABLE}').fetchone()
        with s.db:s.db.execute(f'UPDATE {storage.TABLE} SET packed=?',(packed+b'x',))
        with self.assertRaisesRegex(ValueError,'framing'):storage.original_payload(s.db,value)
        with s.db:s.db.execute(f'UPDATE {storage.TABLE} SET packed=?',(packed,))
        self.assertEqual(storage.original_payload(s.db,value),original)
        with s.db:s.db.execute(f'DELETE FROM {storage.TABLE}')
        with self.assertRaisesRegex(ValueError,'missing'):storage.original_payload(s.db,value)

    def test_legacy_block_upgrade_and_plain_reader_avoid_recompressing(self):
        s,_,_,_=self.fixture();before=list(verified_rows(s))
        values=[r[0] for r in s.db.execute('SELECT payload AS projected_payload FROM easy_careers ORDER BY row_number')]
        key=hashlib.sha256(storage.framed(values)).digest()
        packed=zlib.compress(storage.framed([zlib.decompress(v) for v in values]),6)
        with s.db:
            s.db.execute(storage.DDL)
            s.db.execute(f'INSERT INTO {storage.TABLE} VALUES(?,?)',(key,packed))
            s.db.executemany('UPDATE easy_careers SET payload=? WHERE row_number=?',
                [(storage.LEGACY_MAGIC+key+i.to_bytes(2,'big'),i+1) for i in range(128)])
        self.assertEqual(list(verified_rows(s)),before)
        self.assertEqual(storage.repack(s)['repacked_rows'],128)
        self.assertEqual(list(verified_rows(s)),before)
        storage.decode_plain_block.cache_clear()
        value=s.db.execute('SELECT payload AS projected_payload FROM easy_careers LIMIT 1').fetchone()[0]
        with patch('gapi.projection_storage.zlib.compress',side_effect=AssertionError('hot path recompressed')):
            self.assertEqual(storage.plain_payload(s.db,value),zlib.decompress(values[0]))
        self.assertEqual(storage.prune_unreferenced(s)['unreferenced_blocks_removed'],1)
        self.assertEqual(list(verified_rows(s)),before)
        self.assertEqual(storage.prune_unreferenced(s)['unreferenced_blocks_removed'],0)

    def test_bad_reference_identity_slot_and_bounded_framing(self):
        s,_,_,_=self.fixture();storage.repack(s)
        value=s.db.execute('SELECT payload AS projected_payload FROM easy_careers LIMIT 1').fetchone()[0]
        with self.assertRaisesRegex(ValueError,'reference'):storage.original_payload(s.db,value+b'x')
        with self.assertRaisesRegex(ValueError,'slot'):storage.original_payload(s.db,value[:-2]+b'\xff\xff')
        packed=s.db.execute(f'SELECT packed FROM {storage.TABLE}').fetchone()[0]
        with self.assertRaisesRegex(ValueError,'identity'):storage.decode_block(b'x'*32,packed)
        with self.assertRaises(ValueError):storage.inflate(zlib.compress(b'x'*(storage.MAX_ROW+1)),storage.MAX_ROW)
        for malformed in (b'\0',b'\0\0\0\x05x'):
            with self.assertRaises(ValueError):storage.decode_block(b'x'*32,zlib.compress(malformed))
        with self.assertRaises(ValueError):storage.make_block([])
        with self.assertRaises(ValueError):storage.make_block([zlib.compress(b'x')]*129)
        with self.assertRaisesRegex(ValueError,'reversible'):
            storage.make_block([zlib.compress(b'long repeating value'*50,1)])

    def test_capacity_stop_rolls_back_blocks_and_replacements_together(self):
        s,_,_,_=self.fixture();before=list(verified_rows(s))
        @contextmanager
        def member(*args):
            s.db.execute('BEGIN IMMEDIATE')
            try:yield;raise BudgetStop('post-write fixture capacity')
            except BaseException:s.db.rollback();raise
        s.member=member
        with self.assertRaises(BudgetStop):storage.repack(s)
        self.assertEqual(list(verified_rows(s)),before)
        self.assertIsNone(s.db.execute("SELECT 1 FROM sqlite_master WHERE name=?",(storage.TABLE,)).fetchone())

    def test_resume_cursor_limits_and_foreign_keys(self):
        s,_,_,_=self.fixture()
        first=storage.repack(s,max_rows=64)
        self.assertEqual(first['after_row'],64)
        second=storage.repack(s,after_row=64,max_rows=64)
        self.assertEqual(second['after_row'],128)
        self.assertEqual(first['repacked_rows']+second['repacked_rows'],128)
        for kwargs in ({'after_row':-1},{'max_rows':0},{'max_seconds':float('nan')},{'table':'events'}):
            with self.assertRaises(ValueError):storage.repack(s,**kwargs)
        s.db.execute('BEGIN')
        with self.assertRaisesRegex(ValueError,'unfinished'):storage.repack(s)
        s.db.rollback()
        s.db.executescript('CREATE TABLE dependent(source INTEGER REFERENCES easy_careers(row_number))')
        with self.assertRaisesRegex(ValueError,'foreign'):storage.repack(s)
