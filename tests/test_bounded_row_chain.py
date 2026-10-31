"""Explicit bounded dependencies preserve every frozen input and fail closed."""
import hashlib
import json
import lzma
import unittest
from unittest.mock import patch

import test_streaming as fixtures
import test_global_analysis as analysis
from gapi.task import canonical,BudgetStop
from gapi.row_snapshot import freeze_rows,ranking_lines,inspect
from gapi.row_delta import MAX_DEPTH,FORMAT_V3,dependency_depth,rebase_delta
from gapi.frozen_input import frozen_reader
from gapi.global_analysis import rank_global


class BoundedRowChainTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def advance(self,s,n):
        with s.member('wikidata',f'chain-{n}'):
            s.event(dict(person=f'chain-{n}',institution='wd:Q1',kind='education',stage='doctorate',
                year_end=2000,source='wikidata',locator=f'chain-{n}',status='observed',evidence='evidence '*200))

    def chain(self,s,depth):
        snapshots=[freeze_rows(s)]
        for n in range(depth):
            self.advance(s,n)
            snapshots.append(freeze_rows(s,base_filename=snapshots[-1]['filename'],allow_delta_base=True))
        return snapshots

    def test_depth_limit_idempotent_restore_and_original_scores(self):
        s=self.store(fixtures.archive([]))
        try:
            analysis.AnalysisTests.seed(self,s)
            snapshots=self.chain(s,MAX_DEPTH)
            expected=canonical(rank_global(s));wanted=hashlib.sha256(b''.join(ranking_lines(s))).hexdigest()
            latest=snapshots[-1];folder=s.task.root/'frozen-inputs';path=folder/(latest['filename']+'.delta.xz')
            self.assertEqual(latest['format'],FORMAT_V3)
            self.assertEqual(latest['sha256'],wanted)
            self.assertEqual(dependency_depth(path,s.task),MAX_DEPTH)
            mtime=path.stat().st_mtime_ns
            self.assertEqual(freeze_rows(s,base_filename=snapshots[-2]['filename'],allow_delta_base=True),latest)
            self.assertEqual(path.stat().st_mtime_ns,mtime)
            with frozen_reader(s,latest['filename']):self.assertEqual(canonical(rank_global(s)),expected)
            self.advance(s,99)
            with self.assertRaisesRegex(ValueError,'depth'):
                freeze_rows(s,base_filename=latest['filename'],allow_delta_base=True)
            self.assertFalse(list(folder.glob('*.partial')))
            for snap in snapshots:
                from gapi.row_snapshot import archive_path
                self.assertEqual(inspect(archive_path(s,snap['filename']),s.task)[0],snap['sha256'])
        finally:s.close()

    def test_default_still_forbids_chains_and_unchanged_input_metadata_is_actual(self):
        s=self.store(fixtures.archive([]))
        try:
            snapshots=self.chain(s,1);latest=snapshots[-1]
            with self.assertRaisesRegex(ValueError,'chains forbidden'):freeze_rows(s,base_filename=latest['filename'])
            repeated=freeze_rows(s,base_filename=latest['filename'],allow_delta_base=True)
            self.assertEqual(repeated['base_filename'],snapshots[0]['filename'])
            self.assertEqual(repeated['format'],latest['format'])
            for bad in (1,'yes',None):
                with self.assertRaisesRegex(ValueError,'boolean'):freeze_rows(s,allow_delta_base=bad)
        finally:s.close()

    def test_missing_corrupted_and_cyclic_dependencies_rejected(self):
        s=self.store(fixtures.archive([]))
        try:
            snapshots=self.chain(s,2);folder=s.task.root/'frozen-inputs'
            base=folder/(snapshots[1]['filename']+'.delta.xz');target=folder/(snapshots[2]['filename']+'.delta.xz')
            original=base.read_bytes();base.unlink()
            with self.assertRaises(ValueError):inspect(target,s.task)
            base.write_bytes(original[:-1])
            with self.assertRaises((ValueError,EOFError,lzma.LZMAError)):inspect(target,s.task)
            cycle=canonical({'format':FORMAT_V3,'base':snapshots[2]['filename']})+b'\nC 1\n'
            base.write_bytes(lzma.compress(cycle,preset=3))
            with self.assertRaisesRegex(ValueError,'cycle'):inspect(target,s.task)
            with self.assertRaisesRegex(ValueError,'cycle'):dependency_depth(target,s.task)
            base.write_bytes(original)
            self.assertEqual(inspect(target,s.task)[0],snapshots[2]['sha256'])
        finally:s.close()

    def test_failed_nested_write_preserves_all_inputs_and_cleans_owned_staging(self):
        s=self.store(fixtures.archive([]))
        try:
            snapshots=self.chain(s,1);folder=s.task.root/'frozen-inputs'
            before={p.name:p.read_bytes() for p in folder.iterdir()}
            self.advance(s,9)
            with patch.object(s.task,'check',side_effect=[0,BudgetStop('injected')]):
                with self.assertRaises(BudgetStop):freeze_rows(s,base_filename=snapshots[-1]['filename'],allow_delta_base=True)
            self.assertFalse(list(folder.glob('*.partial')))
            self.assertEqual({p.name:p.read_bytes() for p in folder.iterdir()},before)
        finally:s.close()

    def rebase_fixture(self,s):
        import random
        analysis.AnalysisTests.seed(self,s);rank_global(s)
        base=freeze_rows(s)
        rng=random.Random(135)
        with s.db:
            s.db.execute('CREATE TABLE preserved_rows(id INTEGER PRIMARY KEY,body BLOB)')
            s.db.executemany('INSERT INTO preserved_rows VALUES(?,?)',[(n,rng.randbytes(256)) for n in range(100)])
        previous=freeze_rows(s,base_filename=base['filename'])
        with s.db:s.db.execute('INSERT INTO preserved_rows VALUES(?,?)',(100,b'new fact'))
        current=freeze_rows(s,base_filename=base['filename'])
        return base,previous,current

    def test_rebase_reclaims_bytes_preserves_scores_dependencies_and_is_idempotent(self):
        s=self.store(fixtures.archive([]))
        try:
            base,previous,current=self.rebase_fixture(s)
            folder=s.task.root/'frozen-inputs';path=folder/(current['filename']+'.delta.xz')
            before=inspect(path,s.task);score=canonical(rank_global(s))
            predecessor=path.read_bytes()
            preview=rebase_delta(s,current['filename'],previous['filename'],dry_run=True)
            self.assertEqual(path.read_bytes(),predecessor)
            self.assertTrue(preview['dry_run'])
            result=rebase_delta(s,current['filename'],previous['filename'])
            self.assertEqual(preview['candidate_bytes'],result['packed_bytes'])
            self.assertGreater(result['saved_bytes'],0)
            self.assertEqual(result['dependency_depth'],2)
            self.assertEqual(inspect(path,s.task),before)
            packed=path.read_bytes();modified=path.stat().st_mtime_ns
            self.assertTrue(rebase_delta(s,current['filename'],previous['filename'])['already_rebased'])
            self.assertEqual(path.read_bytes(),packed);self.assertEqual(path.stat().st_mtime_ns,modified)
            with frozen_reader(s,current['filename']):
                self.assertEqual(canonical(rank_global(s)),score)
                self.assertEqual(s.db.execute('SELECT body FROM preserved_rows WHERE id=100').fetchone()[0],b'new fact')
            self.assertTrue((folder/(base['filename']+'.xz')).is_file())
            # The previous release now is a real dependency, not disposable data.
            with self.assertRaisesRegex(ValueError,'cycle'):
                rebase_delta(s,previous['filename'],current['filename'])
        finally:s.close()

    def test_rebase_failures_preserve_predecessor_and_foreign_partial(self):
        s=self.store(fixtures.archive([]))
        try:
            _,previous,current=self.rebase_fixture(s)
            path=s.task.root/'frozen-inputs'/(current['filename']+'.delta.xz');before=path.read_bytes()
            original_check=s.task.check;calls=[0]
            def full(*args,**kwargs):
                calls[0]+=1
                if calls[0]>1:raise BudgetStop('injected staging exhaustion')
                return original_check(*args,**kwargs)
            with patch.object(s.task,'check',side_effect=full):
                with self.assertRaises(BudgetStop):rebase_delta(s,current['filename'],previous['filename'])
            self.assertEqual(path.read_bytes(),before)
            self.assertFalse(list(path.parent.glob('*.partial')))
            partial=path.with_suffix('.xz.rebase.partial');partial.write_bytes(b'foreign recovery')
            with self.assertRaises(FileExistsError):rebase_delta(s,current['filename'],previous['filename'])
            self.assertEqual(partial.read_bytes(),b'foreign recovery')
            self.assertEqual(path.read_bytes(),before)
        finally:s.close()

    def test_rebase_cannot_invalidate_an_existing_descendant_depth(self):
        s=self.store(fixtures.archive([]))
        try:
            chain=self.chain(s,MAX_DEPTH)
            self.advance(s,1000)
            independent=freeze_rows(s,base_filename=chain[0]['filename'])
            folder=s.task.root/'frozen-inputs';path=folder/(chain[1]['filename']+'.delta.xz')
            before=path.read_bytes()
            with self.assertRaisesRegex(ValueError,'dependent depth'):
                rebase_delta(s,chain[1]['filename'],independent['filename'])
            self.assertEqual(path.read_bytes(),before)
            self.assertEqual(dependency_depth(folder/(chain[-1]['filename']+'.delta.xz'),s.task),MAX_DEPTH)
        finally:s.close()
