import hashlib
import json
import lzma
import random
import unittest
from unittest.mock import patch

from gapi.frozen_input import frozen_reader,verified_packed_snapshot
from gapi.global_analysis import rank_global
from gapi.row_delta import commands,addition_record,restore_addition,FORMAT_V2,repack_delta
from gapi.row_snapshot import freeze_rows,inspect,ranking_lines
from gapi.task import BudgetStop,canonical
import test_global_analysis as analysis
import test_streaming as fixtures


class RowDeltaTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def legacy_delta(self,s):
        base=freeze_rows(s)
        shared=random.Random(81).randbytes(2048).hex()
        with s.member('wikidata','legacy'):
            for n in range(30):
                s.event(dict(person=f'person-{n}',institution='wd:Q1',kind='education',
                    stage='doctorate',year_end=2000,source='wikidata',locator='legacy',
                    status='observed',evidence=shared))
        frozen=freeze_rows(s,base_filename=base['filename'])
        path=s.task.root/'frozen-inputs'/(frozen['filename']+'.delta.xz')
        records=lzma.decompress(path.read_bytes()).splitlines(keepends=True)
        records[0]=records[0].replace(FORMAT_V2.encode(),b'gapi-private-ranking-row-delta/1')
        records=[b'A'+restore_addition(r) if r.startswith(b'E') else r for r in records]
        path.write_bytes(lzma.compress(b''.join(records),preset=3))
        return frozen,path

    def test_repack_legacy_delta_preserves_identity_scores_and_is_idempotent(self):
        s=self.store(fixtures.archive([]))
        try:
            frozen,path=self.legacy_delta(s)
            before=inspect(path,s.task);score=canonical(rank_global(s));old=path.read_bytes()
            result=repack_delta(s,frozen['filename'])
            self.assertTrue(result['logical_identity_unchanged'])
            self.assertGreater(result['saved_bytes'],0)
            self.assertEqual(inspect(path,s.task),before)
            self.assertNotEqual(path.read_bytes(),old)
            packed=path.read_bytes()
            again=repack_delta(s,frozen['filename'])
            self.assertTrue(again['already_repacked'])
            self.assertEqual(path.read_bytes(),packed)
            with frozen_reader(s,frozen['filename']):self.assertEqual(canonical(rank_global(s)),score)
            self.assertEqual(list(path.parent.glob('*.partial')),[])
        finally:s.close()

    def test_repack_failure_retains_predecessor_and_cleans_only_own_partial(self):
        from gapi.row_delta import inspect_delta
        s=self.store(fixtures.archive([]))
        try:
            frozen,path=self.legacy_delta(s);old=path.read_bytes()
            def wrong(candidate,task):
                result=inspect_delta(candidate,task)
                return ('0'*64,result[1]) if candidate.name.endswith('.partial') else result
            with patch('gapi.row_delta.inspect_delta',side_effect=wrong):
                with self.assertRaisesRegex(ValueError,'reconstruction mismatch'):
                    repack_delta(s,frozen['filename'])
            self.assertEqual(path.read_bytes(),old)
            self.assertEqual(list(path.parent.glob('*.partial')),[])
            partial=path.with_suffix('.xz.repack.partial');partial.write_bytes(b'unique predecessor')
            with self.assertRaises(FileExistsError):repack_delta(s,frozen['filename'])
            self.assertEqual(partial.read_bytes(),b'unique predecessor')
            partial.unlink()
            checks=[0]
            original_check=s.task.check
            def stop_during_write(*args,**kwargs):
                checks[0]+=1
                if checks[0]>1:raise BudgetStop('capacity')
                return original_check(*args,**kwargs)
            with patch.object(s.task,'check',side_effect=stop_during_write):
                with self.assertRaises(BudgetStop):repack_delta(s,frozen['filename'])
            self.assertEqual(path.read_bytes(),old)
            self.assertEqual(list(path.parent.glob('*.partial')),[])
            path.write_bytes(old[:-3])
            with self.assertRaises((ValueError,EOFError,lzma.LZMAError)):
                repack_delta(s,frozen['filename'])
            self.assertEqual(path.read_bytes(),old[:-3])
        finally:s.close()

    def test_expanded_event_addition_reconstructs_exact_blob_and_bounds(self):
        import base64
        from gapi.event_storage import pack
        text=canonical({'response_url':'https://example.org/?'+('repeated='*300),'references':[{'value':'证据'*200}]}).decode()
        stored=pack(text);self.assertIsInstance(stored,bytes)
        row={'t':'events','v':[['v',str(i)] for i in range(12)]+[['b',base64.b64encode(stored).decode()]]}
        raw=canonical(row)+b'\n';encoded=addition_record(raw)
        self.assertTrue(encoded.startswith(b'E'))
        self.assertEqual(restore_addition(encoded),raw)
        for changed in ('0'*64,'bad'):
            wrong=json.loads(encoded[1:]);wrong['v'][-1][2]=changed
            with self.assertRaises(ValueError):restore_addition(b'E'+canonical(wrong)+b'\n')
        wrong=json.loads(encoded[1:]);wrong['v'][-1][1]='x'*20000
        with self.assertRaises(ValueError):restore_addition(b'E'+canonical(wrong)+b'\n')
        self.assertEqual(addition_record(canonical({'t':'other','v':row['v']})+b'\n'),b'A'+canonical({'t':'other','v':row['v']})+b'\n')

    def test_v2_freeze_restores_compressed_event_and_rejects_tampering(self):
        s=self.store(fixtures.archive([]))
        try:
            base=freeze_rows(s)
            obj=dict(person='private',institution='wd:Q1',kind='education',stage='doctorate',year_end=2000,
                     source='wikidata',locator='one',status='observed',evidence='repeated evidence '*200)
            with s.member('wikidata','one'):s.event(obj)
            original=list(s.db.execute('SELECT * FROM events'))
            frozen=freeze_rows(s,base_filename=base['filename'])
            self.assertEqual(frozen['format'],FORMAT_V2)
            path=s.task.root/'frozen-inputs'/(frozen['filename']+'.delta.xz')
            packed=path.read_bytes();raw=lzma.decompress(packed)
            self.assertIn(b'\nE',raw)
            with frozen_reader(s,frozen['filename']):self.assertEqual(list(s.db.execute('SELECT * FROM events')),original)
            # The old format cannot silently accept a command it cannot decode.
            path.write_bytes(lzma.compress(raw.replace(FORMAT_V2.encode(),b'gapi-private-ranking-row-delta/1',1),preset=3))
            with self.assertRaisesRegex(ValueError,'invalid row delta command'):inspect(path,s.task)
            path.write_bytes(packed)
        finally:s.close()

    def test_exact_stream_scores_idempotence_and_restore_with_changes(self):
        s=self.store(fixtures.archive([]))
        try:
            analysis.AnalysisTests.seed(self,s)
            base=freeze_rows(s);folder=s.task.root/'frozen-inputs'
            with s.db:
                s.db.execute("DELETE FROM events WHERE source='orcid-public-api'")
                s.db.execute("UPDATE events SET country='DE'")
                s.db.execute('CREATE TABLE extra(id INTEGER PRIMARY KEY,value BLOB)')
                s.db.execute('INSERT INTO extra VALUES(?,?)',(1,b'\x00test\xff'))
            expected=canonical(rank_global(s))
            original_events=list(s.db.execute('SELECT * FROM events ORDER BY id'))
            target=b''.join(ranking_lines(s))
            result=freeze_rows(s,base_filename=base['filename'])
            path=folder/(result['filename']+'.delta.xz')
            self.assertEqual(result['sha256'],hashlib.sha256(target).hexdigest())
            self.assertTrue(verified_packed_snapshot(s,path))
            self.assertEqual(inspect(path,s.task),(result['sha256'],len(target)))
            modified=path.stat().st_mtime_ns
            self.assertEqual(freeze_rows(s,base_filename=base['filename']),result)
            self.assertEqual(path.stat().st_mtime_ns,modified)
            with self.assertRaisesRegex(ValueError,'chains forbidden'):
                freeze_rows(s,base_filename=result['filename'])
            live=s.db
            with frozen_reader(s,result['filename']):
                self.assertEqual(canonical(rank_global(s)),expected)
                self.assertEqual(list(s.db.execute('SELECT * FROM events ORDER BY id')),original_events)
                self.assertEqual(s.db.execute('SELECT value FROM extra').fetchone()[0],b'\x00test\xff')
            self.assertIs(s.db,live)
            self.assertEqual(list(folder.glob('frozen-read-*')),[])
            self.assertTrue((folder/(base['filename']+'.xz')).exists())
        finally:s.close()

    def test_unchanged_input_reuses_original_full_archive(self):
        s=self.store(fixtures.archive([]))
        try:
            base=freeze_rows(s)
            self.assertEqual(freeze_rows(s,base_filename=base['filename']),base)
            self.assertEqual(list((s.task.root/'frozen-inputs').glob('*.delta.xz')),[])
        finally:s.close()

    def test_missing_changed_baseline_truncation_and_trailing_stream_fail_closed(self):
        s=self.store(fixtures.archive([]))
        try:
            base=freeze_rows(s);folder=s.task.root/'frozen-inputs'
            with s.db:s.db.execute('CREATE TABLE added(value TEXT)')
            result=freeze_rows(s,base_filename=base['filename']);path=folder/(result['filename']+'.delta.xz')
            original=path.read_bytes();base_path=folder/(base['filename']+'.xz')
            saved=base_path.read_bytes()
            for bad in (original[:-3],original+b'x',original+original):
                path.write_bytes(bad)
                with self.assertRaises((ValueError,EOFError,lzma.LZMAError)):
                    with frozen_reader(s,result['filename']):self.fail('corruption accepted')
            path.write_bytes(original)
            decoded=lzma.decompress(saved).replace(b'global-ranking',b'global-rankinx',1)
            base_path.write_bytes(lzma.compress(decoded,preset=3))
            with self.assertRaisesRegex(ValueError,'baseline identity'):
                with frozen_reader(s,result['filename']):pass
            base_path.write_bytes(saved);base_path.unlink()
            with self.assertRaisesRegex(ValueError,'retained full'):
                with frozen_reader(s,result['filename']):pass
            self.assertEqual(list(folder.glob('frozen-read-*')),[])
        finally:s.close()

    def test_budget_failure_and_foreign_staging_preserve_original(self):
        s=self.store(fixtures.archive([]))
        try:
            base=freeze_rows(s);folder=s.task.root/'frozen-inputs'
            original=(folder/(base['filename']+'.xz')).read_bytes()
            with patch.object(s.task,'check',side_effect=[0,BudgetStop('full')]):
                with self.assertRaises(BudgetStop):freeze_rows(s,base_filename=base['filename'])
            self.assertFalse(s.db.in_transaction)
            self.assertEqual(list(folder.glob('*.partial')),[])
            staging=folder/'normalized-row-delta.xz.partial';staging.write_bytes(b'recovery')
            with self.assertRaises(FileExistsError):freeze_rows(s,base_filename=base['filename'])
            self.assertEqual(staging.read_bytes(),b'recovery')
            self.assertEqual((folder/(base['filename']+'.xz')).read_bytes(),original)
            with self.assertRaises(ValueError):freeze_rows(s,base_filename='../careers.sqlite')
        finally:s.close()

    def test_command_overrun_missing_footer_and_extra_records_are_rejected(self):
        s=self.store(fixtures.archive([]))
        try:
            base=freeze_rows(s);folder=s.task.root/'frozen-inputs'
            header=canonical({'format':'gapi-private-ranking-row-delta/1','base':base['filename']})+b'\n'
            for value in (b'C 999999\n',b'C 0\n',b'S -1\n',b'',
                          b'F {"sha256":"invalid","bytes":0}\n',b'A\n'):
                path=folder/('f'*64+'.rows.delta.xz');path.write_bytes(lzma.compress(header+value,preset=3))
                with self.assertRaises(ValueError):inspect(path,s.task)
            original=lzma.decompress((folder/(base['filename']+'.xz')).read_bytes())
            valid=(header+f'C {len(original.splitlines())}\n'.encode()+b'F '+canonical({
                'sha256':base['sha256'],'bytes':len(original)})+b'\n')
            path.write_bytes(lzma.compress(valid,preset=3))
            self.assertEqual(inspect(path,s.task),(base['sha256'],len(original)))
            path.write_bytes(lzma.compress(valid+b'A{}\n',preset=3))
            with self.assertRaisesRegex(ValueError,'trailing'):inspect(path,s.task)
        finally:s.close()

    def test_deterministic_bounded_alignment_preserves_insert_delete_reorder(self):
        rng=random.Random(34)
        for _ in range(80):
            old=[canonical({'value':i})+b'\n' for i in range(100)]
            new=[value for value in old if rng.random()>.2]
            for i in range(20):new.insert(rng.randrange(len(new)+1),canonical({'new':i})+b'\n')
            rng.shuffle(new)
            result=[];position=0
            emitted=list(commands(iter(old),iter(new)))
            self.assertEqual(emitted,list(commands(iter(old),iter(new))))
            for op,value in emitted:
                if op=='C':result.append(old[position]);position+=1
                elif op=='S':position+=1
                else:result.append(value)
            self.assertEqual(position,len(old));self.assertEqual(result,new)

    def test_non_object_headers_are_rejected_by_quality_without_crashing(self):
        s=self.store(fixtures.archive([]))
        try:
            folder=s.task.root/'frozen-inputs';folder.mkdir()
            path=folder/('f'*64+'.rows.delta.xz')
            for header in ([],None,'invalid',42):
                path.write_bytes(lzma.compress(canonical(header)+b'\n',preset=3))
                self.assertFalse(verified_packed_snapshot(s,path))
                with self.assertRaises(ValueError):inspect(path,s.task)
        finally:s.close()

    def test_long_table_insert_retains_next_unchanged_table(self):
        def row(table,i):return canonical({'t':table,'v':[['v',i]]})+b'\n'
        common=[row('z',i) for i in range(100)]
        old=[row('a',0)]+common
        new=[row('a',i) for i in range(200)]+common
        emitted=list(commands(iter(old),iter(new)));position=0;result=[]
        for op,value in emitted:
            if op=='C':result.append(old[position]);position+=1
            elif op=='S':position+=1
            else:result.append(value)
        self.assertEqual(result,new)
        self.assertEqual(position,len(old))
        self.assertEqual(sum(op=='C' for op,_ in emitted),101)

    def test_open_file_growth_floor_survives_stale_directory_size(self):
        s=self.store(fixtures.archive([]))
        try:
            initial=s.task.used()
            available=s.task.config['budget_bytes']-s.task.config['reserve_bytes']
            with patch.object(s.task,'used',return_value=initial):
                with self.assertRaises(BudgetStop):
                    s.task.check(1,minimum_used=available)
                self.assertEqual(s.task.check(0,minimum_used=initial+100),initial+100)
        finally:s.close()
