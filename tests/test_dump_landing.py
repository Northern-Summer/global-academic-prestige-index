import json
import unittest
from contextlib import closing

from gapi.dump_landing import initialize, stage_entity, promote_one, promote_batch
from gapi.task import digest
import test_streaming as fixtures


def entity():
    return {'id':'Q123', 'lastrevid':17, 'labels':{'en':{'value':'PRIVATE NAME'}},
            'claims':{'P69':[{'id':'Q123$education','mainsnak':{
                'datavalue':{'value':{'id':'Q456'}}}}]}}


class LandingTests(unittest.TestCase):
    setUp = fixtures.StreamingTests.setUp
    tearDown = fixtures.StreamingTests.tearDown
    def store(self):
        return closing(fixtures.StreamingTests.store(self, fixtures.archive([])))

    def prepare(self, store):
        initialize(store)
        store.db.execute("INSERT INTO discovery_feeds(id,spec,status) VALUES('feed','{}','provisional')")
        store.db.commit()

    def test_unverified_isolated_and_normalized_only_then_idempotent_promotion(self):
        with self.store() as s:
            self.prepare(s)
            e=entity()
            with s.db:
                self.assertEqual(stage_entity(s,'feed',e,digest(e)),1)
                stage_entity(s,'feed',e,digest(e))
            self.assertEqual(s.db.execute('SELECT count(*) FROM dump_landing').fetchone()[0],1)
            self.assertEqual(s.db.execute('SELECT count(*) FROM events').fetchone()[0],0)
            payload=s.db.execute('SELECT payload FROM dump_landing').fetchone()[0]
            self.assertNotIn('PRIVATE NAME',payload)
            self.assertNotIn('labels',payload)
            self.assertEqual(json.loads(payload)[0]['status'],'unresolved')
            with self.assertRaisesRegex(ValueError,'checksum'):
                promote_one(s,'feed')
            with s.db:s.db.execute("UPDATE discovery_feeds SET status='verified'")
            self.assertTrue(promote_one(s,'feed'))
            self.assertFalse(promote_one(s,'feed'))
            self.assertEqual(s.db.execute('SELECT count(*) FROM events').fetchone()[0],1)
            self.assertEqual(s.db.execute('SELECT source FROM members').fetchone()[0],'wikidata')

    def test_checkpoint_rollback_and_changed_member_rejected(self):
        with self.store() as s:
            self.prepare(s)
            e=entity()
            try:
                with s.db:
                    stage_entity(s,'feed',e,digest(e))
                    raise RuntimeError('checkpoint interrupted')
            except RuntimeError:pass
            self.assertEqual(s.db.execute('SELECT count(*) FROM dump_landing').fetchone()[0],0)
            with s.db:stage_entity(s,'feed',e,digest(e))
            with self.assertRaisesRegex(ValueError,'changed'):
                stage_entity(s,'feed',e,'0'*64)

    def test_transaction_bound_and_unknown_feed(self):
        with self.store() as s:
            self.prepare(s)
            e=entity()
            with self.assertRaisesRegex(ValueError,'unknown'):
                stage_entity(s,'missing',e,digest(e))
            s.task.config['batch_events']=1
            e['claims']['P69']*=2
            with self.assertRaisesRegex(ValueError,'bounded'):
                stage_entity(s,'feed',e,digest(e))
            self.assertEqual(s.db.execute('SELECT count(*) FROM dump_landing').fetchone()[0],0)

    def test_bounded_promotion_resume_and_arguments(self):
        with self.store() as s:
            self.prepare(s)
            for i in range(3):
                e=entity(); e['id']='Q'+str(i+100)
                with s.db:stage_entity(s,'feed',e,digest(e))
            with self.assertRaisesRegex(ValueError,'checksum'):
                promote_batch(s,'feed',max_members=1)
            with s.db:s.db.execute("UPDATE discovery_feeds SET status='verified'")
            for limit in (0,10001,True):
                with self.assertRaises(ValueError):promote_batch(s,'feed',max_members=limit)
            a=promote_batch(s,'feed',max_members=1)
            self.assertEqual((a['promoted_members'],a['pending_members']),(1,2))
            self.assertFalse(a['promotion_complete'])
            b=promote_batch(s,'feed')
            self.assertEqual(b['promoted_members'],2)
            self.assertTrue(b['promotion_complete'])
            self.assertEqual(promote_batch(s,'feed')['promoted_members'],0)

    def test_scanner_backfills_old_prefix_and_checksum_gates_promotion(self):
        import bz2
        from unittest.mock import patch
        from gapi.dump_discovery import discover_dump
        from gapi.task import BudgetStop
        import gapi.dump_discovery as module
        from test_dump_discovery import HTTP, spec_for
        e=entity()
        e['claims']['P108']=[{'id':'Q123$job','mainsnak':{'datavalue':{'value':{'id':'Q789'}}}}]
        body=bz2.compress(b'[\n'+json.dumps(e).encode()+b'\n]\n')
        with self.store() as s:
            spec=spec_for(body)
            discover_dump(s,HTTP(body),spec)
            original=module.bounded_lines
            def interrupt(stream):
                yield from original(stream)
                raise BudgetStop('interrupted before full validation')
            with patch.object(module,'bounded_lines',interrupt):
                result=discover_dump(s,HTTP(body),spec,stage_careers=True)
            self.assertEqual(result['status'],'provisional')
            self.assertEqual(s.db.execute('SELECT count(*) FROM dump_landing').fetchone()[0],1)
            self.assertEqual(s.db.execute('SELECT count(*) FROM events').fetchone()[0],0)
            with self.assertRaisesRegex(ValueError,'checksum'):
                promote_one(s,spec['id'])
            result=discover_dump(s,HTTP(body),spec,stage_careers=True)
            self.assertEqual(result['status'],'verified')
            self.assertEqual(s.db.execute('SELECT count(*) FROM dump_landing').fetchone()[0],1)
            self.assertTrue(promote_one(s,spec['id']))
            self.assertEqual(s.db.execute('SELECT count(*) FROM events').fetchone()[0],2)

    def test_wrong_source_checksum_keeps_landing_unavailable(self):
        import bz2
        from gapi.dump_discovery import discover_dump
        from test_dump_discovery import HTTP, spec_for
        e=entity()
        e['claims']['P108']=[{'id':'Q123$job','mainsnak':{'datavalue':{'value':{'id':'Q789'}}}}]
        body=bz2.compress(b'[\n'+json.dumps(e).encode()+b'\n]\n')
        with self.store() as s:
            spec=spec_for(body); spec['sha1']='0'*40
            with self.assertRaisesRegex(ValueError,'checksum'):
                discover_dump(s,HTTP(body),spec,stage_careers=True)
            self.assertEqual(s.db.execute('SELECT count(*) FROM events').fetchone()[0],0)
            with self.assertRaisesRegex(ValueError,'checksum'):
                promote_one(s,spec['id'])

    def test_reconnect_configuration_is_bounded_before_network(self):
        from gapi.dump_discovery import discover_dump
        from test_dump_discovery import HTTP, spec_for, dump_bytes
        body=dump_bytes()
        with self.store() as s:
            for limit in (0,-1,4097,True,1.5):
                with self.assertRaisesRegex(ValueError,'reconnect'):
                    discover_dump(s,HTTP(body),spec_for(body),max_reconnects=limit)
            self.assertEqual(discover_dump(s,HTTP(body),spec_for(body),max_reconnects=4096)['status'],'verified')


if __name__=='__main__':unittest.main()
