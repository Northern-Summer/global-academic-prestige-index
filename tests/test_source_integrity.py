"""A completed queue cannot substitute for a verified annual archive."""
import unittest

import test_streaming as fixtures
from test_streaming import archive
from gapi.orcid_stream import stream_archive
import io


class SourceIntegrityTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def test_missing_failed_and_partial_archive_never_complete(self):
        store=self.store(archive([]))
        try:
            self.assertFalse(store.source_integrity()['all_archives_verified'])
            source='orcid-2026-summary'
            store.start(source)
            for status in ('partial','access_failed','budget_stopped','failed'):
                store.status(source,status)
                result=store.source_integrity()
                self.assertFalse(result['all_archives_verified'])
                self.assertFalse(result['sources'][0]['enumeration_complete'])
        finally:store.close()

    def test_real_parser_verification_and_identity_binding(self):
        data=archive([]);store=self.store(data)
        try:
            store.start('orcid-2026-summary')
            stream_archive(store,'orcid-2026-summary',io.BytesIO(data))
            result=store.source_integrity()
            self.assertTrue(result['all_archives_verified'])
            self.assertEqual(result['sources'][0]['compressed_bytes'],len(data))
            self.assertEqual(result,store.source_integrity())
            store.task.config['sources']['orcid-2026-summary']['version']='different'
            self.assertFalse(store.source_integrity()['all_archives_verified'])
        finally:store.close()

    def test_finished_candidate_queue_is_not_source_enumeration(self):
        store=self.store(archive([]))
        try:
            with store.db:
                store.db.execute("INSERT INTO discovery VALUES('ingest-wikidata','Q1','a','b')")
                store.db.execute("INSERT INTO members VALUES('wikidata','Q1','hash','observed',NULL)")
            report=store.report()
            self.assertTrue(report['acquisition_progress']['sources'][0]['enumeration_complete'])
            self.assertTrue(report['candidate_processing']['sources'][0]['processing_complete'])
            self.assertFalse(report['candidate_processing']['source_enumeration_proven'])
            self.assertFalse(report['source_integrity']['all_archives_verified'])
            self.assertFalse(report['complete_global_census'])
        finally:store.close()

    def test_verified_archive_with_error_document_keeps_rejection_unsettled(self):
        rows=[(f'summaries/097/{fixtures.PERSON}.xml',b'<error>upstream response</error>')]
        data=archive(rows);store=self.store(data)
        try:
            stream_archive(store,'orcid-2026-summary',io.BytesIO(data))
            result=store.source_integrity()
            self.assertTrue(result['all_archives_verified'])
            self.assertFalse(result['all_archive_members_processed'])
            processing=result['sources'][0]['member_processing']
            self.assertEqual(processing['committed_members'],1)
            self.assertEqual(processing['parsed_members'],0)
            self.assertEqual(processing['rejected_members'],1)
            self.assertEqual(processing['unsettled_rejections'],1)
            self.assertTrue(processing['ledger_reconciled'])
            self.assertEqual(processing['parsed_fraction'],0)
            self.assertIsNone(processing['legal_exclusions_verified'])
            self.assertEqual(store.report()['events'],0)
        finally:store.close()

    def test_clean_archive_members_complete_only_after_integrity(self):
        data=archive([(f'summaries/097/{fixtures.PERSON}.xml',fixtures.XML)])
        store=self.store(data)
        try:
            stream_archive(store,'orcid-2026-summary',io.BytesIO(data),max_members=1)
            partial=store.source_integrity()
            processing=partial['sources'][0]['member_processing']
            self.assertEqual(processing['parsed_members'],1)
            self.assertTrue(processing['ledger_reconciled'])
            self.assertFalse(processing['processing_complete'])
            self.assertIsNone(processing['parsed_fraction'])
            stream_archive(store,'orcid-2026-summary',io.BytesIO(data))
            complete=store.source_integrity()
            self.assertTrue(complete['all_archive_members_processed'])
            self.assertEqual(complete['sources'][0]['member_processing']['parsed_fraction'],1)
            self.assertFalse(complete['complete_global_census'])
        finally:store.close()

    def test_member_counter_drift_unknown_states_and_identity_drift_hold_completion(self):
        data=archive([]);store=self.store(data)
        source='orcid-2026-summary'
        try:
            stream_archive(store,source,io.BytesIO(data))
            self.assertTrue(store.source_integrity()['all_archive_members_processed'])
            with store.db:
                store.db.execute('UPDATE runs SET members=1 WHERE source=?',(source,))
            result=store.source_integrity()
            self.assertFalse(result['all_archive_members_processed'])
            self.assertFalse(result['sources'][0]['member_processing']['ledger_reconciled'])
            with store.db:
                store.db.execute('INSERT INTO members VALUES(?,?,?,?,?)',(source,'legacy','hash','unknown',None))
            result=store.source_integrity()
            self.assertEqual(result['sources'][0]['member_processing']['unclassified_members'],1)
            self.assertTrue(result['sources'][0]['member_processing']['ledger_reconciled'])
            self.assertFalse(result['all_archive_members_processed'])
            with store.db:
                store.db.execute("UPDATE members SET status='observed'")
            self.assertTrue(store.source_integrity()['all_archive_members_processed'])
            store.task.config['sources'][source]['version']='different'
            self.assertFalse(store.source_integrity()['all_archive_members_processed'])
        finally:store.close()
