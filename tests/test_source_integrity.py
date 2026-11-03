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
