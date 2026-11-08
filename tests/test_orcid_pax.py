"""The official mtime-only PAX header cannot enable semantic tar overrides."""
import gzip
import io
import json
import tarfile
import unittest

import test_streaming as fixtures
from gapi.orcid_stream import read_mtime_pax, stream_archive


def record(value):
    length=len(value)+3
    while True:
        result=str(length).encode()+b' '+value+b'\n'
        if len(result)==length:return result
        length=len(result)


def pax_archive(payload, *, following=True):
    raw=io.BytesIO()
    with tarfile.open(fileobj=raw,mode='w',format=tarfile.USTAR_FORMAT) as archive:
        header=tarfile.TarInfo('PaxHeaders/metadata')
        header.type=tarfile.XHDTYPE;header.size=len(payload)
        archive.addfile(header,io.BytesIO(payload))
        if following:
            header=tarfile.TarInfo(f'ORCID_2026_10_summaries/097/{fixtures.PERSON}.xml')
            header.size=len(fixtures.XML)
            archive.addfile(header,io.BytesIO(fixtures.XML))
    return gzip.compress(raw.getvalue(),mtime=0)


class OrcidPaxTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def test_official_timestamp_shape_parses_and_replays_exactly(self):
        data=pax_archive(record(b'mtime=1791890000.123'))
        store=self.store(data)
        try:
            stream_archive(store,'orcid-2026-summary',io.BytesIO(data))
            stream_archive(store,'orcid-2026-summary',io.BytesIO(data))
            self.assertEqual(store.db.execute('SELECT count(*) FROM events').fetchone()[0],1)
            self.assertTrue(store.source_integrity()['all_archives_verified'])
        finally:store.close()

    def test_path_size_link_sparse_and_global_like_records_stay_rejected(self):
        for value in (b'path=../../escape',b'linkpath=/outside',b'size=0',b'GNU.sparse.size=1',
                      b'hdrcharset=BINARY',b'mtime=NaN',b'mtime=1\nmtime=2'):
            with self.subTest(value=value):
                payload=record(value)
                with self.assertRaises(ValueError):
                    read_mtime_pax(io.BytesIO(payload+b'\0'*512),len(payload))

    def test_bad_lengths_truncation_padding_and_oversize_fail(self):
        payload=record(b'mtime=1')
        for body,size in ((b'99 mtime=1\n',11),(payload[:-1],len(payload)),
                          (payload+b'x'*512,len(payload)),(b'',129)):
            with self.subTest(size=size):
                with self.assertRaises((ValueError,EOFError)):
                    read_mtime_pax(io.BytesIO(body),size)

    def test_orphan_timestamp_cannot_verify_archive(self):
        data=pax_archive(record(b'mtime=1'),following=False)
        store=self.store(data)
        try:
            with self.assertRaisesRegex(ValueError,'no following member'):
                stream_archive(store,'orcid-2026-summary',io.BytesIO(data))
            self.assertFalse(store.source_integrity()['all_archives_verified'])
            self.assertEqual(store.db.execute('SELECT count(*) FROM events').fetchone()[0],0)
        finally:store.close()

    def test_retry_replaces_old_failure_before_reading_new_bytes(self):
        data=pax_archive(record(b'mtime=1'))
        store=self.store(data)
        try:
            store.start('orcid-2026-summary')
            store.status('orcid-2026-summary','access_failed',detail='previous attempt')
            case=self
            class CheckedStream(io.BytesIO):
                def read(self,size):
                    status=store.db.execute('SELECT status FROM runs WHERE source=?',
                        ('orcid-2026-summary',)).fetchone()[0]
                    case.assertEqual(status,'provisional')
                    return super().read(size)
            stream_archive(store,'orcid-2026-summary',CheckedStream(data))
            self.assertTrue(store.source_integrity()['all_archives_verified'])
        finally:store.close()

    def test_cli_preserves_parse_failure_instead_of_claiming_access_failure(self):
        from contextlib import redirect_stdout
        from types import SimpleNamespace
        from unittest.mock import patch
        from gapi.stream_cli import execute_task
        data=pax_archive(record(b'path=../escape'))
        store=self.store(data);task=store.task;store.close()
        class HTTP:
            def within(self,seconds):
                return __import__('contextlib').nullcontext()
            def open_archive(self,source):
                return self.open(task.config['sources'][source]['url'])
            def open(self,url):return io.BytesIO(data)
            def close(self):pass
        args=SimpleNamespace(command='ingest-orcid',source='orcid-2026-summary',
            max_members=None,max_new_members=None,max_seconds=10)
        output=io.StringIO()
        with patch('gapi.stream_cli.PublicHTTP',return_value=HTTP()):
            with redirect_stdout(output),self.assertRaises(SystemExit) as raised:
                execute_task(task,args)
        self.assertEqual(raised.exception.code,2)
        self.assertEqual(json.loads(output.getvalue())['sources'][0]['status'],'failed')
