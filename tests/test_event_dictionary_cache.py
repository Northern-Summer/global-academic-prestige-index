import hashlib
import sqlite3
import unittest
import zlib
from unittest.mock import patch

from gapi.event_storage import (
    DICTIONARY_DDL, DICT_MAGIC, MAX_DICTIONARY_BYTES,
    _decoded_stored_dictionary, decode_event, load_dictionary, pack,
)


class EventDictionaryCacheTests(unittest.TestCase):
    def setUp(self):
        _decoded_stored_dictionary.cache_clear()
        self.db = sqlite3.connect(':memory:')
        self.db.execute(DICTIONARY_DDL)
        self.dictionary = ('公开来源和机构履历 ' * 80).encode()
        self.key = hashlib.sha256(self.dictionary).digest()
        self.body = zlib.compress(self.dictionary)
        self.db.execute('INSERT INTO event_dictionaries VALUES (?,?)', (self.key, self.body))

    def tearDown(self):
        self.db.close()
        _decoded_stored_dictionary.cache_clear()

    def test_repeated_reads_query_storage_but_decompress_once(self):
        statements = []
        self.db.set_trace_callback(statements.append)
        with patch('gapi.event_storage.zlib.decompressobj', wraps=zlib.decompressobj) as decoder:
            for _ in range(4):
                self.assertEqual(load_dictionary(self.db, self.key), self.dictionary)
            self.assertEqual(decoder.call_count, 1)
        self.assertEqual(sum(s.startswith('SELECT compressed_bytes') for s in statements), 4)

    def test_warm_cache_cannot_hide_change_delete_or_rollback(self):
        text = self.dictionary.decode() * 2
        value = pack(text, dictionary=self.dictionary)
        self.assertTrue(value.startswith(DICT_MAGIC))
        self.db.commit()
        self.assertEqual(decode_event(self.db, value), text)
        for body in (zlib.compress(b'wrong'), self.body[:-1], self.body + b'extra'):
            self.db.execute('UPDATE event_dictionaries SET compressed_bytes=?', (body,))
            with self.assertRaises(ValueError):
                decode_event(self.db, value)
            self.db.rollback()
            self.assertEqual(decode_event(self.db, value), text)
        self.db.execute('DELETE FROM event_dictionaries')
        with self.assertRaisesRegex(ValueError, 'missing'):
            decode_event(self.db, value)
        self.db.rollback()
        self.assertEqual(decode_event(self.db, value), text)

    def test_shared_cache_does_not_hide_different_database_contents(self):
        self.assertEqual(load_dictionary(self.db, self.key), self.dictionary)
        other = sqlite3.connect(':memory:')
        try:
            other.execute(DICTIONARY_DDL)
            text = self.dictionary.decode() * 2
            value = pack(text, dictionary=self.dictionary)
            with self.assertRaisesRegex(ValueError, 'missing'):
                decode_event(other, value)
            other.execute('INSERT INTO event_dictionaries VALUES (?,?)',
                          (self.key, zlib.compress(b'wrong')))
            with self.assertRaisesRegex(ValueError, 'integrity'):
                decode_event(other, value)
        finally:
            other.close()

    def test_corrupt_oversized_and_expanding_bodies_are_not_cached(self):
        for body in (b'', b'x' * (MAX_DICTIONARY_BYTES + 65),
                     zlib.compress(b'x' * (MAX_DICTIONARY_BYTES + 1)),
                     zlib.compress(b''), b'invalid', self.body + zlib.compress(b'extra')):
            self.db.execute('UPDATE event_dictionaries SET compressed_bytes=?', (body,))
            with self.assertRaises(ValueError):
                load_dictionary(self.db, self.key)
            self.assertEqual(_decoded_stored_dictionary.cache_info().currsize, 0)

    def test_cache_is_bounded_and_evicted_content_remains_readable(self):
        for i in range(20):
            raw = bytes([i]) * MAX_DICTIONARY_BYTES
            key = hashlib.sha256(raw).digest()
            self.db.execute('INSERT INTO event_dictionaries VALUES (?,?)', (key, zlib.compress(raw)))
            self.assertEqual(load_dictionary(self.db, key), raw)
        self.assertEqual(_decoded_stored_dictionary.cache_info().currsize, 8)
        self.assertEqual(load_dictionary(self.db, self.key), self.dictionary)

    def test_maximum_incompressible_dictionary_is_accepted(self):
        raw = b''.join(hashlib.sha256(i.to_bytes(4, 'big')).digest()
                       for i in range(MAX_DICTIONARY_BYTES // 32))
        key = hashlib.sha256(raw).digest()
        body = zlib.compress(raw)
        self.assertGreater(len(body), MAX_DICTIONARY_BYTES)
        self.assertLessEqual(len(body), MAX_DICTIONARY_BYTES + 64)
        self.db.execute('INSERT INTO event_dictionaries VALUES (?,?)', (key, body))
        self.assertEqual(load_dictionary(self.db, key), raw)


if __name__ == '__main__':
    unittest.main()
