"""Buffering must preserve logical output, field limits and hostile rejection."""
import unittest
from xml.parsers import expat

from gapi.orcid_stream import SummaryParser
from test_streaming import XML, PERSON


def parse(body, chunk, buffered):
    rows=[]
    parser=SummaryParser('p','source',PERSON+'.xml',rows.append)
    parser.parser.buffer_text=buffered
    try:
        for start in range(0,len(body),chunk):parser.feed(body[start:start+chunk])
        parser.feed(b'',True)
    except (ValueError,expat.ExpatError) as error:
        return rows,type(error).__name__,str(error)
    return rows,None,None


class XMLTextBufferingTests(unittest.TestCase):
    def test_buffer_is_enabled_and_explicitly_bounded(self):
        parser=SummaryParser('p','source',PERSON+'.xml',lambda _:None)
        self.assertTrue(parser.parser.buffer_text)
        self.assertEqual(parser.parser.buffer_size,8192)

    def test_text_entities_cdata_unicode_and_chunk_boundaries_match(self):
        variants=(XML,
            XML.replace(b'University A','Example &amp; &#20013; University'.encode()),
            XML.replace(b'University A','<![CDATA[Example 中 & University]]>'.encode()),
            XML.replace(b'University A',('A\n'*1024).encode()))
        for body in variants:
            for chunk in (1,7,64,65536):
                with self.subTest(chunk=chunk,bytes=len(body)):
                    baseline=parse(body,chunk,False)
                    self.assertIsNone(baseline[1])
                    self.assertEqual(parse(body,chunk,True),baseline)

    def test_overlong_field_and_wrong_identity_keep_rejection(self):
        for body in (XML.replace(b'University A',b'A'*2049),
                     XML.replace(PERSON.encode(),b'0000-0001-0000-0000')):
            for chunk in (1,7,65536):
                with self.subTest(chunk=chunk):
                    baseline=parse(body,chunk,False)
                    self.assertIsNotNone(baseline[1])
                    self.assertEqual(parse(body,chunk,True),baseline)

    def test_dtd_entities_truncated_and_malformed_xml_keep_rejection(self):
        variants=(b'<!DOCTYPE record [<!ENTITY x "unsafe">]>'+XML,
                  XML[:-10],XML.replace(b'</education-summary>',b'</wrong>'))
        for body in variants:
            for chunk in (1,7,65536):
                with self.subTest(chunk=chunk):
                    baseline=parse(body,chunk,False)
                    self.assertIsNotNone(baseline[1])
                    self.assertEqual(parse(body,chunk,True),baseline)

    def test_line_fragment_callbacks_are_reduced_without_losing_text(self):
        body=XML.replace(b'University A',(b'A\n'*1000))
        counts={};outputs={}
        for buffered in (False,True):
            rows=[];parser=SummaryParser('p','source',PERSON+'.xml',rows.append)
            parser.parser.buffer_text=buffered
            chunks=[];original=parser.characters
            def record(text):chunks.append(len(text));original(text)
            parser.parser.CharacterDataHandler=record
            parser.feed(body,True)
            counts[buffered]=len(chunks);outputs[buffered]=rows
        self.assertEqual(outputs[True],outputs[False])
        self.assertLess(counts[True],counts[False])
