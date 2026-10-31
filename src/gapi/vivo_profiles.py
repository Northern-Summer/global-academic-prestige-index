"""Bounded CU Experts education parser; no publication-derived affiliations.

Only the official profile's education list and dedicated ORCID list are read.
This adapter deliberately makes no employment or award assertions.
"""
import re
from html.parser import HTMLParser
from urllib.parse import urlsplit

from .career_rules import normalized,valid_orcid
from .career_normalization import completed_education_stage
from .task import digest


class VivoEducationParser(HTMLParser):
    VOID={'area','base','br','col','embed','hr','img','input','link','meta','param','source','track','wbr'}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.stack=[];self.content=None;self.education=None;self.identity=None
        self.item=None;self.name_depth=None;self.year_depth=None
        self.degrees=[];self.orcids=set();self.joined=set();self.academic=False

    def handle_starttag(self,tag,attrs):
        attrs=dict(attrs)
        if tag not in self.VOID:self.stack.append((tag,attrs))
        depth=len(self.stack)
        if depth>128:raise ValueError('profile nesting exceeds bound')
        if tag=='div' and attrs.get('id')=='wrapper-content':
            if self.content is not None:raise ValueError('ambiguous profile content')
            self.content=depth
        if self.content is None or any(t in ('script','style') for t,_ in self.stack):return
        if tag=='ul' and attrs.get('id')=='RO_0000056-EducationalProcess-List':
            if self.education is not None:raise ValueError('nested education lists')
            self.education=depth
        if tag=='ul' and attrs.get('id')=='orcidId-noRangeClass-List':self.identity=depth
        if self.identity is not None and tag=='a':
            link=urlsplit(attrs.get('href') or '')
            identifier=link.path.removeprefix('/')
            # This is a literal identifier in official HTML; no HTTP redirect is followed.
            if (link.scheme in ('http','https') and link.netloc=='orcid.org'
                    and not link.query and not link.fragment and valid_orcid(identifier)):
                self.orcids.add(identifier)
                if len(self.orcids)>8:raise ValueError('too many profile identities')
        if tag=='li' and self.education is not None:
            if self.item is not None:raise ValueError('nested education items')
            self.item={'depth':depth,'text':'','name':'','year':'','names':0,'years':0}
        if self.item is not None:
            college=any(a.get('itemtype') in ('http://schema.org/CollegeOrUniversity',
                'https://schema.org/CollegeOrUniversity') for _,a in self.stack)
            if tag=='a' and attrs.get('itemprop')=='name' and college:
                self.name_depth=depth;self.item['names']+=1
            if tag=='span' and 'listDateTime' in attrs.get('class','').split():
                self.year_depth=depth;self.item['years']+=1

    def handle_startendtag(self,tag,attrs):
        self.handle_starttag(tag,attrs)
        if tag not in self.VOID:self.handle_endtag(tag)

    def handle_data(self,text):
        if self.item is None or any(t in ('script','style') for t,_ in self.stack):return
        self.item['text']+=text
        if self.name_depth is not None:self.item['name']+=text
        if self.year_depth is not None:self.item['year']+=text
        if len(self.item['text'])>8192:raise ValueError('oversized education item')

    def handle_endtag(self,tag):
        indices=[i for i,(t,_) in enumerate(self.stack) if t==tag]
        if not indices:return
        depth=indices[-1]+1
        if self.item is not None and depth<=self.item['depth']:
            row=self.item;self.item=None
            name=' '.join(row['name'].split());year=row['year'].strip()
            text=' '.join(row['text'].split())
            if name and row['names']==row['years']==1 and re.fullmatch(r'\d{4}',year):
                match=re.fullmatch(r'([^,]{1,80}),\s*'+re.escape(name)+r'\s+'+year,text)
                if match:
                    stage=completed_education_stage(match[1])
                    if stage:
                        self.degrees.append((stage,normalized(name),int(year),digest(text)))
                        if len(self.degrees)>100:raise ValueError('too many degrees')
        if self.name_depth is not None and depth<=self.name_depth:self.name_depth=None
        if self.year_depth is not None and depth<=self.year_depth:self.year_depth=None
        if self.education is not None and depth<=self.education:self.education=None
        if self.identity is not None and depth<=self.identity:self.identity=None
        if self.content is not None and depth<=self.content:self.content=None
        del self.stack[depth-1:]

    def boundary(self):
        # Incomplete list items never turn into facts at EOF.
        pass
