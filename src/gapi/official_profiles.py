"""Deterministic official profile verification; references/copies never count twice.

Current adapter scope is explicit. Other layouts stay unsupported instead of being
guessed. P854 supplies a candidate URL only. Independent corroboration requires an
explicit same-ORCID link in the official profile, never a name or citation match.
"""
import codecs
import hashlib
import json
import re
from datetime import datetime, timezone
from html.parser import HTMLParser
from urllib.parse import urlsplit

from .career_rules import classify, normalized, valid_orcid
from .orcid_stream import CHUNK, private_person
from .task import digest

RULE = 'official-profile-education-lines/3'
ADAPTERS = {'www.tamucc.edu': {'path':'/business/about/directory/', 'employer':'ror:01mrfdz82'}}


class ProfileParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.main = False
        self.skip = 0
        self.heading = None
        self.heading_text = ''
        self.education = False
        self.buffer = ''
        self.degrees = []
        self.joined = set()
        self.academic = False
        self.orcids = set()

    def handle_starttag(self,tag,attrs):
        if tag == 'main':self.main = True
        if tag in ('script','style'):self.skip += 1
        if self.main and not self.skip and tag == 'a':
            href=dict(attrs).get('href') or ''
            link=urlsplit(href)
            identifier=link.path.removeprefix('/')
            if (link.scheme=='https' and link.netloc=='orcid.org' and not link.query
                    and not link.fragment and valid_orcid(identifier)):
                self.orcids.add(identifier)
        if self.main and tag in ('h1','h2','h3','h4'):
            self.boundary();self.heading=tag;self.heading_text=''
        if tag in ('p','li','br','div'):self.boundary()

    def handle_endtag(self,tag):
        if self.heading == tag:
            self.education = normalized(self.heading_text) == 'education'
            self.heading=None
        if tag in ('p','li','div','main'):self.boundary()
        if tag == 'main':self.main=False
        if tag in ('script','style'):self.skip=max(0,self.skip-1)

    def handle_data(self,text):
        if not self.main or self.skip:return
        if self.heading:
            self.heading_text += text
            if len(self.heading_text)>2048:raise ValueError('oversized profile heading')
        else:
            self.buffer += text
            if len(self.buffer)>8192:raise ValueError('oversized profile block')

    def boundary(self):
        text=' '.join(self.buffer.split());self.buffer=''
        if not text:return
        if self.education:
            match=re.fullmatch(r'([^,]{1,80}),\s*([^,]{1,250}),\s*(\d{4})',text)
            if match:
                degree,institution,date=match.groups()
                kind,stage=classify('education',degree,completed_only=True)
                if stage:self.degrees.append((stage,normalized(institution),int(date),digest(text)))
                if len(self.degrees)>100:raise ValueError('too many degree lines')
        joined=re.fullmatch(r'Joined the faculty in (\d{4})',text)
        if joined:self.joined.add(int(joined[1]))
        if re.fullmatch(r'(?:Assistant |Associate |Research |Emeritus )?Professor of [\w\s&-]{1,120}',text):
            self.academic=True


def profile_url(event):
    for ref in event.get('references',[]):
        for snak in ref.get('snaks',{}).get('P854',[]):
            url=snak.get('datavalue',{}).get('value')
            if not isinstance(url,str):continue
            parsed=urlsplit(url)
            adapter=ADAPTERS.get(parsed.hostname)
            if (adapter and parsed.scheme=='https' and not parsed.query and not parsed.fragment
                    and re.fullmatch(re.escape(adapter['path'])+r'[a-z-]+\.php',parsed.path)):
                return url
    return None


def fetch_profile(http,task,url):
    parser=ProfileParser();hasher=hashlib.sha256();decoder=codecs.getincrementaldecoder('utf-8')('strict')
    size=0
    with http.open(url,'text/html') as response:
        if 'text/html' not in response.headers.get('Content-Type',''):
            raise ValueError('official profile must be HTML')
        if response.geturl()!=url:
            raise ValueError('profile identity redirected; needs explicit review')
        while chunk:=response.read(CHUNK):
            size+=len(chunk);task.check_memory()
            if size>2*1024**2:raise ValueError('official profile exceeds bound')
            hasher.update(chunk);parser.feed(decoder.decode(chunk))
        parser.feed(decoder.decode(b'',final=True));parser.boundary();parser.close()
    return parser,hasher.hexdigest()


def check_official_profiles(store,http,*,limit=20):
    if not 1<=limit<=100:raise ValueError('official page limit must be between 1 and 100')
    selected={}
    for event_id,payload,institution in store.db.execute('''SELECT e.id,e.payload,COALESCE(a.institution,e.institution)
        FROM events e LEFT JOIN active_institution_aliases a ON a.alias=e.institution
        WHERE e.source='wikidata' ORDER BY e.id'''):
        event=json.loads(payload)
        url=profile_url(event)
        if url and (url in selected or len(selected)<limit):
            selected.setdefault(url,[]).append((event_id,event,institution))
    confirmed=failed=identity_bound=field_candidates=0
    for url,events in selected.items():
        try:
            parsed,sha=fetch_profile(http,store.task,url)
            subject=private_person(next(iter(parsed.orcids))) if len(parsed.orcids)==1 else None
            identity_bound+=int(subject is not None)
            checked=[]
            for event_id,event,institution in events:
                label=store.db.execute('SELECT name FROM entities WHERE id=?',(institution,)).fetchone()
                field=None
                if label and event['kind']=='education':
                    matches=[h for stage,name,date,h in parsed.degrees if stage==event.get('stage')
                        and name==normalized(label[0]) and date==event.get('year_end')]
                    if len(matches)==1:field=matches[0]
                if event['kind']=='employment' and parsed.academic and event.get('year_start') in parsed.joined:
                    if institution==ADAPTERS[urlsplit(url).hostname]['employer']:
                        field=digest({'academic':True,'joined':event['year_start']})
                if field:
                    bound=subject==event['person']
                    checked.append((event_id,url,sha,RULE,datetime.now(timezone.utc).isoformat(),
                        'official-underlying-source-confirmed' if bound else 'official-fields-match-identity-unresolved',
                        'same-origin-as-wikidata-reference',field))
            store.task.check(1024**2)
            with store.db:
                # A successful negative reread invalidates the previous derived
                # check. Historical frozen inputs retain the preceding evidence.
                store.db.execute('DELETE FROM official_checks WHERE url=?', (url,))
                store.db.execute('DELETE FROM official_profile_subjects WHERE url=?', (url,))
                if subject:
                    store.db.execute('INSERT INTO official_profile_subjects VALUES(?,?,?,?)',(url,sha,RULE,subject))
                store.db.executemany('INSERT OR REPLACE INTO official_checks VALUES(?,?,?,?,?,?,?,?)',checked)
            confirmed+=sum(row[5]=='official-underlying-source-confirmed' for row in checked)
            field_candidates+=sum(row[5]=='official-fields-match-identity-unresolved' for row in checked)
        except (OSError,ValueError,RuntimeError) as exc:
            store.failure('official-profile',url,str(exc));failed+=1
    return {'official_pages_checked':len(selected)-failed,'official_fields_confirmed':confirmed,
        'official_identity_bound_pages':identity_bound,'fields_with_unresolved_identity':field_candidates,
        'failed_pages':failed,'independent_source_promotions':0,
        'scope':'exact supported profile layout; referenced original evidence is not an independent second source'}
