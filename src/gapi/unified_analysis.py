"""Two frozen stores, one person-level calculation and recoverable compact state.

The source databases remain read-only. Scientific rules stay in global_analysis;
this module owns only the unified input traversal, accumulator and orchestration.
"""
from array import array
from collections import Counter, defaultdict
from contextlib import contextmanager
import hashlib
import io
import json
import math
from pathlib import Path
import sqlite3
import sys
import time
from types import SimpleNamespace
import zipfile

from .task import BudgetStop, Task, canonical, digest
from .career_store import exclusive_writer, orcid_archive_sources
from .event_storage import configure_reader
from .global_analysis import (_relation_people, select_analysis_members,
    observed_relations, _institution_node_row, _rank_graph, _rank_intensity,
    scenario_result, DEFAULT_CONFIG, training_observation_eligible, _write_aggregate)
from .analysis_checkpoint import input_binding

MAX_CHECKPOINT = 32*1024**2
MAX_DECODED = 256*1024**2


@contextmanager
def read_stores(paths):
    """Use the existing common writer exclusion once, without schema writes."""
    tasks=[Task(Path(p)) for p in paths]
    if len(tasks)!=2 or tasks[0].resource_root.resolve()!=tasks[1].resource_root.resolve():
        raise ValueError('unified stores require one existing shared resource owner')
    stores=[]
    with exclusive_writer(tasks[0]):
        try:
            for task in tasks:
                task.require_frozen()
                db=sqlite3.connect((task.root/'careers.sqlite').as_uri()+'?mode=ro',uri=True)
                db.execute('PRAGMA query_only=ON')
                db.execute('PRAGMA cache_size=-4096')
                db.execute('PRAGMA temp_store=MEMORY')
                configure_reader(db)
                stores.append(SimpleNamespace(task=task,db=db))
            yield stores
        finally:
            for store in stores:store.db.close()


class Graphs:
    """Shared integer endpoint index; numeric columns, no Python edge objects."""
    def __init__(self, sources):
        self.sources=list(sources)
        self.nodes=[];self.node_ids={};self.keys=array('Q');self.positions={}
        self.columns=[array('d') for _ in range(6+len(sources))]
        self.order=None

    def node(self, name):
        index=self.node_ids.get(name)
        if index is None:
            index=len(self.nodes)
            if index>=1000000:raise BudgetStop('unified institution index exceeds bound')
            self.node_ids[name]=index;self.nodes.append(name)
        return index

    def add(self, pair, weight, column, stage=None, recent=False):
        key=(self.node(pair[0])<<32)|self.node(pair[1])
        index=self.positions.get(key)
        if index is None:
            index=len(self.keys)
            if index>=2000000:raise BudgetStop('unified edge index exceeds bound')
            self.positions[key]=index;self.keys.append(key)
            for values in self.columns:values.append(0.)
        self.columns[column][index]+=weight
        if stage is not None:self.columns[stage][index]+=weight
        if recent:self.columns[5][index]+=weight
        self.order=None

    def sorted_order(self):
        if self.order is None:
            self.order=array('I',sorted(range(len(self.keys)),key=lambda i:(
                self.nodes[self.keys[i]>>32],self.nodes[self.keys[i]&0xffffffff])))
        return self.order

    def view(self,column):return GraphView(self,column)


class GraphView:
    def __init__(self, owner, column, allowed=None):self.owner,self.column,self.allowed=owner,column,allowed
    def __len__(self):return sum(v>0 for v in self.owner.columns[self.column]) if self.allowed is None else sum(1 for _ in self.items())
    def restrict_to(self,nodes):return GraphView(self.owner,self.column,nodes)
    def __iter__(self):return (pair for pair,_ in self.items())
    def items(self):
        g=self.owner;values=g.columns[self.column]
        for i in g.sorted_order():
            if values[i]>0:
                key=g.keys[i]
                pair=(g.nodes[key>>32],g.nodes[key&0xffffffff])
                if self.allowed is None or pair[0] in self.allowed and pair[1] in self.allowed:yield pair,values[i]
    def packed_rows(self):return ((a,b,w) for (a,b),w in self.items())


def _canonical_directory(stores):
    result={}
    for store in stores:
        for key,name,country,payload in store.db.execute('SELECT id,name,country,payload FROM entities'):
            value=(name,country,json.loads(payload))
            if key in result and result[key]!=value:
                # The annual freeze is the newer canonical directory; retain
                # its canonical ID, never merge distinct IDs through a name.
                if result[key][1]!=country or result[key][2].get('ror')!=value[2].get('ror'):
                    raise ValueError('canonical institution identities disagree')
                continue
            result[key]=value
    return result


def _all_people(store):
    cursor=store.db.cursor();cursor.row_factory=None
    try:
        previous=None
        for (person,) in cursor.execute('SELECT person FROM events INDEXED BY events_person ORDER BY person'):
            if person!=previous:yield person
            previous=person
    finally:cursor.close()


class UnifiedScan:
    """Physical annual traversal with an exact contiguity oracle at settlement.

    Other-store and primary non-archive people form a bounded lookup index. A
    person is joined before scientific rules run. A repeated physical person
    group makes the final group/unique-person equality fail, never a completion.
    """
    def __init__(self,stores,allowed=None):
        from .career_normalization import has_interpretations
        from .corroboration import decisions
        self.stores=tuple(stores);self.task=stores[0].task;self.allowed=allowed
        self.plan=select_analysis_members(stores,allowed_sources=allowed)
        self.binding=digest({'schema':'gapi-unified-input/1','inputs':[
            input_binding(s,allowed) for s in stores],'selection':self.plan})
        key=digest({'stores':[str(s.task.root.resolve()) for s in stores],
                    'representation':'physical-selected-person/2'})[:16]
        self.path=self.task.root/('analysis-unified-'+key+'.checkpoint')
        self.canonical=_canonical_directory(stores)
        self.aliases={}
        for store in stores:
            for alias,target in store.db.execute('SELECT alias,institution FROM active_institution_aliases'):
                if alias in self.aliases and self.aliases[alias]!=target:
                    raise ValueError('analysis institution aliases disagree')
                self.aliases[alias]=target
        self.sources=sorted({source for s in stores for source in s.task.config['sources']
                             if allowed is None or source in allowed})
        self.prepared=[{'enriched':has_interpretations(s),'verified':decisions(s,allowed_sources=allowed),
                        'canonical_ids':self.canonical} for s in stores]
        archives=orcid_archive_sources(stores[0].task)
        self.archive=archives[0] if len(archives)==1 else None
        self.other_people=set(_all_people(stores[1]))
        if len(self.other_people)>1000000:raise BudgetStop('secondary person index exceeds bound')
        self.primary_extra_people=set()
        for source in self.sources:
            if source!=self.archive:
                self.primary_extra_people.update(row[0] for row in stores[0].db.execute(
                    'SELECT DISTINCT person FROM events WHERE source=?',(source,)))
        self.other_people.update(self.primary_extra_people)
        if len(self.other_people)>1000000:raise BudgetStop('secondary person index exceeds bound')
        self.graphs=Graphs(self.sources)
        self.state={'schema':'gapi-unified-scan/1','binding':self.binding,'phase':'archive',
            'after_rowid':0,'after_other':None,'complete':False,'archive_groups':0,
            'people':0,'events':0,'statuses':{},'countries':[],'seen_other':[],
            'counts':{},'source_counts':{},'corroborated':{},'origins':{},
            'denominators':{},'relations':0,'directory_ids':[],'labels':{}}
        self.seen=set();self.directory_ids=set();self.countries=set()
        self.counts=Counter();self.source_counts=defaultdict(set);self.corroborated=Counter()
        self.origins=defaultdict(set);self.denominators=Counter();self.statuses=Counter();self.labels={}
        self.last_save=time.monotonic()

    def rows(self,index,**options):
        return _relation_people(self.stores[index],allowed_sources=self.allowed,
            excluded_members=set(map(tuple,self.plan['excluded_members'][index])),
            shared_aliases=self.aliases,prepared=self.prepared[index],capture_directory=True,**options)

    def combine(self, groups):
        events=[];summary={'events':0,'event_status':Counter(),'countries':set(),
                          'directory_ids':set(),'labels':{}}
        for _,rows,s in groups:
            events.extend(rows)
            if len(events)>4096:raise BudgetStop('combined person event fanout exceeds bound')
            summary['events']+=s['events'];summary['event_status'].update(s['event_status'])
            if summary['events']>4096:raise BudgetStop('combined person event fanout exceeds bound')
            summary['countries'].update(s['countries']);summary['directory_ids'].update(s['directory_ids'])
            for institution,label in s['labels'].items():
                if institution not in summary['labels'] or label[0]<summary['labels'][institution][0]:
                    summary['labels'][institution]=label
        return events,summary

    def consume(self,events,summary):
        self.state['people']+=bool(summary['events']);self.state['events']+=summary['events']
        self.statuses.update(summary['event_status']);self.countries.update(summary['countries'])
        self.directory_ids.update(summary['directory_ids'])
        for institution,label in summary['labels'].items():
            if institution not in self.labels or label[0]<self.labels[institution][0]:self.labels[institution]=label
        self.denominators.update({r['institution'] for r in events if training_observation_eligible(
            r['kind'],r.get('role'),r.get('year_start'),r.get('year_end'))})
        for (employer,training,stage),train,job in observed_relations(events):
            weight=DEFAULT_CONFIG['stage_weights'][stage];stage_index=list(DEFAULT_CONFIG['stage_weights']).index(stage)
            self.graphs.add((employer,training),weight,4,stage_index,job['year_start']>=2016)
            self.counts[training]+=1;self.source_counts[training].update(train['supporting_sources'])
            self.source_counts[training].update(job['supporting_sources']);self.state['relations']+=1
            if train['status']==job['status']=='corroborated':
                self.corroborated[training]+=1
                self.origins[training].update(set(train.get('evidence_origins',()))&set(job.get('evidence_origins',())))
        present={r['source'] for r in events}
        for n,source in enumerate(self.sources):
            if source not in present:
                # Exact same input facts: replay still uses the common owner.
                selected=events
            else:selected=[r for r in events if r['source']!=source]
            for (employer,training,stage),_,_ in observed_relations(selected):
                self.graphs.add((employer,training),DEFAULT_CONFIG['stage_weights'][stage],6+n)

    def _metadata(self):
        return dict(self.state,statuses=dict(self.statuses),countries=sorted(self.countries),
            counts=dict(self.counts),source_counts={k:sorted(v) for k,v in self.source_counts.items()},
            corroborated=dict(self.corroborated),origins={k:sorted(v) for k,v in self.origins.items()},
            denominators=dict(self.denominators),seen_other=sorted(self.seen),
            directory_ids=sorted(self.directory_ids),labels=self.labels,sources=self.sources)

    def save(self):
        self.check_binding();self.task.check_memory()
        buffer=io.BytesIO();hashes={};decoded=0
        with zipfile.ZipFile(buffer,'w',compression=zipfile.ZIP_DEFLATED,compresslevel=6) as archive:
            def write(name,chunks):
                nonlocal decoded
                sha=hashlib.sha256();size=0
                with archive.open(name,'w') as output:
                    for block in chunks:
                        sha.update(block);size+=len(block);decoded+=len(block)
                        if decoded>MAX_DECODED:raise BudgetStop('unified checkpoint decoded bound')
                        output.write(block)
                        if buffer.tell()>MAX_CHECKPOINT:raise BudgetStop('unified checkpoint packed bound')
                hashes[name]={'bytes':size,'sha256':sha.hexdigest()}
            encoder=json.JSONEncoder(sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False)
            write('meta.json',(x.encode('ascii') for x in encoder.iterencode(self._metadata())))
            write('nodes.json',(x.encode('ascii') for x in encoder.iterencode(self.graphs.nodes)))
            for name,values in [('keys',self.graphs.keys)]+[(str(i),v) for i,v in enumerate(self.graphs.columns)]:
                if sys.byteorder!='little':raise ValueError('unsupported checkpoint host byte order')
                data=memoryview(values).cast('B')
                write(name+'.bin',(data[i:i+65536] for i in range(0,len(data),65536)))
            archive.writestr('integrity.json',canonical(hashes))
        body=buffer.getvalue()
        if len(body)>MAX_CHECKPOINT:raise BudgetStop('unified checkpoint packed bound')
        _write_aggregate(self.task,self.path,body);self.last_save=time.monotonic()

    def load(self, *, predecessor_code=None):
        if not self.path.exists():return False
        if getattr(self,'_released_person_index',False):
            self.other_people=set(_all_people(self.stores[1]))|self.primary_extra_people
            self._released_person_index=False
        expected_binding=self.binding
        if predecessor_code is not None:
            current={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in Path(__file__).parent.glob('*.py')}
            if (set(predecessor_code)!=set(current) or any(predecessor_code[k]!=current[k]
                    for k in current if k not in ('unified_analysis.py','global_analysis.py'))):
                raise ValueError('unified recovery permits only explicit bounded representation code migration')
            expected_binding=digest({'schema':'gapi-unified-input/1','inputs':[
                input_binding(s,self.allowed,predecessor_code=predecessor_code) for s in self.stores],'selection':self.plan})
        if self.path.is_symlink() or self.path.is_junction() or self.path.stat().st_size>MAX_CHECKPOINT:
            raise ValueError('invalid unified checkpoint file')
        with zipfile.ZipFile(self.path) as archive:
            entries=archive.infolist()
            if len(entries)!=len({v.filename for v in entries}) or sum(v.file_size for v in entries)>MAX_DECODED:
                raise ValueError('invalid unified checkpoint entries')
            hashes=json.loads(archive.read('integrity.json'))
            expected={'meta.json','nodes.json','keys.bin',*(str(i)+'.bin' for i in range(6+len(self.sources)))}
            if set(hashes)!=expected or {v.filename for v in entries}!=expected|{'integrity.json'}:
                raise ValueError('unexpected unified checkpoint entries')
            def read(name):
                raw=archive.read(name);d=hashes[name]
                if len(raw)!=d['bytes'] or hashlib.sha256(raw).hexdigest()!=d['sha256']:
                    raise ValueError('unified checkpoint content mismatch')
                return raw
            state=json.loads(read('meta.json'))
            if state.get('schema')!='gapi-unified-scan/1' or state.get('binding')!=expected_binding or state.get('sources')!=self.sources:
                raise ValueError('unified checkpoint input or version drift')
            if predecessor_code is not None and not state.get('complete'):
                raise ValueError('orchestration migration requires a completed immutable person scan')
            state['binding']=self.binding
            g=Graphs(self.sources);g.nodes=json.loads(read('nodes.json'));g.node_ids={n:i for i,n in enumerate(g.nodes)}
            if len(g.node_ids)!=len(g.nodes):raise ValueError('duplicate checkpoint institution')
            g.keys.frombytes(read('keys.bin'));g.positions={k:i for i,k in enumerate(g.keys)}
            if len(g.positions)!=len(g.keys) or any(k>>32>=len(g.nodes) or k&0xffffffff>=len(g.nodes) for k in g.keys):
                raise ValueError('invalid checkpoint graph endpoints')
            for i,column in enumerate(g.columns):
                column.frombytes(read(str(i)+'.bin'))
                if len(column)!=len(g.keys) or any(not math.isfinite(v) or v<0 for v in column):
                    raise ValueError('invalid checkpoint graph column')
            if (type(state['people']) is not int or not 0<=state['people']<=state['events']
                    or sum(state['statuses'].values())!=state['events']
                    or state['phase'] not in ('archive','other','complete')
                    or type(state['complete']) is not bool or state['complete']!=(state['phase']=='complete')):
                raise ValueError('invalid unified checkpoint accounting')
            self.graphs=g;self.state=state;self.seen=set(state['seen_other'])
            if not self.seen<=self.other_people:raise ValueError('checkpoint secondary identity drift')
            self.directory_ids=set(state['directory_ids']);self.labels=state['labels'];self.countries=set(state['countries'])
            self.statuses=Counter(state['statuses']);self.counts=Counter(state['counts'])
            self.source_counts=defaultdict(set,{k:set(v) for k,v in state['source_counts'].items()})
            self.corroborated=Counter(state['corroborated']);self.origins=defaultdict(set,{k:set(v) for k,v in state['origins'].items()})
            self.denominators=Counter(state['denominators'])
        return True

    def check_binding(self):
        actual=digest({'schema':'gapi-unified-input/1','inputs':[
            input_binding(s,self.allowed) for s in self.stores],'selection':self.plan})
        if actual!=self.binding:raise ValueError('unified analysis input changed')

    def run(self,*,seconds,max_people=None,progress=None):
        if not math.isfinite(seconds) or seconds<=0 or isinstance(seconds,bool):raise ValueError('invalid unified time bound')
        start=time.monotonic();processed=0;last=start
        def settle(person,groups):
            nonlocal processed,last
            events,summary=self.combine(groups);self.consume(events,summary);processed+=1
            if person in self.other_people:self.seen.add(person)
            if processed%1000==0:self.task.check_memory()
            if progress and time.monotonic()-last>=30:
                progress(self.report());last=time.monotonic()
        def boundary():
            if time.monotonic()-start>=seconds or max_people is not None and processed>=max_people:
                self.save();return True
            if time.monotonic()-self.last_save>=180:self.save()
            return False
        if self.state['phase']=='archive':
            rows=self.rows(0,physical_source=self.archive,after_rowid=self.state['after_rowid']) if self.archive else iter(())
            try:
                batch=[]
                while True:
                    for _ in range(1000-len(batch)):
                        group=next(rows,None)
                        if group is None:break
                        batch.append(group)
                    if not batch:break
                    if len({g[0] for g in batch})!=len(batch):
                        raise ValueError('physical person groups repeat within a batch')
                    ids=[p for p,_,_ in batch if p in self.other_people]
                    additions={}
                    if ids:
                        for index in (1,):
                            for group in self.rows(index,persons=ids):
                                additions.setdefault(group[0],[]).append(group)
                        # Primary API snapshots are rare; fetch once per batch
                        # with a source filter before scientific classification.
                        original_allowed=self.allowed
                        try:
                            self.allowed=set(self.sources)-{self.archive}
                            for group in self.rows(0,persons=ids):additions.setdefault(group[0],[]).append(group)
                        finally:self.allowed=original_allowed
                    for person,events,summary in batch:
                        settle(person,[(person,events,summary),*additions.get(person,[])])
                        self.state['archive_groups']+=1;self.state['after_rowid']=summary['end_rowid']
                        if boundary():return self.report()
                    batch=[]
            finally:rows.close() if hasattr(rows,'close') else None
            # Covering index contains no payload reads. Subtract primary people
            # with no archive events, then compare with physical group count.
            total=sum(1 for _ in _all_people(self.stores[0]));api_only=0
            for person in self.primary_extra_people:
                if not self.stores[0].db.execute('SELECT 1 FROM events WHERE person=? AND source=? LIMIT 1',(person,self.archive)).fetchone():api_only+=1
            if total-api_only!=self.state['archive_groups']:
                raise ValueError('physical person groups differ from independent unique-person count')
            self.state['phase']='other';self.save()
        if self.state['phase']=='other':
            remaining=sorted(self.other_people-self.seen)
            for offset in range(0,len(remaining),1000):
                ids=[p for p in remaining[offset:offset+1000] if self.state['after_other'] is None or p>self.state['after_other']]
                if not ids:continue
                groups={}
                for index in (0,1):
                    for group in self.rows(index,persons=ids):groups.setdefault(group[0],[]).append(group)
                for person in ids:
                    settle(person,groups.get(person,[]));self.state['after_other']=person
                    if boundary():return self.report()
            self.state.update(phase='complete',complete=True);self.save()
        self.check_binding();return self.report()

    def report(self):
        return {'phase':self.state['phase'],'complete':self.state['complete'],'people':self.state['people'],
            'events':self.state['events'],'relations':self.state['relations'],'edges':len(self.graphs.view(4)),
            'archive_groups':self.state['archive_groups'],'checkpoint':str(self.path),
            'checkpoint_bytes':self.path.stat().st_size if self.path.exists() else 0,
            'rss':self.task.check_memory(),'binding':self.binding}

    def result(self,progress=None):
        if not self.state['complete']:raise ValueError('unified scan is incomplete')
        self.check_binding();store=self.stores[0];g=self.graphs
        # Accumulation indexes and deserialized copies are no longer consumers
        # once the entire person traversal has committed. Numeric columns,
        # labels, denominators and all public rules remain unchanged.
        g.positions.clear();g.node_ids.clear()
        self.other_people.clear();self._released_person_index=True
        for name in ('statuses','countries','counts','source_counts','corroborated','origins',
                     'denominators','seen_other','directory_ids','labels','sources'):
            self.state.pop(name,None)
        import gc
        gc.collect()
        edges=g.view(4)
        directory=dict(self.canonical)
        unresolved_metadata={'identity_status':'reported-label-unresolved'}
        for key,(_,name,country) in self.labels.items():
            if key not in directory:directory[key]=(name,country,unresolved_metadata)
        nodes=sorted({n for pair in edges for n in pair});network,conv=_rank_graph(store,nodes,edges,.85,1e-12,200)
        order={n:i+1 for i,n in enumerate(sorted(network,key=lambda n:(-network[n],n)))}
        scenarios={};expanded={}
        def scenario(name,view,alpha=.85,prior=None):
            if progress:progress({'phase':'sensitivity','scenario':name})
            a,b=scenario_result(store,nodes,view,alpha,prior);scenarios[name]=a
            if b is not None:expanded[name]=b
        for alpha in (.65,.85,.95):scenario('alpha_'+str(alpha),edges,alpha)
        for i,stage in enumerate(DEFAULT_CONFIG['stage_weights']):scenario('stage_only_'+stage,g.view(i))
        for i,source in enumerate(self.sources):scenario('delete_source_'+source,g.view(6+i))
        scenario('employment_since_2016',g.view(5))
        groups=Counter(directory.get(n,(None,None,{}))[2].get('region') or 'unknown' for n in nodes)
        if nodes:scenario('region_balanced_prior',edges,prior={n:1/groups[directory.get(n,(None,None,{}))[2].get('region') or 'unknown'] for n in nodes})
        intensity,_=_rank_intensity(store,nodes,edges,self.denominators)
        from .aggregate_chunks import AggregateRows
        ids=sorted(self.directory_ids|set(nodes))
        def node_rows():
            for i,node in enumerate(ids):
                if i%1000==0:self.task.check_memory()
                yield _institution_node_row(node,directory,network,order,intensity,self.denominators,
                    self.counts,self.source_counts,self.corroborated,self.origins,scenarios)
        def edge_rows():
            for i in g.sorted_order():
                weight=g.columns[4][i]
                if weight<=0:continue
                key=g.keys[i]
                yield {'source':g.nodes[key>>32],'target':g.nodes[key&0xffffffff],'weight':weight,
                    'stages':{stage:g.columns[j][i] for j,stage in enumerate(DEFAULT_CONFIG['stage_weights']) if g.columns[j][i]>0}}
        member_status=Counter()
        for index,s in enumerate(self.stores):
            # No status index exists on the real 31-million-member table.
            # Stream scalar columns instead of a SQLite temporary GROUP BY tree.
            cursor=s.db.cursor();cursor.row_factory=None
            try:
                for number,(source,status) in enumerate(cursor.execute('SELECT source,status FROM members NOT INDEXED'),1):
                    if self.allowed is None or source in self.allowed:member_status[status]+=1
                    if number%100000==0:self.task.check_memory()
            finally:cursor.close()
            for source,locator in self.plan['excluded_members'][index]:
                status=s.db.execute('SELECT status FROM members WHERE source=? AND locator=?',(source,locator)).fetchone()[0]
                member_status[status]-=1
        result={'schema_version':'gapi-global-observed/v1','dataset_kind':'real/global-public-observations',
            'publication_status':'local-only-source-license-review',
            'scope':{'complete_global_census':False,'enumeration_complete':False,'source_scope':'two frozen source stores; person-level selected member versions'},
            'coverage':{'people':self.state['people'],'events':self.state['events'],'event_status':dict(self.statuses),
                        'countries':sorted(self.countries),'member_status':dict(member_status)},
            'nodes':AggregateRows(len(ids),node_rows),'edges':AggregateRows(len(edges),edge_rows),
            'algorithm':{'method_version':'gapi-observed-network/4','direction':'employer -> training',
                'config':DEFAULT_CONFIG,'network_universe':nodes,'unified_input_binding':self.binding,
                'input_selection':{'identical_members':self.plan['identical_members'],'changed_members':self.plan['changed_members'],
                                   'failed_versions_excluded':self.plan['failed_versions_excluded']}},
            'convergence':conv,'sensitivity':scenarios,'expanded_universe_sensitivity':expanded,
            'uncertainty':{'kind':'parameter-scenarios-only','unknown_global_missingness_covered':False,
                'rank_range_scope':'fixed-reference-universe; excludes prior-only and expanded-universe scenarios',
                'not_run':['discipline','bootstrap','english_source_deletion','official_profile_loss'],
                'intensity_denominator':'observed training people; not all graduates or job seekers'},
            'evidence_adjudication':{'corroborated_events':sum(len(p['verified']) for p in self.prepared),
                'canonical_events_modified':0,'scope':'existing exact official-evidence policy; no new corroboration'},
            'limitations':['Observed scores are not employment rates or causal training effects.',
                'Formal ranking and composite data are unavailable.','Independent career verification remains incomplete.']}
        from .nobel import public_inventory
        from .award_projection import public_projection
        for field,reader in [('official_award_inventory',public_inventory),('official_award_attribution',public_projection)]:
            for source in self.stores:
                value=reader(source)
                if value is not None:
                    if field in result and result[field]!=value:raise ValueError('unified award inventories disagree')
                    result[field]=value
        self.check_binding()
        return result


def export_unified(paths, *, seconds, progress=None, predecessor_code=None):
    """Public CLI consumer: frozen normalized stores to a private aggregate.

    This does not promote a publication status. The existing independent replay
    and source-policy gate must subsequently qualify the selected result.
    """
    from .aggregate_partitions import write_result
    start=time.monotonic()
    with read_stores(paths) as stores:
        scan=UnifiedScan(stores);scan.load(predecessor_code=predecessor_code)
        remaining=seconds-(time.monotonic()-start)
        if remaining<=0:raise BudgetStop('unified deadline reached before scan')
        run=scan.run(seconds=remaining,progress=progress)
        if not run['complete']:return dict(run,publication_ready=False)
        def deadline(item):
            if time.monotonic()-start>=seconds:raise BudgetStop('unified calculation deadline reached')
            if progress:progress(item)
        result=scan.result(progress=deadline)
        target=stores[0].task.root/'global-unified-partitions'
        manifest=write_result(stores[0].task,result,target,progress=deadline)
        scan.check_binding()
        return dict(run,manifest=str(target/'manifest.json'),manifest_sha256=digest(manifest),
                    logical_sha256=manifest['logical_sha256'],nodes=manifest['nodes'],
                    seconds=time.monotonic()-start,publication_ready=False)
