"""Private first-pass graph recovery; not a source freeze or ranking verdict.

The caller holds CareerStore's sole-writer lock. Complete person groups are the
commit boundary; the owned aggregate writer preserves the preceding checkpoint
on any failed replacement. No source rows are written or recollected.
"""
import gzip
import hashlib
import io
import json
import math
import struct
import time
import zlib
from collections import Counter
from pathlib import Path

from .task import BudgetStop, digest

MAGIC = b'GAPIgraph1\0'
MAX_DECODED = 128*1024**2
MAX_PACKED = 32*1024**2
CHECKPOINT_SECONDS = 60
COMPACT_MAGIC = b'GAPICMP1'


class PackedRows:
    """Keep fixed-width records as bytes, rather than one tuple per edge."""
    def __init__(self, data, format):
        self.data=memoryview(data)
        self.record=struct.Struct(format)

    def __len__(self):
        return len(self.data)//self.record.size

    def __iter__(self):
        return self.record.iter_unpack(self.data)

    def __getitem__(self, index):
        return self.record.unpack_from(self.data,index*self.record.size)


class AdditiveGraph:
    """Sorted, immutable edge overlay for a verified append-only person delta.

    Input-generation and person disjointness checks belong to the caller. This
    view only combines numeric edge weights; it cannot authorize checkpoint
    migration or infer that replacement/deletion inputs are additive.
    """
    def __init__(self, baseline, additions):
        if not isinstance(additions,dict) or len(additions)>200000:
            raise ValueError('invalid additive graph bound')
        self.baseline=baseline
        self.order=None
        # Compact checkpoints preserve accumulation order. Store only a uint32
        # permutation; never duplicate their half-million endpoint tuples.
        if isinstance(baseline,PackedMap) and baseline.kind=='edge':
            from array import array
            def key(i):
                row=baseline.rows[i]
                return baseline.nodes[row[0]],baseline.nodes[row[1]]
            self.order=array('I',sorted(range(len(baseline)),key=key))
        self.additions={}
        for pair,weight in additions.items():
            if (not isinstance(pair,tuple) or len(pair)!=2
                    or any(not isinstance(x,str) or not x for x in pair)
                    or type(weight) not in (int,float) or not math.isfinite(weight) or weight<=0):
                raise ValueError('invalid additive edge')
            self.additions[pair]=float(weight)
        overlap=0;previous=None
        for pair,weight in self._baseline_items():
            if (previous is not None and pair<=previous) or not math.isfinite(weight) or weight<=0:
                raise ValueError('additive graph requires ordered positive baseline')
            previous=pair;overlap+=pair in self.additions
        self.count=len(baseline)+len(self.additions)-overlap

    def __len__(self):return self.count

    def _baseline_items(self):
        if self.order is None:
            yield from self.baseline.items()
        else:
            for i in self.order:
                a,b,w=self.baseline.rows[i]
                yield (self.baseline.nodes[a],self.baseline.nodes[b]),w

    def items(self):
        pending=iter(sorted(self.additions.items()));new=next(pending,None)
        for pair,weight in self._baseline_items():
            while new is not None and new[0]<pair:
                yield new;new=next(pending,None)
            if new is not None and new[0]==pair:
                weight+=new[1];new=next(pending,None)
            yield pair,weight
        while new is not None:
            yield new;new=next(pending,None)

    def __iter__(self):
        return (pair for pair,_ in self.items())

    def packed_rows(self):
        return ((pair[0],pair[1],weight) for pair,weight in self.items())


class PackedMap:
    """Read-only compact edge/stage mapping used after a completed resume."""
    def __init__(self, nodes, rows, kind):
        self.nodes, self.rows, self.kind = nodes, rows, kind
        self._index = None
        self._nodes = None

    def __len__(self):
        return len(self.rows)

    def __iter__(self):
        for row in self.rows:
            yield (self.nodes[row[0]], self.nodes[row[1]])

    def items(self):
        if self.kind == 'stage':
            for a,b,w1,w2,w3,w4 in self.rows:
                yield ((self.nodes[a],self.nodes[b]),
                       Counter({k:w for k,w in zip(
                           ('bachelor','master','doctorate','postdoc'),(w1,w2,w3,w4)) if w}))
        else:
            for a,b,w in self.rows:
                yield ((self.nodes[a],self.nodes[b]),w)

    def __getitem__(self, key):
        if self._index is None:
            self._index={(row[0]<<32)|row[1]:i for i,row in enumerate(self.rows)}
            self._nodes={node:i for i,node in enumerate(self.nodes)}
        a=self._nodes[key[0]]; b=self._nodes[key[1]]
        row=self.rows[self._index[(a<<32)|b]]
        if self.kind == 'stage':
            return Counter({k:w for k,w in zip(
                ('bachelor','master','doctorate','postdoc'),row[2:]) if w})
        return row[2]

    def packed_rows(self):
        for row in self.rows:
            yield (self.nodes[row[0]],self.nodes[row[1]],row[2])


def input_binding(store, allowed, *, predecessor_code=None):
    """Bind local file generation under writer exclusion, config and code bytes.

    This is a local recovery identity, not a portable logical source digest.
    Restored/copied/modified DB generations deliberately require a new pass.
    """
    path=store.task.root/'careers.sqlite'
    if store.db.in_transaction:
        raise ValueError('analysis recovery requires settled input')
    if any(Path(str(path)+suffix).exists() for suffix in ('-journal','-wal')):
        raise ValueError('analysis recovery requires settled database files')
    info=path.stat()
    code={p.name:hashlib.sha256(p.read_bytes()).hexdigest()
          for p in sorted(Path(__file__).parent.glob('*.py'))}
    if predecessor_code is not None:
        if (set(predecessor_code)!=set(code)
                or any(not isinstance(v,str) or len(v)!=64
                       or any(c not in '0123456789abcdef' for c in v)
                       for v in predecessor_code.values())):
            raise ValueError('invalid predecessor code manifest')
        code=dict(predecessor_code)
    return digest({'database':str(path.absolute()),'size':info.st_size,
        'mtime_ns':info.st_mtime_ns,'inode':info.st_ino,'device':info.st_dev,
        'config':store.task.config,'code':code,
        'allowed_sources':None if allowed is None else sorted(allowed)})


class CoverageCheckpoint:
    """Completed scalar coverage only; no personal identifiers are retained."""
    def __init__(self,store,allowed):
        self.store,self.allowed=store,allowed
        self.binding=input_binding(store,allowed)
        key=digest(None if allowed is None else sorted(allowed))[:16]
        self.path=store.task.root/('analysis-coverage-'+key+'.json')

    @staticmethod
    def validate(value):
        if (not isinstance(value,dict) or set(value)!={'people','events','countries','event_status','member_status'}
                or any(type(value[k]) is not int or value[k]<0 for k in ('people','events'))
                or value['people']>value['events']
                or not isinstance(value['countries'],list)
                or any(not isinstance(x,str) for x in value['countries'])
                or value['countries']!=sorted(set(value['countries']))):
            raise ValueError('invalid coverage checkpoint counts')
        for field in ('event_status','member_status'):
            if (not isinstance(value[field],dict) or any(not isinstance(k,str)
                    or type(v) is not int or v<=0 for k,v in value[field].items())):
                raise ValueError('invalid coverage checkpoint status counts')
        if sum(value['event_status'].values())!=value['events']:
            raise ValueError('coverage event status reconciliation mismatch')

    def load(self):
        if not self.path.exists():return None
        if self.path.is_symlink() or self.path.is_junction():raise ValueError('linked coverage checkpoint')
        with self.path.open('rb') as stream:raw=stream.read(1024**2+1)
        if len(raw)>1024**2:raise ValueError('coverage checkpoint bound')
        def unique(pairs):
            result={}
            for k,v in pairs:
                if k in result:raise ValueError('duplicate coverage checkpoint key')
                result[k]=v
            return result
        value=json.loads(raw,object_pairs_hook=unique)
        if (set(value)!={'schema','binding','coverage'} or value['schema']!='gapi-coverage/1'
                or value['binding']!=self.binding):raise ValueError('coverage checkpoint input or version drift')
        self.validate(value['coverage']);return value['coverage']

    def save(self,coverage):
        from .task import canonical
        from .global_analysis import _write_aggregate
        self.validate(coverage)
        if input_binding(self.store,self.allowed)!=self.binding:raise ValueError('coverage checkpoint input changed')
        raw=canonical({'schema':'gapi-coverage/1','binding':self.binding,'coverage':coverage})
        if len(raw)>1024**2:raise BudgetStop('coverage checkpoint capacity exhausted')
        _write_aggregate(self.store.task,self.path,raw)


class InstitutionDirectoryCheckpoint:
    """Completed presentation directory, bound to the exact analysis input."""
    def __init__(self, store, allowed):
        self.store,self.allowed=store,allowed
        self.binding=input_binding(store,allowed)
        key=digest(None if allowed is None else sorted(allowed))[:16]
        self.path=store.task.root/('analysis-institution-directory-'+key+'.json.gz')

    def load(self, *, expected_binding=None):
        if not self.path.exists():return None
        if self.path.is_symlink() or self.path.is_junction():raise ValueError('linked directory checkpoint')
        with self.path.open('rb') as stream:body=stream.read(16*1024**2+1)
        if len(body)>16*1024**2:raise ValueError('directory checkpoint compressed bound')
        decoder=zlib.decompressobj(31);raw=decoder.decompress(body,64*1024**2+1)
        if len(raw)>64*1024**2 or not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
            raise ValueError('directory checkpoint stream bound')
        def unique(pairs):
            result={}
            for key,value in pairs:
                if key in result:raise ValueError('duplicate directory checkpoint key')
                result[key]=value
            return result
        value=json.loads(raw,object_pairs_hook=unique)
        if (not isinstance(value,dict) or set(value)!={'schema','binding','directory','ids'}
                or value['schema']!='gapi-institution-directory/1'
                or value['binding']!=(self.binding if expected_binding is None else expected_binding)):
            raise ValueError('directory checkpoint input or version drift')
        directory,ids=value['directory'],value['ids']
        if (not isinstance(directory,dict) or len(directory)>1000000
                or any(not isinstance(key,str) or not key or not isinstance(row,list) or len(row)!=3
                       or row[1] is not None and not isinstance(row[1],str)
                       or not isinstance(row[2],dict) for key,row in directory.items())
                or not isinstance(ids,list) or len(ids)>1000000
                or any(not isinstance(key,str) or not key for key in ids)
                or ids!=sorted(set(ids))):
            raise ValueError('invalid directory checkpoint rows')
        self.store.task.check_memory()
        return directory,set(ids)

    def save(self, directory, ids):
        from .global_analysis import _write_aggregate
        if input_binding(self.store,self.allowed)!=self.binding:raise ValueError('directory checkpoint input changed')
        value={'schema':'gapi-institution-directory/1','binding':self.binding,'directory':directory,'ids':sorted(ids)}
        encoder=json.JSONEncoder(ensure_ascii=True,sort_keys=True,separators=(',',':'),allow_nan=False)
        buffer=io.BytesIO();size=0;pending=bytearray()
        with gzip.GzipFile(fileobj=buffer,mode='wb',mtime=0,filename='') as stream:
            for token in encoder.iterencode(value):
                raw=token.encode('ascii');size+=len(raw)
                if size>64*1024**2:raise BudgetStop('directory checkpoint decoded capacity exhausted')
                pending.extend(raw)
                if len(pending)>=65536:
                    stream.write(pending);pending.clear();self.store.task.check_memory()
                if buffer.tell()>16*1024**2:raise BudgetStop('directory checkpoint compressed capacity exhausted')
            if pending:stream.write(pending)
        body=buffer.getvalue()
        if len(body)>16*1024**2:raise BudgetStop('directory checkpoint compressed capacity exhausted')
        _write_aggregate(self.store.task,self.path,body)

    def migrate_completed(self, predecessor_code, *, original_sha256):
        """Explicit code-generation move; retain the exact completed directory."""
        if self.path.is_symlink() or self.path.is_junction():raise ValueError('linked directory checkpoint')
        with self.path.open('rb') as stream:body=stream.read(16*1024**2+1)
        if len(body)>16*1024**2:raise ValueError('directory checkpoint compressed bound')
        if hashlib.sha256(body).hexdigest()!=original_sha256:
            raise ValueError('directory predecessor identity mismatch')
        old=input_binding(self.store,self.allowed,predecessor_code=predecessor_code)
        directory,ids=self.load(expected_binding=old)
        self.save(directory,ids)
        actual,actual_ids=self.load()
        if actual!=directory or actual_ids!=ids:raise ValueError('directory migration content mismatch')
        return {'predecessor_binding':old,'current_binding':self.binding,
                'original_sha256':original_sha256,
                'output_sha256':hashlib.sha256(self.path.read_bytes()).hexdigest(),
                'directory_records':len(directory),'directory_endpoints':len(ids)}


class TrainingDenominatorCheckpoint:
    """Completed exact counts; caller owns the existing writer exclusion.

    Source selection and the full input/code generation share GraphCheckpoint's
    binding contract. Legacy import is explicit, digest-bound and preserves its
    predecessor. Invalid checkpoints never silently trigger recomputation.
    """
    def __init__(self, store, allowed):
        self.store,self.allowed=store,allowed
        self.binding=input_binding(store,allowed)
        key=digest(None if allowed is None else sorted(allowed))[:16]
        self.path=store.task.root/('analysis-training-denominators-'+key+'.json.gz')

    def _read(self, path, expected_binding):
        if path.is_symlink() or path.is_junction():raise ValueError('linked denominator checkpoint')
        with path.open('rb') as stream:body=stream.read(4*1024**2+1)
        if len(body)>4*1024**2:raise ValueError('denominator checkpoint compressed bound')
        decoder=zlib.decompressobj(31)
        raw=decoder.decompress(body,16*1024**2+1)
        if (len(raw)>16*1024**2 or not decoder.eof or decoder.unused_data or decoder.unconsumed_tail):
            raise ValueError('denominator checkpoint stream bound')
        def unique(pairs):
            result={}
            for key,value in pairs:
                if key in result:raise ValueError('duplicate denominator checkpoint key')
                result[key]=value
            return result
        value=json.loads(raw,object_pairs_hook=unique)
        if (not isinstance(value,dict) or set(value)!={'schema','binding','counts','source'}
                or value['schema']!='gapi-training-denominators/1'
                or value['binding']!=expected_binding):
            raise ValueError('denominator checkpoint input or version drift')
        counts=value['counts']
        if (not isinstance(counts,dict) or len(counts)>1000000
                or any(not isinstance(key,str) or not key or type(count) is not int or count<=0
                       for key,count in counts.items())):
            raise ValueError('invalid denominator checkpoint counts')
        self.store.task.check_memory()
        return Counter(counts)

    def load(self):
        return self._read(self.path,self.binding) if self.path.exists() else None

    def save(self, counts):
        from .task import canonical
        from .global_analysis import _write_aggregate
        if input_binding(self.store,self.allowed)!=self.binding:
            raise ValueError('denominator checkpoint input changed')
        value={'schema':'gapi-training-denominators/1','binding':self.binding,
               'counts':dict(counts),'source':'exact training/member/archive predicates; completed calculation'}
        raw=canonical(value)
        if len(raw)>16*1024**2:raise BudgetStop('denominator checkpoint decoded capacity exhausted')
        body=gzip.compress(raw,mtime=0)
        if len(body)>4*1024**2:raise BudgetStop('denominator checkpoint compressed capacity exhausted')
        _write_aggregate(self.store.task,self.path,body)
        if self.load()!=counts:raise ValueError('denominator checkpoint readback mismatch')

    def import_completed(self, predecessor_code, *, original_sha256, predecessor_receipt=None):
        source=self.store.task.root/'analysis-training-denominators.json.gz'
        if source.is_symlink() or source.is_junction():raise ValueError('linked denominator checkpoint')
        with source.open('rb') as stream:body=stream.read(4*1024**2+1)
        if hashlib.sha256(body).hexdigest()!=original_sha256:
            raise ValueError('denominator predecessor identity mismatch')
        old_binding=input_binding(self.store,self.allowed,predecessor_code=predecessor_code)
        counts=self._read(source,old_binding)
        if self.path.exists():
            try:existing=self.load()
            except ValueError as exc:
                if str(exc)!='denominator checkpoint input or version drift':raise
                if (not isinstance(predecessor_receipt,dict)
                        or predecessor_receipt.get('predecessor_binding')!=old_binding
                        or predecessor_receipt.get('original_sha256')!=original_sha256
                        or hashlib.sha256(self.path.read_bytes()).hexdigest()!=predecessor_receipt.get('output_sha256')):
                    raise ValueError('denominator predecessor receipt mismatch') from None
                existing=self._read(self.path,predecessor_receipt.get('current_binding'))
                if existing!=counts:raise ValueError('denominator predecessor counts mismatch')
                self.save(counts)
            if existing!=counts:raise ValueError('denominator predecessor counts mismatch')
        else:self.save(counts)
        return {'predecessor_binding':old_binding,'current_binding':self.binding,
                'original_sha256':original_sha256,'institutions':len(counts),
                'output_sha256':hashlib.sha256(self.path.read_bytes()).hexdigest()}


class GraphCheckpoint:
    def __init__(self, store, allowed):
        self.store=store
        self.allowed=allowed
        self.binding=input_binding(store,allowed)
        # Public/local source selections cannot consume each other's partials.
        key=digest(None if allowed is None else sorted(allowed))[:16]
        self.path=store.task.root/('analysis-graph-'+key+'.checkpoint')
        self.after_person=None
        self.complete=False
        self.last_save=time.monotonic()

    def load(self):
        if not self.path.exists():return None
        if self.path.is_symlink() or self.path.is_junction():
            raise ValueError('linked graph checkpoint')
        compact=self.path.with_suffix('.compact')
        if compact.exists():
            return self._load_compact(compact)
        if self.path.is_symlink() or self.path.is_junction():
            raise ValueError('linked graph checkpoint')
        with self.path.open('rb') as stream:body=stream.read(MAX_PACKED+1)
        prefix=len(MAGIC)+8+32
        if not body.startswith(MAGIC) or not prefix<len(body)<=MAX_PACKED:
            raise ValueError('invalid graph checkpoint framing')
        size=int.from_bytes(body[len(MAGIC):len(MAGIC)+8],'big')
        expected=body[len(MAGIC)+8:prefix]
        if not 0<size<=MAX_DECODED:raise ValueError('graph checkpoint decoded bound')
        self.store.task.check_memory()
        with gzip.GzipFile(fileobj=io.BytesIO(body[prefix:])) as stream:
            raw=stream.read(size+1)
        if len(raw)!=size or hashlib.sha256(raw).digest()!=expected:
            raise ValueError('graph checkpoint content mismatch')
        value=json.loads(raw)
        del raw,body
        if (value.get('schema')!='gapi-analysis-graph/1'
                or value.get('binding')!=self.binding
                or type(value.get('complete')) is not bool
                or value.get('after_person') is not None and not isinstance(value['after_person'],str)):
            raise ValueError('graph checkpoint input or version drift')
        state=value['state']
        if (set(state)!={'edges','stages','counts','source_counts','corroborated',
                         'independent_origins','recent_edges','relation_count'}
                or type(state['relation_count']) is not int or state['relation_count']<0):
            raise ValueError('invalid graph checkpoint state')
        self.after_person=value['after_person'];self.complete=value['complete']
        self.store.task.check_memory()
        return state

    def _load_compact(self, path, *, expected_binding=None):
        if path.is_symlink() or path.is_junction():
            raise ValueError('linked compact graph checkpoint')
        with path.open('rb') as stream:
            body=stream.read(MAX_PACKED+1)
        if len(body)>MAX_PACKED:
            raise ValueError('compact graph checkpoint compressed bound')
        if len(body)<72 or body[:8]!=COMPACT_MAGIC:
            raise ValueError('invalid compact graph checkpoint framing')
        _,nodes_n,edges_n,stages_n,recent_n,meta_n,version=struct.unpack('<8sIIIIII',body[:32])
        if version!=1 or meta_n<=0:
            raise ValueError('invalid compact graph checkpoint bounds')
        size=int.from_bytes(body[32:40],'big'); expected=body[40:72]
        if not 0<size<=MAX_DECODED:
            raise ValueError('compact graph checkpoint decoded bound')
        self.store.task.check_memory()
        decoder=zlib.decompressobj()
        raw=decoder.decompress(body[72:],size+1)
        if not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
            raise ValueError('compact graph checkpoint stream boundary')
        if len(raw)!=size or hashlib.sha256(raw).digest()!=expected:
            raise ValueError('compact graph checkpoint content mismatch')
        edge_bytes=edges_n*16; stage_bytes=stages_n*40; recent_bytes=recent_n*16
        if edge_bytes+stage_bytes+recent_bytes+meta_n!=len(raw):
            raise ValueError('compact graph checkpoint length mismatch')
        pos=0
        view=memoryview(raw)
        edges=PackedRows(view[pos:pos+edge_bytes],'<IId'); pos+=edge_bytes
        stages=PackedRows(view[pos:pos+stage_bytes],'<II4d'); pos+=stage_bytes
        recent=PackedRows(view[pos:pos+recent_bytes],'<IId'); pos+=recent_bytes
        meta=json.loads(raw[pos:])
        binding=meta.get('binding')
        if binding!=(self.binding if expected_binding is None else expected_binding):
            raise ValueError('compact graph checkpoint input or version drift')
        nodes=meta.get('nodes')
        if (not isinstance(nodes,list) or len(nodes)!=nodes_n
                or any(not isinstance(x,str) for x in nodes) or len(set(nodes))!=nodes_n):
            raise ValueError('invalid compact graph checkpoint nodes')
        for rows in (edges,stages,recent):
            seen=set()
            for row in rows:
                key=row[:2]
                if (max(key)>=nodes_n or key in seen
                        or any(not math.isfinite(w) or w<0 for w in row[2:])):
                    raise ValueError('invalid compact graph checkpoint row')
                seen.add(key)
        # Each accumulated main edge has exactly one stage breakdown. Recent
        # edges are a subset of that graph, never an independent universe.
        edge_keys={(a<<32)|b for a,b,_ in edges}
        if len(stages)!=len(edges) or any(((row[0]<<32)|row[1]) not in edge_keys for row in stages):
            raise ValueError('compact graph stage endpoints mismatch')
        if any(((a<<32)|b) not in edge_keys for a,b,_ in recent):
            raise ValueError('compact graph recent endpoints mismatch')
        del seen,edge_keys
        if type(meta.get('relation_count')) is not int or meta['relation_count']<0:
            raise ValueError('invalid compact graph checkpoint count')
        self.after_person=meta.get('after_person'); self.complete=meta.get('complete') is True
        if not self.complete or (self.after_person is not None and not isinstance(self.after_person,str)):
            raise ValueError('compact graph checkpoint is not a completed first pass')
        self.store.task.check_memory()
        return {'edges':PackedMap(nodes,edges,'edge'),'stages':PackedMap(nodes,stages,'stage'),
                'counts':meta['counts'],'source_counts':meta['source_counts'],
                'corroborated':meta['corroborated'],'independent_origins':meta['independent_origins'],
                'recent_edges':PackedMap(nodes,recent,'edge'),'relation_count':meta['relation_count']}

    def write_compact(self, state):
        """Convert a validated completed state under the caller's writer lock.

        This does not migrate input identities. The original checkpoint stays
        authoritative and retained; a failed write preserves the prior compact.
        """
        if not self.complete or input_binding(self.store,self.allowed)!=self.binding:
            raise ValueError('compact conversion requires completed matching input')
        self.store.task.check_memory()
        nodes=[];index={}
        def edge_items(key):
            value=state[key]
            return value.items() if isinstance(value,PackedMap) else iter(value)
        for key in ('edges','stages','recent_edges'):
            for endpoints,_ in edge_items(key):
                for node in endpoints:
                    if node not in index:index[node]=len(nodes);nodes.append(node)
        chunks=bytearray();sizes=[]
        for key,format in (('edges','<IId'),('stages','<II4d'),('recent_edges','<IId')):
            count=0
            for (a,b),value in edge_items(key):
                weights=([value.get(k,0.0) for k in ('bachelor','master','doctorate','postdoc')]
                         if key=='stages' else [value])
                if key=='stages' and set(value)-{'bachelor','master','doctorate','postdoc'}:
                    raise ValueError('unsupported compact graph stage')
                if any(not math.isfinite(w) or w<0 for w in weights):
                    raise ValueError('invalid compact graph weight')
                chunks.extend(struct.pack(format,index[a],index[b],*weights));count+=1
                if count%1000==0:self.store.task.check_memory()
                if len(chunks)>MAX_DECODED:raise BudgetStop('compact graph decoded capacity exhausted')
            sizes.append(count)
        meta={key:state[key] for key in ('counts','source_counts','corroborated',
                                       'independent_origins','relation_count')}
        meta.update(nodes=nodes,binding=self.binding,complete=True,after_person=self.after_person)
        from .task import canonical
        metadata=canonical(meta);chunks.extend(metadata)
        if len(chunks)>MAX_DECODED:raise BudgetStop('compact graph decoded capacity exhausted')
        header=struct.pack('<8sIIIIII',COMPACT_MAGIC,len(nodes),*sizes,len(metadata),1)
        content=header+len(chunks).to_bytes(8,'big')+hashlib.sha256(chunks).digest()+zlib.compress(chunks,6)
        if len(content)>MAX_PACKED:raise BudgetStop('compact graph compressed capacity exhausted')
        self.store.task.check_memory()
        from .global_analysis import _write_aggregate
        target=self.path.with_suffix('.compact')
        _write_aggregate(self.store.task,target,content)
        return target

    def migrate_compact(self, predecessor_code, *, original_sha256, predecessor_receipt=None):
        """Explicit representation migration; caller holds existing writer lock.

        The caller supplies reviewed predecessor code hashes. Database,
        configuration and source selection remain those of the current store.
        Full original/compact logical equivalence is checked before writing.
        """
        old_binding=input_binding(self.store,self.allowed,predecessor_code=predecessor_code)
        if self.path.is_symlink() or self.path.is_junction():
            raise ValueError('linked graph checkpoint')
        file_hash=hashlib.sha256();logical=hashlib.sha256();size=0
        with self.path.open('rb') as stream:
            while block:=stream.read(65536):file_hash.update(block)
            if file_hash.hexdigest()!=original_sha256:
                raise ValueError('original checkpoint identity mismatch')
            stream.seek(0);header=stream.read(len(MAGIC)+40)
            if not header.startswith(MAGIC):raise ValueError('invalid graph checkpoint framing')
            length=int.from_bytes(header[len(MAGIC):len(MAGIC)+8],'big')
            expected=header[len(MAGIC)+8:]
            if not 0<length<=MAX_DECODED:raise ValueError('graph checkpoint decoded bound')
            with gzip.GzipFile(fileobj=stream) as decoded:
                while block:=decoded.read(65536):
                    logical.update(block);size+=len(block)
                    if size>length:raise ValueError('graph checkpoint decoded bound')
                    self.store.task.check_memory()
        if size!=length or logical.digest()!=expected:
            raise ValueError('graph checkpoint content mismatch')
        if predecessor_receipt is not None:
            if (not isinstance(predecessor_receipt,dict)
                    or predecessor_receipt.get('predecessor_binding')!=old_binding
                    or predecessor_receipt.get('original_sha256')!=original_sha256
                    or predecessor_receipt.get('logical_sha256')!=expected.hex()
                    or any(not isinstance(predecessor_receipt.get(key),str)
                           or len(predecessor_receipt[key])!=64
                           or any(c not in '0123456789abcdef' for c in predecessor_receipt[key])
                           for key in ('current_binding','compact_sha256'))):
                raise ValueError('compact migration predecessor receipt mismatch')
        try:
            state=self._load_compact(self.path.with_suffix('.compact'),expected_binding=old_binding)
        except ValueError as exc:
            if str(exc)!='compact graph checkpoint input or version drift':raise
            # A retry may find this exact migration already committed. The
            # original logical comparison below is still mandatory.
            compact=self.path.with_suffix('.compact')
            if (predecessor_receipt is not None and
                    hashlib.sha256(compact.read_bytes()).hexdigest()==predecessor_receipt['compact_sha256']):
                state=self._load_compact(compact,expected_binding=predecessor_receipt['current_binding'])
            else:
                state=self._load_compact(compact)
        from .task import canonical
        reconstructed=hashlib.sha256();count=0
        def emit(value):
            nonlocal count
            reconstructed.update(value);count+=len(value)
        emit(b'{"after_person":'+canonical(self.after_person)+b',"binding":'+canonical(old_binding)
             +b',"complete":true,"schema":"gapi-analysis-graph/1","state":{')
        for number,key in enumerate(sorted(state)):
            if number:emit(b',')
            emit(canonical(key)+b':')
            value=state[key]
            if isinstance(value,PackedMap):
                emit(b'[')
                for j,(pair,weight) in enumerate(value.items()):
                    if j:emit(b',')
                    emit(canonical([pair,dict(weight) if value.kind=='stage' else weight]))
                    if j%1000==0:self.store.task.check_memory()
                emit(b']')
            else:emit(canonical(value))
        emit(b'}}')
        if count!=length or reconstructed.digest()!=expected:
            raise ValueError('compact predecessor logical mismatch')
        if input_binding(self.store,self.allowed)!=self.binding:
            raise ValueError('migration input changed')
        target=self.write_compact(state)
        return {'predecessor_binding':old_binding,'current_binding':self.binding,
                'original_sha256':original_sha256,'logical_sha256':expected.hex(),
                'compact_sha256':hashlib.sha256(target.read_bytes()).hexdigest(),
                'relation_count':state['relation_count']}

    def boundary(self, person, state):
        if self.after_person is not None and person<=self.after_person:
            raise ValueError('graph checkpoint person cursor moved backwards')
        self.after_person=person
        if time.monotonic()-self.last_save>=CHECKPOINT_SECONDS:
            self.save(state,complete=False)

    def finish(self, state):
        if not self.complete:self.save(state,complete=True)

    def save(self, state, *, complete):
        if input_binding(self.store,self.allowed)!=self.binding:
            raise ValueError('graph checkpoint input changed during calculation')
        self.store.task.check_memory()
        value={'schema':'gapi-analysis-graph/1','binding':self.binding,
               'after_person':self.after_person,'complete':complete,'state':state()}
        self.store.task.check_memory()
        buffer=io.BytesIO();logical=hashlib.sha256();size=0
        pending=bytearray()
        encoder=json.JSONEncoder(ensure_ascii=True,sort_keys=True,separators=(',',':'),allow_nan=False)
        with gzip.GzipFile(fileobj=buffer,filename='',mode='wb',mtime=0) as stream:
            for token in encoder.iterencode(value):
                raw=token.encode('ascii');size+=len(raw)
                if size>MAX_DECODED:raise BudgetStop('graph checkpoint decoded capacity exhausted')
                pending.extend(raw)
                if len(pending)>=65536:
                    logical.update(pending);stream.write(pending);pending.clear()
                if buffer.tell()>MAX_PACKED-len(MAGIC)-40:
                    raise BudgetStop('graph checkpoint compressed capacity exhausted')
            if pending:logical.update(pending);stream.write(pending)
        del value
        content=MAGIC+size.to_bytes(8,'big')+logical.digest()+buffer.getvalue()
        if len(content)>MAX_PACKED:raise BudgetStop('graph checkpoint compressed capacity exhausted')
        self.store.task.check_memory()
        from .global_analysis import _write_aggregate
        _write_aggregate(self.store.task,self.path,content)
        self.complete=complete;self.last_save=time.monotonic()
