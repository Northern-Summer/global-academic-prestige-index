"""Stand-alone public aggregate replay; stdlib only, no gapi imports or person DB.

This checks graph-to-score reproduction. It cannot confirm the truth of private
career inputs, source independence, licensing or complete world coverage.
"""
import argparse
import hashlib
import gzip
import io
import json
import math
from pathlib import Path
import re
import os
from array import array


def original_gzip(block, entry):
    """Reconstruct exact frozen bytes across Python gzip OS-header variants.

    Kept independent of the production reader. Every candidate still has to
    match the original complete compressed SHA-256 and length; deflate, CRC,
    timestamp and input differences are never accepted.
    """
    body=gzip.compress(block,compresslevel=6,mtime=0)
    if len(body)==entry['bytes']:
        for os_byte in dict.fromkeys((body[9],3,10,255)):
            candidate=body[:9]+bytes([os_byte])+body[10:]
            if hashlib.sha256(candidate).hexdigest()==entry['sha256']:return candidate
    raise ValueError('partition cannot reconstruct original compressed source bytes')


def bounded(path, maximum=32*1024**2):
    with path.open('rb') as stream:body=stream.read(maximum+1)
    if len(body)>maximum:raise ValueError('public input exceeds byte cap')
    return body


def expand_transport(value):
    """Independent reader for docs/DATA_TRANSPORT.md; no canonical gapi imports."""
    if not isinstance(value,dict) or value.get('schema_version')!='gapi-institution-defaults/1':return value
    if set(value)!={'schema_version','payload','node_defaults','node_rows'}:raise ValueError('compact envelope')
    body,defaults,rows=value['payload'],value['node_defaults'],value['node_rows']
    forbidden={'__proto__','constructor','prototype'}
    def fields(v):return isinstance(v,dict) and len(v)<=64 and all(isinstance(k,str) and 0<len(k)<=64 and k not in forbidden for k in v)
    if (not isinstance(body,dict) or body.get('schema_version')!='gapi-global-observed/v1'
            or 'nodes' in body or forbidden & body.keys() or not fields(defaults)
            or {'institution_id','name'} & defaults.keys()
            or len(json.dumps(defaults,ensure_ascii=True,sort_keys=True,separators=(',',':')).encode())>65536
            or not isinstance(rows,list) or len(rows)>131072):raise ValueError('compact structure')
    restored=[]
    for row in rows:
        if not fields(row) or not {'institution_id','name'}<=row.keys() or len(defaults.keys()|row.keys())>64:
            raise ValueError('compact row')
        restored.append({**defaults,**row})
    return {**body,'nodes':restored}


def replay(data, *, edge_limit=200000):
    # Public entry points retain their existing default. Larger frozen local
    # graphs can opt into a bounded capacity without Python tuple duplication.
    if type(edge_limit) is not int or not 1<=edge_limit<=1000000:
        raise ValueError('invalid independent graph edge limit')
    rows=data['nodes']; nodes=sorted(row['institution_id'] for row in rows if row['network_score'] is not None)
    if not nodes or len(nodes)>200000 or len(set(nodes))!=len(nodes):raise ValueError('invalid graph universe')
    if data['algorithm']['direction']!='employer -> training':raise ValueError('wrong declared edge direction')
    if sorted(data['algorithm']['network_universe'])!=nodes:raise ValueError('universe mismatch')
    alpha=data['algorithm']['config']['alpha']
    if type(alpha) not in (float,int) or not math.isfinite(alpha) or not 0<=alpha<1:raise ValueError('invalid alpha')
    totals={node:0.0 for node in nodes};seen=set()
    index={node:i for i,node in enumerate(nodes)}
    sources,targets,weights=array('I'),array('I'),array('d')
    if len(data['edges'])>edge_limit:raise ValueError('edge cap')
    for edge in data['edges']:
        source,target,weight=edge['source'],edge['target'],edge['weight']
        if source not in totals or target not in totals or (source,target) in seen:raise ValueError('invalid edge endpoints/duplicate')
        if type(weight) not in (float,int) or not math.isfinite(weight) or weight<=0:raise ValueError('invalid edge weight')
        seen.add((source,target));totals[source]+=weight
        sources.append(index[source]);targets.append(index[target]);weights.append(weight)
    if not all(math.isfinite(value) for value in totals.values()):raise ValueError('row sum overflow')
    del seen,index
    n=len(nodes);scores=[1/n]*n;residual=None
    outgoing=[totals[node] for node in nodes]
    for iteration in range(1,201):
        dangling=math.fsum(scores[i] for i in range(n) if not outgoing[i])
        new=[((1-alpha)+alpha*dangling)/n]*n
        for source,target,weight in zip(sources,targets,weights):
            new[target]+=alpha*scores[source]*weight/outgoing[source]
        residual=math.fsum(abs(new[i]-scores[i]) for i in range(n));scores=new
        if residual<1e-12:break
    if residual>=1e-12:raise ValueError('independent replay did not converge')
    stored={row['institution_id']:row['network_score'] for row in rows if row['network_score'] is not None}
    if any(type(value) not in (float,int) or not math.isfinite(value) or not 0<=value<=1 for value in stored.values()):
        raise ValueError('invalid stored score')
    error=max(abs(scores[i]-stored[node]) for i,node in enumerate(nodes))
    mass=math.fsum(scores)
    if error>1e-10 or abs(mass-1)>1e-10:raise ValueError('graph/score mismatch or mass loss')
    ranks={node:i for i,node in enumerate(sorted(nodes,key=lambda node:(-stored[node],node)),1)}
    if any(row['observed_rank']!=ranks[row['institution_id']] for row in rows if row['institution_id'] in ranks):
        raise ValueError('stored ordering mismatch')
    return {'scored_nodes':n,'edges':len(weights),'iterations':iteration,'l1_residual':residual,
            'max_score_error':error,'probability_mass':mass,'ordering_consistent':True}


def verify(manifest_path):
    manifest_bytes=bounded(manifest_path,1024**2);manifest=json.loads(manifest_bytes)
    if manifest.get('schema_version')=='gapi-aggregate-partitions/1':return verify_public_partitions(manifest_path)
    name=manifest.get('snapshot')
    if not isinstance(name,str) or not re.fullmatch(r'global-[a-f0-9]{16}\.json',name):raise ValueError('unsafe snapshot locator')
    storage=manifest.get('repository_snapshot')
    if 'repository_snapshot' in manifest:
        if (not isinstance(storage,dict) or storage.get('encoding')!='gzip'
                or storage.get('filename')!=name+'.gz'
                or not isinstance(storage.get('sha256'),str) or not re.fullmatch(r'[a-f0-9]{64}',storage['sha256'])
                or any(type(storage.get(k)) is not int or not 0<storage[k]<=32*1024**2 for k in ('bytes','decoded_bytes'))):
            raise ValueError('invalid repository snapshot representation')
        packed=bounded(manifest_path.parent/storage['filename'])
        if len(packed)!=storage['bytes'] or hashlib.sha256(packed).hexdigest()!=storage['sha256']:
            raise ValueError('stored snapshot identity mismatch')
        with gzip.GzipFile(fileobj=io.BytesIO(packed)) as stream:body=stream.read(32*1024**2+1)
        if len(body)>32*1024**2 or len(body)!=storage['decoded_bytes']:raise ValueError('decoded snapshot size mismatch')
    else:body=bounded(manifest_path.parent/name)
    sha=hashlib.sha256(body).hexdigest()
    if sha!=manifest['output_sha256']:raise ValueError('snapshot hash mismatch')
    data=expand_transport(json.loads(body))
    if 'logical_output_sha256' in manifest:
        logical=json.dumps(data,ensure_ascii=True,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
        if hashlib.sha256(logical).hexdigest()!=manifest['logical_output_sha256']:raise ValueError('logical snapshot identity mismatch')
    result=replay(data)
    return {'schema_version':'gapi-public-independent-replay/v1','status':'passed',
        'snapshot_sha256':sha,'manifest_sha256':hashlib.sha256(manifest_bytes).hexdigest(),**result,
        'scope':'independently implemented public graph-to-score replay; no canonical gapi ranking imports',
        'not_verified':['underlying personal facts','source independence','world coverage','source rights','third-party auditor identity']}


def _successor_directory(directory, source_raw):
    link=directory/'successor.json'
    if link.is_symlink():raise ValueError('linked aggregate successor')
    value=json.loads(bounded(link,65536));name=value.get('directory')
    if (value.get('schema_version')!='gapi-aggregate-successor/1' or not isinstance(name,str)
            or not re.fullmatch('[A-Za-z0-9_-]+',name) or name==directory.name
            or value.get('source_manifest_sha256')!=hashlib.sha256(source_raw).hexdigest()):
        raise ValueError('invalid aggregate successor binding')
    target=directory.parent/name
    for path in (target,*target.parents):
        if path.is_symlink() or (path.exists() and path.is_junction()):raise ValueError('linked aggregate successor')
    if (target/'manifest.json').is_symlink():raise ValueError('linked successor manifest')
    if hashlib.sha256(bounded(target/'manifest.json',4*1024**2)).hexdigest()!=value.get('manifest_sha256'):
        raise ValueError('aggregate successor manifest drift')
    return target


def verify_local_chunks(directory):
    """Independent byte verification only; never grants publication or rank status.

    Protocol owner: gapi.aggregate_chunks, docs/DATA_TRANSPORT.md. This reader
    deliberately imports neither the canonical codec nor the ranking code.
    """
    directory=Path(directory).absolute()
    def regular(path):
        for item in (path,*path.parents):
            if item.is_symlink() or item.is_junction():raise ValueError('linked chunk path')
        if not path.is_file():raise ValueError('missing chunk file')
        return path
    manifest_bytes=bounded(regular(directory/'manifest.json'),4*1024**2)
    manifest=json.loads(manifest_bytes);entries=manifest.get('chunks')
    if (manifest.get('schema_version')!='gapi-local-aggregate-chunks/1'
            or manifest.get('logical_schema_version')!='gapi-global-observed/v1'
            or manifest.get('publication_status')!='local-only-source-license-review'
            or not isinstance(entries,list) or not 0<len(entries)<=10000
            or any(type(manifest.get(k)) is not int or manifest[k]<0 for k in ('nodes','edges'))
            or type(manifest.get('decoded_bytes')) is not int or manifest['decoded_bytes']<=0
            or not re.fullmatch('[a-f0-9]{64}',str(manifest.get('logical_sha256','')))):
        raise ValueError('invalid local chunk manifest')
    if (directory/'successor.json').exists():
        result=verify_local_partitions(_successor_directory(directory,manifest_bytes),directory)
        return {'schema_version':'gapi-independent-local-chunk-check/v1','status':'passed',
                'manifest_sha256':hashlib.sha256(manifest_bytes).hexdigest(),'logical_sha256':result['logical_sha256'],
                'chunks':len(entries),'decoded_bytes':manifest['decoded_bytes'],
                'compressed_bytes':sum(e['bytes'] for e in entries),
                'scope':'original transport identities reconstructed independently through selected lossless successor',
                'not_verified':['graph-to-score reproduction','publication eligibility','underlying personal facts','source independence','world coverage','source rights']}
    logical=hashlib.sha256();total=packed_total=0
    for entry in entries:
        if not isinstance(entry,dict):raise ValueError('invalid chunk descriptor')
        sha=entry.get('sha256');name=entry.get('file')
        if (not isinstance(sha,str) or not re.fullmatch('[a-f0-9]{64}',sha)
                or name!=sha+'.json.part.gz'
                or type(entry.get('bytes')) is not int or not 0<entry['bytes']<=1024**2+65536
                or type(entry.get('decoded_bytes')) is not int or not 0<entry['decoded_bytes']<=1024**2):
            raise ValueError('invalid chunk descriptor')
        body=bounded(regular(directory/name),1024**2+65536)
        if len(body)!=entry['bytes'] or hashlib.sha256(body).hexdigest()!=sha:
            raise ValueError('compressed chunk identity mismatch')
        with gzip.GzipFile(fileobj=io.BytesIO(body)) as stream:raw=stream.read(1024**2+1)
        if (len(raw)!=entry['decoded_bytes']
                or hashlib.sha256(raw).hexdigest()!=entry.get('decoded_sha256')):
            raise ValueError('decoded chunk identity mismatch')
        total+=len(raw);packed_total+=len(body);logical.update(raw)
        if total>manifest['decoded_bytes']:raise ValueError('decoded aggregate exceeds manifest')
    if total!=manifest['decoded_bytes'] or logical.hexdigest()!=manifest['logical_sha256']:
        raise ValueError('logical aggregate identity mismatch')
    return {'schema_version':'gapi-independent-local-chunk-check/v1','status':'passed',
        'manifest_sha256':hashlib.sha256(manifest_bytes).hexdigest(),
        'logical_sha256':logical.hexdigest(),'chunks':len(entries),
        'decoded_bytes':total,'compressed_bytes':packed_total,
        'scope':'independently verified local transport bytes; no whole JSON materialization',
        'not_verified':['JSON semantics','graph-to-score reproduction','publication eligibility',
                        'underlying personal facts','source independence','world coverage','source rights']}


class _JSONStream:
    """Bounded ASCII JSON reader for canonical local chunks; no gapi imports."""
    def __init__(self, pieces):
        self.pieces=iter(pieces);self.buffer='';self.position=0;self.retained_remaining=None

    def peek(self):
        while self.position==len(self.buffer):
            try:self.buffer=next(self.pieces).decode('ascii')
            except StopIteration:return ''
            self.position=0
        return self.buffer[self.position]

    def take(self):
        value=self.peek()
        if not value:raise ValueError('truncated aggregate JSON')
        if self.retained_remaining is not None:
            self.retained_remaining-=1
            if self.retained_remaining<0:raise ValueError('aggregate retained value bound')
        self.position+=1
        return value

    def space(self):
        while self.peek() and self.peek() in ' \t\r\n':self.take()

    def expect(self, value):
        self.space()
        if self.take()!=value:raise ValueError('invalid aggregate JSON delimiter')

    def value(self, *, keep=True, depth=0):
        if depth==0:
            self.retained_remaining=8*1024**2 if keep else None
            try:return self._value(keep=keep,depth=depth)
            finally:self.retained_remaining=None
        return self._value(keep=keep,depth=depth)

    def _value(self, *, keep=True, depth=0):
        if depth>64:raise ValueError('aggregate JSON nesting bound')
        self.space();first=self.peek()
        if first in ('{','['):
            self.take();object_value=first=='{';end='}' if object_value else ']'
            result=({} if object_value else []) if keep else None
            keys=set();self.space()
            if self.peek()==end:self.take();return result
            while True:
                if object_value:
                    key=self.value(depth=depth+1)
                    if not isinstance(key,str) or key in keys:raise ValueError('invalid or duplicate JSON key')
                    keys.add(key);self.expect(':')
                    if len(keys)>200000:raise ValueError('aggregate object field bound')
                item=self.value(keep=keep,depth=depth+1)
                if keep:
                    if object_value:result[key]=item
                    else:result.append(item)
                self.space();delimiter=self.take()
                if delimiter==end:return result
                if delimiter!=',':raise ValueError('invalid aggregate JSON delimiter')
        token=[]
        if first=='"':
            token.append(self.take());escaped=False
            while True:
                char=self.take();token.append(char)
                if len(token)>1024**2:raise ValueError('aggregate JSON token bound')
                if char=='"' and not escaped:break
                if char=='\\' and not escaped:escaped=True
                else:escaped=False
        else:
            while self.peek() and self.peek() not in ',]} \t\r\n':
                token.append(self.take())
                if len(token)>1024:raise ValueError('aggregate JSON scalar bound')
        def invalid_constant(value):raise ValueError('nonfinite JSON scalar')
        result=json.loads(''.join(token),parse_constant=invalid_constant)
        if isinstance(result,float) and not math.isfinite(result):raise ValueError('nonfinite JSON scalar')
        return result if keep else None


def replay_local_chunks(directory, *, edge_limit=600000):
    """Validate all JSON while retaining only the existing replay's projection."""
    directory=Path(directory)
    if (directory/'successor.json').exists():
        source_raw=bounded(directory/'manifest.json',4*1024**2)
        return verify_local_partitions(_successor_directory(directory,source_raw),directory,replay_graph=True,edge_limit=edge_limit)
    transport=verify_local_chunks(directory)
    directory=Path(directory)
    manifest_bytes=bounded(directory/'manifest.json',4*1024**2)
    if hashlib.sha256(manifest_bytes).hexdigest()!=transport['manifest_sha256']:
        raise ValueError('manifest changed before semantic replay')
    manifest=json.loads(manifest_bytes)
    def pieces():
        # Recheck each immutable chunk on this second pass; do not trust names.
        for entry in manifest['chunks']:
            body=bounded(directory/entry['file'],1024**2+65536)
            if hashlib.sha256(body).hexdigest()!=entry['sha256']:raise ValueError('chunk changed before semantic replay')
            raw=gzip.decompress(body)
            if len(raw)!=entry['decoded_bytes'] or hashlib.sha256(raw).hexdigest()!=entry['decoded_sha256']:
                raise ValueError('chunk changed before semantic replay')
            yield raw
    stream=_JSONStream(pieces());stream.expect('{');keys=set();data={};counts={}
    while True:
        key=stream.value()
        if not isinstance(key,str) or key in keys:raise ValueError('invalid or duplicate aggregate field')
        keys.add(key);stream.expect(':')
        if key in ('nodes','edges'):
            stream.expect('[');rows=[];count=0;stream.space()
            if stream.peek()!=']':
                while True:
                    row=stream.value();count+=1
                    if not isinstance(row,dict):raise ValueError('aggregate row must be object')
                    if key=='nodes':
                        if row['network_score'] is not None:
                            rows.append({k:row[k] for k in ('institution_id','network_score','observed_rank')})
                    else:rows.append({k:row[k] for k in ('source','target','weight')})
                    if len(rows)>(200000 if key=='nodes' else edge_limit):raise ValueError('semantic projection capacity bound')
                    stream.space()
                    if stream.peek()==']':break
                    stream.expect(',')
            stream.expect(']');data[key]=rows;counts[key]=count
        elif key in ('algorithm','schema_version','publication_status'):
            data[key]=stream.value()
        else:stream.value(keep=False)
        stream.space();delimiter=stream.take()
        if delimiter=='}':break
        if delimiter!=',':raise ValueError('invalid aggregate JSON delimiter')
    stream.space()
    if stream.peek():raise ValueError('trailing aggregate JSON data')
    if (data.get('schema_version')!='gapi-global-observed/v1'
            or data.get('publication_status')!='local-only-source-license-review'
            or counts!={k:manifest[k] for k in ('nodes','edges')}):
        raise ValueError('semantic aggregate manifest mismatch')
    result=replay(data,edge_limit=edge_limit)
    return {**transport,'schema_version':'gapi-independent-local-semantic-replay/v1',
            **result,'scope':'full JSON syntax and manifest row counts; independently replayed graph-to-score projection',
            'not_verified':['underlying personal facts','source independence','world coverage','source rights','publication eligibility']}


def verify_local_partitions(directory, source_directory, *, replay_graph=False, edge_limit=600000,
                            manifest_name='manifest.json',source_name='manifest.json',public_status=None,asset_reader=None):
    """Independent partition-to-accepted-source equivalence; no gapi imports."""
    directory=Path(directory).absolute()
    for path in (directory,*directory.parents,Path(source_directory).absolute(),*Path(source_directory).absolute().parents):
        if path.is_symlink() or (path.exists() and path.is_junction()):raise ValueError('linked partition directory')
    if manifest_name not in ('manifest.json','local-input-manifest.json') or source_name not in ('manifest.json','local-input-source-manifest.json'):
        raise ValueError('unsafe partition evidence filename')
    if public_status not in (None,'public-observations-pending-external-review'):raise ValueError('invalid public status transform')
    pointer=directory/manifest_name
    if pointer.is_symlink():raise ValueError('linked partition manifest')
    original=bounded(pointer,4*1024**2);m=json.loads(original)
    source_pointer=Path(source_directory)/source_name
    if source_pointer.is_symlink():raise ValueError('linked partition source manifest')
    source_raw=bounded(source_pointer,4*1024**2)
    upstream=json.loads(source_raw)
    if (upstream.get('schema_version')!='gapi-local-aggregate-chunks/1'
            or upstream.get('logical_schema_version')!='gapi-global-observed/v1'
            or upstream.get('publication_status')!='local-only-source-license-review'
            or any(type(upstream.get(k)) is not int or upstream[k]<0 for k in ('nodes','edges'))
            or not isinstance(upstream.get('chunks'),list) or not 0<len(upstream['chunks'])<=10000
            or type(upstream.get('decoded_bytes')) is not int or upstream['decoded_bytes']<=0
            or not re.fullmatch('[a-f0-9]{64}',str(upstream.get('logical_sha256','')))):
        raise ValueError('invalid partition source manifest')
    source={'manifest_sha256':hashlib.sha256(source_raw).hexdigest(),'logical_sha256':upstream['logical_sha256'],'decoded_bytes':upstream['decoded_bytes']}
    if (m.get('schema_version')!='gapi-aggregate-partitions/1'
            or m.get('logical_schema_version')!='gapi-global-observed/v1'
            or m.get('publication_status')!='local-only-source-license-review'
            or m.get('source_manifest_sha256')!=source['manifest_sha256']
            or m.get('logical_sha256')!=source['logical_sha256']
            or m.get('logical_bytes')!=source['decoded_bytes']
            or any(type(m.get(k)) is not int or m[k]!=upstream[k] for k in ('nodes','edges'))
            or not isinstance(m.get('fields'),list) or len(m['fields'])>64):
        raise ValueError('partition source contract mismatch')
    sha=hashlib.sha256();public_sha=hashlib.sha256();public_size=0;size=assets=packed=0;counts={};universe=None;pending=bytearray();legacy_index=0;projection={'nodes':[],'edges':[]}
    def encode(value):
        return json.dumps(value,sort_keys=True,ensure_ascii=True,separators=(',',':'),allow_nan=False).encode('ascii')
    def emit(value,public_value=None):
        nonlocal size,legacy_index,public_size
        sha.update(value);size+=len(value)
        transformed=value if public_value is None else public_value
        public_sha.update(transformed);public_size+=len(transformed)
        pending.extend(value)
        while legacy_index<len(upstream['chunks']):
            entry=upstream['chunks'][legacy_index]
            if (not isinstance(entry,dict) or type(entry.get('decoded_bytes')) is not int or not 0<entry['decoded_bytes']<=1024**2
                    or type(entry.get('bytes')) is not int or not 0<entry['bytes']<=1024**2+65536
                    or not re.fullmatch('[a-f0-9]{64}',str(entry.get('sha256','')))
                    or entry.get('file')!=entry['sha256']+'.json.part.gz'):raise ValueError('invalid reconstructed source descriptor')
            if len(pending)<entry['decoded_bytes']:break
            block=bytes(pending[:entry['decoded_bytes']]);del pending[:entry['decoded_bytes']]
            if hashlib.sha256(block).hexdigest()!=entry.get('decoded_sha256'):
                raise ValueError('partition cannot reconstruct original compressed source bytes')
            original_gzip(block,entry)
            legacy_index+=1
    def load(entry,maximum):
        nonlocal assets,packed
        if (not isinstance(entry,dict) or not re.fullmatch('[a-f0-9]{64}',str(entry.get('sha256','')))
                or entry.get('file')!=entry['sha256']+'.json.gz' or entry.get('encoding')!='gzip'
                or type(entry.get('bytes')) is not int or not 0<entry['bytes']<=maximum
                or type(entry.get('stored_bytes')) is not int or not 0<entry['stored_bytes']<=maximum+65536):
            raise ValueError('invalid partition asset descriptor')
        path=directory/entry['file']
        if not path.exists() and os.environ.get('GAPI_PARTITION_ASSETS'):
            path=Path(os.environ['GAPI_PARTITION_ASSETS'])/entry['file']
            for ancestor in (path,*path.absolute().parents):
                if ancestor.is_symlink() or (ancestor.exists() and ancestor.is_junction()):raise ValueError('linked external partition')
        if path.is_symlink():raise ValueError('linked partition asset')
        body=bounded(path,maximum+65536) if asset_reader is None else asset_reader(path)
        if not isinstance(body,bytes) or len(body)>maximum+65536:raise ValueError('partition packed byte bound')
        if len(body)!=entry['stored_bytes'] or hashlib.sha256(body).hexdigest()!=entry.get('stored_sha256'):
            raise ValueError('partition packed identity mismatch')
        with gzip.GzipFile(fileobj=io.BytesIO(body)) as stream:raw=stream.read(maximum+1)
        if len(raw)!=entry['bytes'] or hashlib.sha256(raw).hexdigest()!=entry['sha256']:
            raise ValueError('partition decoded identity mismatch')
        assets+=1;packed+=len(body)
        if assets>10000:raise ValueError('partition asset count bound')
        return json.loads(raw)
    emit(b'{');previous=None
    for field in m['fields']:
        key=field.get('field')
        if not isinstance(key,str) or (previous is not None and key<=previous):raise ValueError('partition field order')
        if previous is not None:emit(b',')
        previous=key;emit(encode(key)+b':')
        if field.get('kind')=='rows' and key in ('nodes','edges'):
            emit(b'[');count=0
            if not isinstance(field.get('assets'),list):raise ValueError('invalid partition rows')
            for entry in field['assets']:
                v=load(entry,1024**2);columns=v.get('columns');rows=v.get('rows');references=v.get('references',{})
                if (v.get('schema_version')!='gapi-aggregate-row-columns/1' or not isinstance(columns,list) or not columns
                        or any(not isinstance(k,str) for k in columns) or columns!=sorted(set(columns)) or len(columns)>64
                        or not isinstance(rows,list) or type(entry.get('rows')) is not int or entry['rows']!=len(rows)):
                    raise ValueError('invalid partition row columns/count')
                if references and (key!='edges' or universe is None or references!=dict(source='algorithm.network_universe',target='algorithm.network_universe')
                                   or not set(references)<=set(columns)):raise ValueError('invalid partition references')
                for row in rows:
                    if not isinstance(row,list) or len(row)!=len(columns):raise ValueError('partition row width')
                    restored=dict(zip(columns,row))
                    for name in references:
                        index=restored[name]
                        if type(index) is not int or not 0<=index<len(universe):raise ValueError('partition reference index')
                        restored[name]=universe[index]
                    if replay_graph:
                        if key=='edges':projection['edges'].append({k:restored[k] for k in ('source','target','weight')})
                        elif restored['network_score'] is not None:projection['nodes'].append({k:restored[k] for k in ('institution_id','network_score','observed_rank')})
                        if len(projection['nodes'])>200000 or len(projection['edges'])>edge_limit:raise ValueError('partition replay projection bound')
                    if count:emit(b',')
                    emit(encode(restored));count+=1
            emit(b']');counts[key]=count
            if type(field.get('rows')) is not int or field['rows']!=count:raise ValueError('partition row count')
        elif field.get('kind')=='object' and key=='sensitivity':
            if not isinstance(field.get('entries'),list) or len(field['entries'])>256:raise ValueError('invalid scenario partitions')
            emit(b'{');last=None
            for item in field['entries']:
                name=item.get('key')
                if not isinstance(name,str) or (last is not None and name<=last):raise ValueError('scenario partition order')
                if last is not None:emit(b',')
                last=name;emit(encode(name)+b':'+encode(load(item['asset'],32*1024**2)))
            emit(b'}')
        elif field.get('kind')=='value' and key not in ('nodes','edges','sensitivity'):
            value=load(field['asset'],32*1024**2)
            if replay_graph and key=='algorithm':projection['algorithm']=value
            if key=='algorithm' and isinstance(value,dict) and 'network_universe' in value:
                universe=value['network_universe']
                if (not isinstance(universe,list) or len(universe)>200000 or any(not isinstance(v,str) for v in universe)
                        or len(set(universe))!=len(universe)):raise ValueError('invalid partition universe')
            emit(encode(value),encode(public_status) if public_status is not None and key=='publication_status' else None)
        else:raise ValueError('invalid partition field kind')
    emit(b'}')
    if (sha.hexdigest()!=source['logical_sha256'] or size!=source['decoded_bytes']
            or counts!={k:m[k] for k in ('nodes','edges')} or assets!=m.get('asset_count') or packed!=m.get('stored_bytes')
            or pending or legacy_index!=len(upstream['chunks'])
            or pointer.read_bytes()!=original or source_pointer.read_bytes()!=source_raw):raise ValueError('partition reconstructed identity mismatch')
    result={'schema_version':'gapi-independent-partition-equivalence/v1','status':'passed',
            'manifest_sha256':hashlib.sha256(original).hexdigest(),'source_manifest_sha256':source['manifest_sha256'],
            'logical_sha256':sha.hexdigest(),'logical_bytes':size,'nodes':counts['nodes'],'edges':counts['edges'],
            'assets':assets,'compressed_bytes':packed,
            'original_compressed_chunks_reconstructed':legacy_index,
            'scope':'independent exact canonical partition-to-original-source equivalence; no canonical codec imports',
            'not_verified':['publication eligibility','personal facts','source independence','world coverage']}
    if replay_graph:
        result.update(replay(projection,edge_limit=edge_limit))
        result['scope']='independent exact original transport reconstruction and graph-to-score replay through selected successor'
    if public_status is not None:result.update(public_logical_sha256=public_sha.hexdigest(),public_logical_bytes=public_size)
    return result


def verify_public_partitions(manifest_path, *, asset_reader=None):
    """Verify the policy-bound status-only publication transform and reproduce
    the full graph through the pinned local predecessor, without gapi imports.
    """
    raw=bounded(manifest_path,1024**2);m=json.loads(raw);p=m.get('publication',{})
    if (m.get('publication_status')!='public-observations-pending-external-review'
            or p.get('schema_version')!='gapi-public-partition-qualification/1'
            or p.get('qualification')!=m['publication_status']
            or p.get('purpose')!='noncommercial-research' or p.get('export_scope')!='institution-aggregates-only'
            or p.get('formal_ranking_qualified') is not False or p.get('independent_personal_facts_verified') is not False
            or p.get('local_manifest')!='local-input-manifest.json' or p.get('local_source_manifest')!='local-input-source-manifest.json'):
        raise ValueError('invalid public partition qualification')
    encode=lambda v:json.dumps(v,ensure_ascii=True,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
    local_raw=bounded(manifest_path.parent/p['local_manifest'],4*1024**2);local=json.loads(local_raw)
    if hashlib.sha256(local_raw).hexdigest()!=p.get('local_manifest_sha256') or hashlib.sha256(encode(p.get('publication_policy'))).hexdigest()!=p.get('publication_policy_sha256'):
        raise ValueError('public policy/predecessor identity mismatch')
    old=next(f for f in local['fields'] if f['field']=='publication_status')
    new=next(f for f in m['fields'] if f['field']=='publication_status');a=new.get('asset',{})
    if a.get('file')!=a.get('sha256','')+'.json.gz' or not re.fullmatch(r'[a-f0-9]{64}\.json\.gz',a.get('file','')) or a.get('encoding')!='gzip':raise ValueError('unsafe public status asset')
    body=bounded(manifest_path.parent/a['file'],65536) if asset_reader is None else asset_reader(manifest_path.parent/a['file'])
    if not isinstance(body,bytes) or len(body)>65536:raise ValueError('public status packed byte bound')
    if len(body)!=a.get('stored_bytes') or hashlib.sha256(body).hexdigest()!=a.get('stored_sha256'):raise ValueError('public status stored identity')
    with gzip.GzipFile(fileobj=io.BytesIO(body)) as stream:status=stream.read(1025)
    if (status!=encode(m['publication_status']) or len(status)!=a.get('bytes') or hashlib.sha256(status).hexdigest()!=a.get('sha256')):raise ValueError('public status decoded identity')
    result=verify_local_partitions(manifest_path.parent,manifest_path.parent,replay_graph=True,
        manifest_name=p['local_manifest'],source_name=p['local_source_manifest'],public_status=m['publication_status'],asset_reader=asset_reader)
    expected=json.loads(local_raw);expected['fields']=[new if f['field']=='publication_status' else f for f in expected['fields']]
    expected.update(publication_status=m['publication_status'],logical_sha256=result['public_logical_sha256'],
                    logical_bytes=result['public_logical_bytes'],stored_bytes=local['stored_bytes']-old['asset']['stored_bytes']+a['stored_bytes'])
    actual=dict(m);actual.pop('publication',None);actual.pop('derived_bundle',None)
    if actual!=expected or p.get('logical_sha256')!=local.get('logical_sha256') or bounded(manifest_path,1024**2)!=raw:
        raise ValueError('public transform changed content outside publication status')
    return {**result,'schema_version':'gapi-public-independent-replay/v1','manifest_sha256':hashlib.sha256(raw).hexdigest(),
            'logical_sha256':m['logical_sha256'],'logical_bytes':m['logical_bytes'],
            'scope':'independent public status transform, original local transport reconstruction and full graph-to-score replay'}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    source=parser.add_mutually_exclusive_group()
    source.add_argument('--manifest',type=Path)
    source.add_argument('--local-chunks',type=Path,help='local transport byte checks only; no rank or publication verdict')
    source.add_argument('--replay-local-chunks',type=Path,help='bounded semantic projection and independent graph replay')
    source.add_argument('--local-partitions',type=Path,help='independent exact reconstruction of row/value partitions')
    parser.add_argument('--source-local-chunks',type=Path,help='original committed source required by --local-partitions')
    args=parser.parse_args()
    if bool(args.local_partitions)!=bool(args.source_local_chunks):parser.error('--local-partitions and --source-local-chunks must be supplied together')
    result=verify_local_partitions(args.local_partitions,args.source_local_chunks) if args.local_partitions else replay_local_chunks(args.replay_local_chunks) if args.replay_local_chunks else verify_local_chunks(args.local_chunks) if args.local_chunks else verify(
        args.manifest or Path('outputs/global/manifest.json'))
    print(json.dumps(result,sort_keys=True))


if __name__=='__main__':main()
