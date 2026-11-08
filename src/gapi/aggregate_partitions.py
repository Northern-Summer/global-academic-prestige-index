"""Lossless, bounded row assets for an already completed local aggregate.

This reuses aggregate_chunks' verified input and the existing atomic writer.
It changes representation only; local publication status cannot be promoted.
Caller owns the existing writer exclusion. No person database is opened.
"""
import gzip
import hashlib
import json
from pathlib import Path
import re

from .aggregate_chunks import chunks
from .task import canonical

FORMAT = 'gapi-aggregate-partitions/1'
ROWS = 'gapi-aggregate-row-columns/1'
ROW_ASSET_BYTES = 1024**2
VALUE_BYTES = 32*1024**2
MAX_ASSETS = 10000


def read_field(directory, name, *, manifest_sha256):
    """Read one pinned field; row fields yield bounded decoded dictionaries.
    Callers exhaust row iterators before accepting the result. This is an
    aggregate reader, never a source-record or publication decision.
    """
    directory=Path(directory).absolute()
    for p in (directory,*directory.parents):
        if p.is_symlink() or (p.exists() and p.is_junction()):raise ValueError('linked partition field path')
    raw=(directory/'manifest.json').read_bytes()
    if len(raw)>4*1024**2 or hashlib.sha256(raw).hexdigest()!=manifest_sha256:raise ValueError('partition manifest drift')
    m=json.loads(raw)
    if m.get('schema_version')!=FORMAT:raise ValueError('invalid partition field manifest')
    matches=[f for f in m['fields'] if f['field']==name]
    if len(matches)!=1:raise ValueError('missing or duplicate partition field')
    f=matches[0]
    def load(a):
        if not re.fullmatch(r'[a-f0-9]{64}\.json\.gz',str(a.get('file',''))):raise ValueError('unsafe partition asset')
        path=directory/a['file']
        if path.is_symlink() or path.stat().st_size!=a['stored_bytes']:raise ValueError('partition stored length')
        packed=path.read_bytes()
        if hashlib.sha256(packed).hexdigest()!=a['stored_sha256']:raise ValueError('partition stored hash')
        import io
        with gzip.GzipFile(fileobj=io.BytesIO(packed)) as stream:decoded=stream.read(min(a['bytes'],VALUE_BYTES)+1)
        if len(decoded)!=a['bytes'] or hashlib.sha256(decoded).hexdigest()!=a['sha256']:raise ValueError('partition decoded hash/length')
        return json.loads(decoded)
    if f['kind']=='value':return load(f['asset'])
    if f['kind']=='object':return {v['key']:load(v['asset']) for v in f['entries']}
    if f['kind']!='rows':raise ValueError('invalid partition field kind')
    def rows():
        count=0;universe=None
        for a in f['assets']:
            v=load(a);columns=v['columns'];values=v['rows'];references=v.get('references',{})
            if v.get('schema_version')!=ROWS or len(values)!=a['rows'] or columns!=sorted(set(columns)):raise ValueError('partition row contract')
            if references:
                if references!={'source':'algorithm.network_universe','target':'algorithm.network_universe'} or name!='edges':raise ValueError('partition references')
                if universe is None:universe=read_field(directory,'algorithm',manifest_sha256=manifest_sha256)['network_universe']
            for row in values:
                if len(row)!=len(columns):raise ValueError('partition row width')
                item=dict(zip(columns,row))
                for key in references:
                    i=item[key]
                    if type(i) is not int or not 0<=i<len(universe):raise ValueError('partition row reference')
                    item[key]=universe[i]
                count+=1;yield item
        if count!=f['rows'] or (directory/'manifest.json').read_bytes()!=raw:raise ValueError('partition field count/drift')
    return rows()


class _Input:
    def __init__(self, pieces):
        self.pieces=iter(pieces); self.buffer=''; self.position=0; self.eof=False
        def reject(value):raise ValueError('nonfinite aggregate number')
        self.decoder=json.JSONDecoder(parse_constant=reject)

    def more(self):
        self.buffer=self.buffer[self.position:]; self.position=0
        try:self.buffer+=next(self.pieces).decode('ascii')
        except StopIteration:self.eof=True

    def peek(self):
        while self.position==len(self.buffer) and not self.eof:self.more()
        return self.buffer[self.position:self.position+1]

    def expect(self, char):
        if self.peek()!=char:raise ValueError('noncanonical aggregate delimiter')
        self.position+=1

    def value(self, maximum=VALUE_BYTES):
        while True:
            try:value,end=self.decoder.raw_decode(self.buffer,self.position)
            except json.JSONDecodeError:
                if self.eof:raise ValueError('invalid or truncated aggregate value') from None
            else:
                if end-self.position>maximum:raise ValueError('aggregate value exceeds bound')
                # A scalar ending at a buffer edge may continue in the next piece.
                if end<len(self.buffer) and self.buffer[end] not in ',]}:':
                    if self.eof:raise ValueError('invalid aggregate scalar boundary')
                elif end<len(self.buffer) or self.eof:
                    self.position=end
                    return value
            if len(self.buffer)-self.position>maximum:raise ValueError('aggregate value exceeds bound')
            self.more()


def _directory(task, path):
    path=Path(path).absolute()
    if not path.is_relative_to(task.root.absolute()) or path==task.root.absolute():
        raise ValueError('partition directory must be inside the task directory')
    for p in (path,*path.parents):
        if p.is_symlink() or (p.exists() and p.is_junction()):raise ValueError('linked partition path')
    return path


def repartition(task, source, target=None, *, progress=None):
    """Plan in bounded memory, or write and commit a fully equivalent manifest.

    An omitted target measures actual gzip bytes without creating any files.
    Interrupted writes leave reusable assets; the old manifest remains selected.
    Row columns are exact per asset, including absent fields and null values.
    """
    source=_directory(task,source)
    pointer=source/'manifest.json'; original=pointer.read_bytes()
    upstream=json.loads(original)
    return _convert(task,upstream,chunks(source,check=task.check_memory),target,
                    source=source,original=lambda:original,
                    final_check=lambda:pointer.read_bytes()==original,progress=progress)


def write_result(task, result, target, *, progress=None):
    """Emit a completed local calculation directly, without a duplicate legacy
    gzip set. Its exact legacy manifest is retained as a reproducible anchor.
    The caller owns analysis acceptance and the existing writer exclusion.
    """
    from .aggregate_chunks import AggregateRows,CHUNK_BYTES,FORMAT as LEGACY
    if (result.get('schema_version')!='gapi-global-observed/v1'
            or result.get('publication_status')!='local-only-source-license-review'
            or any(not isinstance(result.get(k),(list,AggregateRows)) for k in ('nodes','edges'))):
        raise ValueError('completed local aggregate required')
    upstream={'schema_version':LEGACY,'logical_schema_version':result['schema_version'],
              'nodes':len(result['nodes']),'edges':len(result['edges']),
              'publication_status':result['publication_status'],'chunks':[]}
    def pieces():
        sha=hashlib.sha256();size=0;pending=bytearray()
        def block(raw):
            nonlocal size
            sha.update(raw);size+=len(raw);packed=gzip.compress(raw,compresslevel=6,mtime=0)
            h=hashlib.sha256(packed).hexdigest()
            upstream['chunks'].append({'file':h+'.json.part.gz','sha256':h,'bytes':len(packed),
                'decoded_bytes':len(raw),'decoded_sha256':hashlib.sha256(raw).hexdigest()})
            return raw
        def tokens():
            yield b'{'
            for n,key in enumerate(sorted(result)):
                if n:yield b','
                yield canonical(key)+b':'
                if key in ('nodes','edges'):
                    yield b'[';count=0
                    for row in result[key]:
                        if count:yield b','
                        yield canonical(row);count+=1
                    if count!=len(result[key]):raise ValueError('aggregate row producer count mismatch')
                    yield b']'
                else:yield canonical(result[key])
            yield b'}'
        for number,raw in enumerate(tokens(),1):
            for offset in range(0,len(raw),65536):
                pending.extend(raw[offset:offset+65536])
                while len(pending)>=CHUNK_BYTES:
                    yield block(bytes(pending[:CHUNK_BYTES]));del pending[:CHUNK_BYTES]
            if number%10000==0:task.check_memory()
        if pending:yield block(bytes(pending))
        upstream.update(logical_sha256=sha.hexdigest(),decoded_bytes=size)
    value=_convert(task,upstream,pieces(),target,original=lambda:canonical(upstream),progress=progress)
    return value


def _convert(task, upstream, pieces, target, *, source=None, original, final_check=lambda:True, progress=None):
    if target is not None:
        target=_directory(task,target)
        if target==source:raise ValueError('source and partition target must differ')
        target.mkdir(parents=True,exist_ok=True)
    reader=_Input(pieces)
    fields=[];logical=hashlib.sha256();logical_bytes=0;stored_bytes=0;asset_count=0;node_index=None
    def emit(raw):
        nonlocal logical_bytes
        logical.update(raw);logical_bytes+=len(raw)
    def asset(value, rows=None):
        nonlocal stored_bytes,asset_count
        raw=canonical(value)
        if len(raw)>(ROW_ASSET_BYTES if rows is not None else VALUE_BYTES):
            raise ValueError('partition asset exceeds decoded bound')
        body=gzip.compress(raw,compresslevel=9,mtime=0)
        sha=hashlib.sha256(raw).hexdigest(); name=sha+'.json.gz'
        entry={'file':name,'sha256':sha,'bytes':len(raw),'stored_sha256':hashlib.sha256(body).hexdigest(),
               'stored_bytes':len(body),'encoding':'gzip'}
        if rows is not None:entry['rows']=rows
        asset_count+=1
        if asset_count>MAX_ASSETS:raise ValueError('partition asset count exceeds bound')
        stored_bytes+=len(body);task.check_memory()
        if target is not None:
            from .global_analysis import _write_aggregate
            path=target/name
            if path.exists():
                if path.is_symlink() or path.read_bytes()!=body:raise ValueError('partition content address collision')
            else:_write_aggregate(task,path,body)
            if gzip.decompress(path.read_bytes())!=raw:raise ValueError('partition readback mismatch')
        return entry
    reader.expect('{');emit(b'{');previous=None
    while True:
        if len(fields)>=64:raise ValueError('aggregate top-level field bound')
        key=reader.value(1024)
        if not isinstance(key,str) or (previous is not None and key<=previous):
            raise ValueError('duplicate or unordered aggregate field')
        if previous is not None:emit(b',')
        previous=key;reader.expect(':');emit(canonical(key)+b':')
        if key in ('nodes','edges'):
            reader.expect('[');emit(b'[');entries=[];rows=[];columns=None;size=2;count=0
            references=({'source':'algorithm.network_universe','target':'algorithm.network_universe'}
                        if key=='edges' and node_index is not None else {})
            def flush():
                nonlocal rows,columns,size
                if rows:
                    value={'schema_version':ROWS,'columns':columns,'rows':rows}
                    if references:value['references']=references
                    entries.append(asset(value,len(rows)))
                rows=[];columns=None;size=2
            while reader.peek()!=']':
                row=reader.value(ROW_ASSET_BYTES)
                if not isinstance(row,dict) or not row:raise ValueError('invalid aggregate row')
                keys=sorted(row)
                if len(keys)>64:raise ValueError('aggregate row column bound')
                values=[node_index[row[k]] if k in references else row[k] for k in keys]
                encoded=canonical(values)
                frame={'schema_version':ROWS,'columns':keys,'rows':[]}
                if references:frame['references']=references
                overhead=len(canonical(frame))
                if rows and (columns!=keys or size+len(encoded)+overhead>ROW_ASSET_BYTES):flush()
                if len(encoded)+overhead>ROW_ASSET_BYTES:raise ValueError('aggregate row exceeds asset bound')
                columns=keys;rows.append(values);size+=len(encoded)+1
                if count:emit(b',')
                emit(canonical(row));count+=1
                if count%10000==0:task.check_memory()
                if reader.peek()==']':break
                reader.expect(',')
                if reader.peek()==']':raise ValueError('trailing aggregate row comma')
            flush();reader.expect(']');emit(b']')
            if count!=upstream[key]:raise ValueError('source aggregate row count mismatch')
            fields.append({'field':key,'kind':'rows','rows':count,'assets':entries})
        elif key=='sensitivity':
            reader.expect('{');emit(b'{');entries=[];last=None
            while reader.peek()!='}':
                name=reader.value(1024)
                if not isinstance(name,str) or (last is not None and name<=last):raise ValueError('invalid scenario key order')
                if last is not None:emit(b',')
                last=name;reader.expect(':');value=reader.value()
                emit(canonical(name)+b':'+canonical(value));entries.append({'key':name,'asset':asset(value)})
                if reader.peek()=='}':break
                reader.expect(',')
                if reader.peek()=='}':raise ValueError('trailing scenario comma')
            reader.expect('}');emit(b'}');fields.append({'field':key,'kind':'object','entries':entries})
        else:
            value=reader.value();raw=canonical(value);emit(raw)
            if key=='algorithm' and isinstance(value,dict) and 'network_universe' in value:
                universe=value['network_universe']
                if (not isinstance(universe,list) or len(universe)>200000 or any(not isinstance(v,str) for v in universe)
                        or len(set(universe))!=len(universe)):raise ValueError('invalid reference universe')
                node_index={v:i for i,v in enumerate(universe)}
            fields.append({'field':key,'kind':'value','asset':asset(value)})
        if progress:progress({'field':key,'packed_bytes':stored_bytes,'assets':asset_count})
        delimiter=reader.peek()
        if delimiter=='}':reader.expect('}');emit(b'}');break
        reader.expect(',')
    if reader.peek():raise ValueError('trailing aggregate data')
    if (logical.hexdigest()!=upstream['logical_sha256'] or logical_bytes!=upstream['decoded_bytes']
            or not final_check()):raise ValueError('partition logical input mismatch or drift')
    result={'schema_version':FORMAT,'logical_schema_version':upstream['logical_schema_version'],
            'publication_status':'local-only-source-license-review','source_manifest_sha256':hashlib.sha256(original()).hexdigest(),
            'logical_sha256':logical.hexdigest(),'logical_bytes':logical_bytes,'nodes':upstream['nodes'],'edges':upstream['edges'],
            'stored_bytes':stored_bytes,'asset_count':asset_count,'fields':fields}
    if target is not None:
        from .global_analysis import _write_aggregate
        if source is None:_write_aggregate(task,target/'source-manifest.json',original())
        _write_aggregate(task,target/'manifest.json',canonical(result))
    return result


def logical_chunks(directory, *, check=None):
    """Restore exact canonical logical bytes from committed row/value assets.

    Exhaustion verifies the final logical hash and both declared row counts.
    This is a representation check, not independent ranking or source review.
    """
    directory=Path(directory).absolute()
    for p in (directory,*directory.parents):
        if p.is_symlink() or (p.exists() and p.is_junction()):raise ValueError('linked partition directory')
    pointer=directory/'manifest.json'
    if pointer.is_symlink() or pointer.stat().st_size>4*1024**2:raise ValueError('invalid partition manifest')
    original=pointer.read_bytes();m=json.loads(original)
    if (m.get('schema_version')!=FORMAT or m.get('logical_schema_version')!='gapi-global-observed/v1'
            or m.get('publication_status')!='local-only-source-license-review'
            or not isinstance(m.get('fields'),list) or len(m['fields'])>64):raise ValueError('invalid partition contract')
    total=0;sha=hashlib.sha256();counts={};assets=packed=0;universe=None
    def read(entry, maximum):
        nonlocal assets,packed
        if check:check()
        if (not isinstance(entry,dict) or not re.fullmatch('[a-f0-9]{64}',str(entry.get('sha256','')))
                or entry.get('file')!=entry['sha256']+'.json.gz' or entry.get('encoding')!='gzip'
                or type(entry.get('bytes')) is not int or not 0<entry['bytes']<=maximum
                or type(entry.get('stored_bytes')) is not int or not 0<entry['stored_bytes']<=maximum+65536):
            raise ValueError('invalid partition asset descriptor')
        p=directory/entry['file']
        if p.is_symlink() or p.stat().st_size!=entry['stored_bytes']:raise ValueError('partition stored length mismatch')
        body=p.read_bytes()
        if hashlib.sha256(body).hexdigest()!=entry.get('stored_sha256'):raise ValueError('partition stored hash mismatch')
        import io
        with gzip.GzipFile(fileobj=io.BytesIO(body)) as stream:raw=stream.read(maximum+1)
        if len(raw)!=entry['bytes'] or hashlib.sha256(raw).hexdigest()!=entry['sha256']:
            raise ValueError('partition decoded identity mismatch')
        assets+=1;packed+=len(body)
        if assets>MAX_ASSETS:raise ValueError('partition asset count bound')
        return json.loads(raw)
    def parts():
        nonlocal universe
        yield b'{';previous=None
        for field in m['fields']:
            key=field.get('field')
            if not isinstance(key,str) or (previous is not None and key<=previous):raise ValueError('invalid partition field order')
            if previous is not None:yield b','
            previous=key;yield canonical(key)+b':'
            if field.get('kind')=='rows' and key in ('nodes','edges'):
                yield b'[';count=0
                if not isinstance(field.get('assets'),list):raise ValueError('invalid partition rows')
                for entry in field['assets']:
                    value=read(entry,ROW_ASSET_BYTES);columns=value.get('columns');rows=value.get('rows')
                    references=value.get('references',{})
                    if (value.get('schema_version')!=ROWS or not isinstance(columns,list) or not columns
                            or any(not isinstance(k,str) for k in columns) or columns!=sorted(set(columns)) or len(columns)>64
                            or not isinstance(rows,list) or type(entry.get('rows')) is not int or len(rows)!=entry['rows']):
                        raise ValueError('invalid partition row columns/count')
                    if references and (key!='edges' or references!={'source':'algorithm.network_universe','target':'algorithm.network_universe'}
                                       or universe is None or not set(references)<=set(columns)):
                        raise ValueError('invalid partition reference contract')
                    for row in rows:
                        if not isinstance(row,list) or len(row)!=len(columns):raise ValueError('partition row width mismatch')
                        if count:yield b','
                        restored=dict(zip(columns,row))
                        for name in references:
                            index=restored[name]
                            if type(index) is not int or not 0<=index<len(universe):raise ValueError('partition reference index out of bounds')
                            restored[name]=universe[index]
                        yield canonical(restored);count+=1
                yield b']';counts[key]=count
                if type(field.get('rows')) is not int or count!=field['rows']:raise ValueError('partition field row mismatch')
            elif field.get('kind')=='object' and key=='sensitivity':
                if not isinstance(field.get('entries'),list) or len(field['entries'])>256:raise ValueError('invalid partition scenarios')
                yield b'{';last=None
                for item in field['entries']:
                    name=item.get('key')
                    if not isinstance(name,str) or (last is not None and name<=last):raise ValueError('invalid partition scenario order')
                    if last is not None:yield b','
                    last=name;yield canonical(name)+b':'+canonical(read(item['asset'],VALUE_BYTES))
                yield b'}'
            elif field.get('kind')=='value' and key not in ('nodes','edges','sensitivity'):
                value=read(field['asset'],VALUE_BYTES)
                if key=='algorithm' and isinstance(value,dict) and 'network_universe' in value:
                    universe=value['network_universe']
                    if (not isinstance(universe,list) or len(universe)>200000 or any(not isinstance(v,str) for v in universe)
                            or len(set(universe))!=len(universe)):raise ValueError('invalid partition universe')
                yield canonical(value)
            else:raise ValueError('invalid partition field kind')
        yield b'}'
    for part in parts():
        total+=len(part);sha.update(part)
        yield part
    if (total!=m.get('logical_bytes') or sha.hexdigest()!=m.get('logical_sha256')
            or counts!={k:m.get(k) for k in ('nodes','edges')} or assets!=m.get('asset_count')
            or packed!=m.get('stored_bytes') or pointer.read_bytes()!=original):raise ValueError('partition logical identity mismatch')
