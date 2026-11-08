"""Lossless local aggregate transport; scoring and publication policy stay outside.

The manifest is committed last. Content-addressed gzip chunks are bounded and
self-contained; a failed run cannot replace a previously readable manifest.
This transport does not make a local result eligible for public/browser use.
"""
import gzip
import hashlib
import io
import json
import re
from pathlib import Path

from .task import canonical

FORMAT = 'gapi-local-aggregate-chunks/1'
CHUNK_BYTES = 1024**2
MAX_CHUNKS = 10000


def _descriptor(entry):
    if not isinstance(entry,dict):raise ValueError('invalid aggregate chunk descriptor')
    checksum=entry.get('sha256','');name=entry.get('file','')
    if (not re.fullmatch('[a-f0-9]{64}',str(checksum)) or name!=checksum+'.json.part.gz'
            or type(entry.get('bytes')) is not int or not 0<entry['bytes']<=CHUNK_BYTES+65536
            or type(entry.get('decoded_bytes')) is not int or not 0<entry['decoded_bytes']<=CHUNK_BYTES):
        raise ValueError('invalid aggregate chunk descriptor')
    return name,checksum


class AggregateRows:
    """Repeatable rows owned by a completed calculation, without a row list."""
    def __init__(self, count, factory):
        if type(count) is not int or count<0 or not callable(factory):
            raise ValueError('invalid aggregate row producer')
        self.count,self.factory=count,factory

    def __len__(self):return self.count

    def __iter__(self):return iter(self.factory())


def write(task, result, directory):
    """Caller holds the existing task writer exclusion for the whole operation."""
    from .global_analysis import _write_aggregate
    directory = Path(directory).absolute()
    if not directory.is_relative_to(task.root.absolute()):
        raise ValueError('local aggregate chunks must remain in task storage')
    for path in (directory, *directory.parents):
        if path.is_symlink() or (path.exists() and path.is_junction()):
            raise ValueError('linked aggregate directory')
    if (directory/'successor.json').exists():raise ValueError('frozen chunk generation has a selected successor; do not overwrite it')
    if result.get('schema_version') != 'gapi-global-observed/v1':
        raise ValueError('global observation aggregate required')
    if any(not isinstance(result.get(key),(list,AggregateRows)) for key in ('nodes','edges')):
        raise ValueError('aggregate nodes and edges must be lists or repeatable aggregate rows')
    task.check_memory()
    task.check(CHUNK_BYTES*3)
    directory.mkdir(parents=True, exist_ok=True)
    entries=[]
    logical=hashlib.sha256()
    decoded=0
    encoder=json.JSONEncoder(ensure_ascii=True,sort_keys=True,separators=(',',':'),allow_nan=False)
    pending=bytearray()

    def emit():
        nonlocal decoded
        if len(entries)>=MAX_CHUNKS:
            raise ValueError('aggregate chunk count exceeds bound')
        raw=bytes(pending);logical.update(raw);decoded+=len(raw)
        body=gzip.compress(raw,compresslevel=6,mtime=0)
        checksum=hashlib.sha256(body).hexdigest();filename=checksum+'.json.part.gz'
        path=directory/filename
        if path.exists():
            if path.is_symlink() or path.stat().st_size!=len(body) or path.read_bytes()!=body:
                raise ValueError('aggregate content address conflict')
        else:
            _write_aggregate(task,path,body)
        with gzip.GzipFile(fileobj=io.BytesIO(path.read_bytes())) as stream:
            if stream.read(CHUNK_BYTES+1)!=raw:
                raise ValueError('aggregate chunk write verification failed')
        entries.append({'file':filename,'sha256':checksum,'bytes':len(body),
                        'decoded_bytes':len(raw),'decoded_sha256':hashlib.sha256(raw).hexdigest()})
        pending.clear();task.check_memory()

    # JSONEncoder preserves canonical field/order semantics. Large string tokens
    # are split before byte-buffer admission; no complete serialized copy exists.
    def tokens():
        yield '{'
        for index,key in enumerate(sorted(result)):
            if index:yield ','
            yield from encoder.iterencode(key)
            yield ':'
            value=result[key]
            if key in ('nodes','edges'):
                yield '[';count=0
                for row in value:
                    if count:yield ','
                    yield from encoder.iterencode(row)
                    count+=1
                if count!=len(value):raise ValueError('aggregate row producer count mismatch')
                yield ']'
            else:yield from encoder.iterencode(value)
        yield '}'
    for token in tokens():
        for start in range(0,len(token),65536):
            data=token[start:start+65536].encode('ascii')
            while data:
                room=CHUNK_BYTES-len(pending)
                pending.extend(data[:room]);data=data[room:]
                if len(pending)==CHUNK_BYTES:emit()
    if pending:emit()
    manifest={'schema_version':FORMAT,'logical_schema_version':result['schema_version'],
              'logical_sha256':logical.hexdigest(),'decoded_bytes':decoded,
              'nodes':len(result['nodes']),'edges':len(result['edges']),
              'publication_status':'local-only-source-license-review','chunks':entries}
    _write_aggregate(task,directory/'manifest.json',canonical(manifest))
    return manifest


def chunks(directory, *, check=None):
    """Verify transport bytes and concatenated logical hash before final success.

    Consumers must consume to exhaustion before accepting any derived result.
    This reader returns bytes, avoiding unbounded JSON materialization.
    """
    directory=Path(directory)
    for path in (directory,*directory.absolute().parents):
        if path.is_symlink() or (path.exists() and path.is_junction()):
            raise ValueError('linked aggregate directory')
    pointer=directory/'manifest.json'
    if pointer.is_symlink() or pointer.stat().st_size>4*1024**2:
        raise ValueError('aggregate manifest exceeds bound or is linked')
    manifest=json.loads(pointer.read_text(encoding='utf-8'))
    entries=manifest.get('chunks')
    if (manifest.get('schema_version')!=FORMAT
            or manifest.get('logical_schema_version')!='gapi-global-observed/v1'
            or manifest.get('publication_status')!='local-only-source-license-review'
            or any(type(manifest.get(k)) is not int or manifest[k]<0 for k in ('nodes','edges'))
            or not isinstance(entries,list)
            or not 0<len(entries)<=MAX_CHUNKS
            or type(manifest.get('decoded_bytes')) is not int or manifest['decoded_bytes']<=0
            or not re.fullmatch('[a-f0-9]{64}',str(manifest.get('logical_sha256','')))):
        raise ValueError('invalid aggregate manifest')
    if (directory/'successor.json').exists():
        yield from _successor_chunks(directory,manifest,check=check)
        return
    total=0;logical=hashlib.sha256()
    for entry in entries:
        if check:check()
        name,checksum=_descriptor(entry)
        path=directory/name
        if path.is_symlink() or path.stat().st_size!=entry['bytes']:
            raise ValueError('aggregate chunk length or link mismatch')
        body=path.read_bytes()
        if hashlib.sha256(body).hexdigest()!=checksum:
            raise ValueError('aggregate chunk hash mismatch')
        with gzip.GzipFile(fileobj=io.BytesIO(body)) as stream:
            raw=stream.read(CHUNK_BYTES+1)
        if (len(raw)!=entry['decoded_bytes'] or hashlib.sha256(raw).hexdigest()!=entry.get('decoded_sha256')):
            raise ValueError('aggregate decoded chunk mismatch')
        total+=len(raw);logical.update(raw)
        if total>manifest['decoded_bytes']:
            raise ValueError('aggregate decoded total exceeds manifest')
        yield raw
    if total!=manifest['decoded_bytes'] or logical.hexdigest()!=manifest['logical_sha256']:
        raise ValueError('aggregate logical hash mismatch')


def _successor_chunks(directory, manifest, *, check=None, selection=None):
    """Recreate original chunk boundaries and exact gzip identities on demand."""
    pointer=directory/'manifest.json';original=pointer.read_bytes()
    link=directory/'successor.json'
    if selection is None:
        if link.is_symlink() or link.stat().st_size>65536:raise ValueError('invalid aggregate successor link')
        selection=json.loads(link.read_bytes())
    name=selection.get('directory')
    if (selection.get('schema_version')!='gapi-aggregate-successor/1'
            or not isinstance(name,str) or not re.fullmatch('[A-Za-z0-9_-]+',name)
            or name==directory.name
            or selection.get('source_manifest_sha256')!=hashlib.sha256(original).hexdigest()):
        raise ValueError('aggregate successor source binding mismatch')
    successor=directory.parent/name;selected=successor/'manifest.json'
    if successor.is_symlink() or successor.is_junction() or selected.is_symlink():raise ValueError('linked aggregate successor')
    raw=selected.read_bytes()
    if len(raw)>4*1024**2 or hashlib.sha256(raw).hexdigest()!=selection.get('manifest_sha256'):
        raise ValueError('aggregate successor manifest drift')
    m=json.loads(raw)
    if (m.get('source_manifest_sha256')!=selection['source_manifest_sha256']
            or m.get('logical_sha256')!=manifest['logical_sha256']
            or m.get('logical_bytes')!=manifest['decoded_bytes']
            or any(m.get(k)!=manifest[k] for k in ('nodes','edges'))):raise ValueError('aggregate successor content binding mismatch')
    from .aggregate_partitions import logical_chunks
    pending=bytearray();index=0;sha=hashlib.sha256();total=0
    for piece in logical_chunks(successor,check=check):
        pending.extend(piece)
        while index<len(manifest['chunks']) and len(pending)>=manifest['chunks'][index]['decoded_bytes']:
            entry=manifest['chunks'][index];_descriptor(entry);length=entry['decoded_bytes'];block=bytes(pending[:length]);del pending[:length]
            body=gzip.compress(block,compresslevel=6,mtime=0)
            if (hashlib.sha256(block).hexdigest()!=entry.get('decoded_sha256') or len(body)!=entry['bytes']
                    or hashlib.sha256(body).hexdigest()!=entry['sha256']):raise ValueError('successor cannot recreate original gzip bytes')
            sha.update(block);total+=len(block);index+=1
            yield block
    if (pending or index!=len(manifest['chunks']) or total!=manifest['decoded_bytes']
            or sha.hexdigest()!=manifest['logical_sha256'] or pointer.read_bytes()!=original
            or selected.read_bytes()!=raw):raise ValueError('aggregate successor reconstruction mismatch')


def select_successor(task, directory, successor):
    """Prove exact recovery before selecting a compact successor. Deletes nothing.

    Caller holds the existing writer lock. The immutable original manifest stays
    in place, so existing readers and explicitly admitted rematerialization work.
    """
    directory=Path(directory).absolute();successor=Path(successor).absolute()
    if (directory.parent!=successor.parent or not directory.is_relative_to(task.root.absolute())
            or directory==successor):raise ValueError('successor must be a sibling inside the task directory')
    for p in (directory,successor,*directory.parents):
        if p.is_symlink() or (p.exists() and p.is_junction()):raise ValueError('linked successor selection')
    source=directory/'manifest.json';target=successor/'manifest.json'
    selection={'schema_version':'gapi-aggregate-successor/1','directory':successor.name,
               'source_manifest_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),
               'manifest_sha256':hashlib.sha256(target.read_bytes()).hexdigest()}
    manifest=json.loads(source.read_bytes());count=total=0
    for block in _successor_chunks(directory,manifest,check=task.check_memory,selection=selection):count+=1;total+=len(block)
    from .global_analysis import _write_aggregate
    _write_aggregate(task,directory/'successor.json',canonical(selection))
    return {**selection,'original_chunks_reconstructed':count,'original_decoded_bytes':total,'source_files_deleted':0}


def materialize_original(task, directory):
    """Restore exact legacy gzip files only when their actual storage fits."""
    directory=Path(directory).absolute()
    if not directory.is_relative_to(task.root.absolute()):raise ValueError('legacy materialization outside task')
    manifest=json.loads((directory/'manifest.json').read_bytes());created=0
    from .global_analysis import _write_aggregate
    for entry,block in zip(manifest['chunks'],chunks(directory,check=task.check_memory),strict=True):
        name,_=_descriptor(entry);body=gzip.compress(block,compresslevel=6,mtime=0)
        if hashlib.sha256(body).hexdigest()!=entry['sha256']:raise ValueError('legacy materialization identity mismatch')
        path=directory/name
        if path.exists():
            if path.is_symlink() or path.read_bytes()!=body:raise ValueError('legacy file content drift')
        else:_write_aggregate(task,path,body);created+=1
    return {'created_files':created,'original_manifest_retained':True}
