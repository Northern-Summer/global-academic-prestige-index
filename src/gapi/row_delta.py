"""Bounded, lossless row-stream differences against one immutable full baseline.

The delta is a storage representation, never a scientific data correction. Both
baseline and reconstructed target hashes must match. Chains are deliberately not
accepted: every delta depends on exactly one full, independently retained input.
"""
from collections import deque
import hashlib
import json
import lzma
import os
import re

from .task import canonical

FORMAT='gapi-private-ranking-row-delta/1'
WINDOW=32
WINDOW_BYTES=4*1024**2
MAX_RUN=1000000


def baseline(task, filename):
    if not isinstance(filename,str) or not re.fullmatch(r'[a-f0-9]{64}\.rows',filename):
        raise ValueError('delta baseline requires a content-addressed row filename')
    folder=task.root/'frozen-inputs'
    if folder.is_symlink() or getattr(folder,'is_junction',lambda:False)():
        raise ValueError('linked snapshot directory forbidden')
    path=folder/(filename+'.xz')
    if path.is_symlink() or not path.is_file():
        raise ValueError('delta requires a retained full row baseline; chains forbidden')
    return path


def decoded_lines(path, task):
    from .row_snapshot import _packed_lines, lines, MAX_LINE
    records=iter(_packed_lines(path,task,max_line=MAX_LINE+1))
    base=None
    try:
        header=json.loads(next(records))
        if not isinstance(header,dict) or header.get('format')!=FORMAT:
            raise ValueError('unsupported row delta')
        base_path=baseline(task,header.get('base'))
        base=iter(lines(base_path,task))
        old_hash=hashlib.sha256();new_hash=hashlib.sha256();total=0;ended=False
        def consume():
            value=next(base,None)
            if value is None:raise ValueError('row delta overruns baseline')
            old_hash.update(value)
            return value
        for record in records:
            if ended:raise ValueError('trailing row delta commands')
            if record.startswith(b'A'):
                values=(record[1:],)
                if len(values[0])>MAX_LINE or values[0]==b'\n':
                    raise ValueError('invalid delta addition')
            elif re.fullmatch(rb'[CS] [1-9][0-9]{0,6}\n',record):
                count=int(record[2:])
                if count>MAX_RUN:raise ValueError('row delta run exceeds bound')
                if record.startswith(b'S'):
                    for _ in range(count):consume()
                    continue
                values=(consume() for _ in range(count))
            elif record.startswith(b'F '):
                footer=json.loads(record[2:])
                if next(base,None) is not None:raise ValueError('unconsumed row baseline')
                if old_hash.hexdigest()!=base_path.name[:64]:
                    raise ValueError('row baseline identity mismatch')
                if footer!={'sha256':new_hash.hexdigest(),'bytes':total}:
                    raise ValueError('row delta target identity mismatch')
                ended=True;continue
            else:raise ValueError('invalid row delta command')
            for value in values:
                total+=len(value)
                if total>task.config['budget_bytes']*4:raise ValueError('row delta decoded bound exceeded')
                new_hash.update(value)
                yield value
        if not ended:raise ValueError('incomplete row delta')
    except StopIteration:
        raise ValueError('incomplete row delta') from None
    finally:
        records.close()
        if base is not None:base.close()


def commands(old, new):
    """Deterministic exact-line synchronization; bounded lookahead, no inference."""
    left=deque();right=deque()
    def fill(queue, source):
        size=sum(map(len,queue))
        while len(queue)<WINDOW and size<WINDOW_BYTES:
            value=next(source,None)
            if value is None:break
            queue.append(value);size+=len(value)
    while True:
        fill(left,old);fill(right,new)
        if not left and not right:return
        if not left:
            yield 'A',right.popleft();continue
        if not right:
            left.popleft();yield 'S',None;continue
        if left[0]==right[0]:
            left.popleft();right.popleft();yield 'C',None;continue
        positions={value:i for i,value in reversed(list(enumerate(right)))}
        matches=[(i+positions[value],i,positions[value]) for i,value in enumerate(left) if value in positions]
        if matches:
            _,skip,add=min(matches)
            for _ in range(skip):left.popleft();yield 'S',None
            for _ in range(add):yield 'A',right.popleft()
        else:
            # Tables are emitted in name order. A long insertion must not
            # consume the next table's unchanged rows while seeking an anchor.
            # This only selects exact copy/skip/add operations; final stream
            # hashes still validate every reconstructed byte.
            a=json.loads(left[0]);b=json.loads(right[0])
            at=a.get('t') if isinstance(a,dict) else None
            bt=b.get('t') if isinstance(b,dict) else None
            if isinstance(at,str) and isinstance(bt,str) and at!=bt:
                if at<bt:left.popleft();yield 'S',None
                else:yield 'A',right.popleft()
                continue
            left.popleft();yield 'S',None
            yield 'A',right.popleft()


def freeze_delta(store, filename):
    from .row_snapshot import FORMAT as ROW_FORMAT, inspect, ranking_lines, lines
    task=store.task;initial_used=task.check(1024**2);task.check_memory()
    if store.db.in_transaction:raise ValueError('cannot freeze unfinished transaction')
    base_path=baseline(task,filename);folder=base_path.parent
    temporary=folder/'normalized-row-delta.xz.partial'
    created=False;old=None;new=None
    old_hash=hashlib.sha256();new_hash=hashlib.sha256();total=0
    def observed(source,sha,target=False):
        nonlocal total
        for line in source:
            sha.update(line)
            if target:total+=len(line)
            yield line
    try:
        with temporary.open('xb') as output:
            created=True
            encoder=lzma.LZMACompressor(format=lzma.FORMAT_XZ,preset=3)
            written=0
            def write(packed):
                nonlocal written
                task.check(len(packed)+65536,minimum_used=initial_used+written)
                task.check_memory();output.write(packed);written+=len(packed)
            def emit(value):
                packed=encoder.compress(value)
                if packed:
                    write(packed)
            emit(canonical({'format':FORMAT,'base':filename})+b'\n')
            old=lines(base_path,task);new=ranking_lines(store)
            operation=None;run=0
            def flush():
                nonlocal run
                if run:emit(f'{operation} {run}\n'.encode());run=0
            for op,value in commands(iter(observed(old,old_hash)),iter(observed(new,new_hash,True))):
                if op=='A':
                    flush();operation=None;emit(b'A'+value)
                else:
                    if operation!=op or run==MAX_RUN:flush()
                    operation=op;run+=1
            flush()
            if old_hash.hexdigest()!=base_path.name[:64]:raise ValueError('row baseline identity mismatch')
            checksum=new_hash.hexdigest()
            emit(b'F '+canonical({'sha256':checksum,'bytes':total})+b'\n')
            packed=encoder.flush();write(packed)
            output.flush();os.fsync(output.fileno())
        # Name-independent full reconstruction validates both inputs before promotion.
        if inspect_delta(temporary,task)!=(checksum,total):raise ValueError('row delta verification failed')
        full=folder/(checksum+'.rows.xz');target=folder/(checksum+'.rows.delta.xz')
        chosen=full if full.exists() else target
        if chosen.is_symlink():raise ValueError('linked row delta forbidden')
        if chosen.exists():
            if inspect(chosen,task)!=(checksum,total):raise ValueError('existing row input mismatch')
            temporary.unlink()
        else:os.replace(temporary,target)
        result={'sha256':checksum,'filename':checksum+'.rows','bytes':total,
            'packed_bytes':chosen.stat().st_size,'format':ROW_FORMAT if chosen==full else FORMAT,
            'contains_personal_input':True,'publicly_bundled':False,
            'restoration_requires_separate_resource_admission':True}
        if chosen!=full:result.update(base_filename=filename,logical_format=ROW_FORMAT)
        return result
    finally:
        if old is not None:old.close()
        if new is not None:new.close()
        if created:temporary.unlink(missing_ok=True)


def inspect_delta(path,task):
    sha=hashlib.sha256();total=0;count=0
    for line in decoded_lines(path,task):
        sha.update(line);total+=len(line);count+=1
    if count<2:raise ValueError('incomplete reconstructed row archive')
    return sha.hexdigest(),total
