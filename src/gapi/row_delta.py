"""Bounded, lossless row-stream differences against one immutable full baseline.

The delta is a storage representation, never a scientific data correction. Both
baseline and reconstructed target hashes must match. Chains are deliberately not
accepted: every delta depends on exactly one full, independently retained input.
"""
from collections import deque
import base64
import hashlib
import json
import lzma
import os
import re

from .task import canonical

FORMAT='gapi-private-ranking-row-delta/1'
FORMAT_V2='gapi-private-ranking-row-delta/2'
WINDOW=32
WINDOW_BYTES=4*1024**2
MAX_RUN=1000000


def addition_record(value):
    """Expose repeated normalized JSON to stream compression, restoring exact BLOBs.

    GAPIz1 already compressed each event before the row archive's Base64 layer.
    For matching canonical events only, E carries its decoded text plus the old
    BLOB digest; reconstruction must reproduce that exact original row line.
    Unknown schemas, codecs and noncanonical rows retain the ordinary A command.
    """
    from .event_storage import MAGIC, pack, unpack
    from .row_snapshot import MAX_LINE
    row=json.loads(value)
    if not isinstance(row,dict) or set(row)!={'t','v'} or row['t']!='events':return b'A'+value
    cells=row['v']
    if not isinstance(cells,list) or len(cells)!=13:return b'A'+value
    cell=cells[-1]
    if not isinstance(cell,list) or len(cell)!=2 or cell[0]!='b' or not isinstance(cell[1],str):return b'A'+value
    try:stored=base64.b64decode(cell[1],validate=True)
    except ValueError:return b'A'+value
    if not stored.startswith(MAGIC):return b'A'+value
    text=unpack(stored)
    if pack(text)!=stored:return b'A'+value
    if canonical(row)+b'\n'!=value:return b'A'+value
    row['v'][-1]=['z1',text,hashlib.sha256(stored).hexdigest()]
    encoded=b'E'+canonical(row)+b'\n'
    return encoded if len(encoded)<=MAX_LINE+1 else b'A'+value


def restore_addition(record):
    from .event_storage import pack, MAX_BYTES
    from .row_snapshot import MAX_LINE
    row=json.loads(record[1:])
    if (not isinstance(row,dict) or set(row)!={'t','v'} or row.get('t')!='events'
            or not isinstance(row.get('v'),list) or len(row['v'])!=13):
        raise ValueError('invalid expanded event addition')
    cell=row['v'][-1]
    if (not isinstance(cell,list) or len(cell)!=3 or cell[0]!='z1'
            or not isinstance(cell[1],str) or len(cell[1].encode('utf-8'))>MAX_BYTES
            or not isinstance(cell[2],str) or not re.fullmatch('[a-f0-9]{64}',cell[2])):
        raise ValueError('invalid expanded event payload')
    stored=pack(cell[1])
    if not isinstance(stored,bytes) or hashlib.sha256(stored).hexdigest()!=cell[2]:
        raise ValueError('expanded event reconstruction mismatch')
    row['v'][-1]=['b',base64.b64encode(stored).decode('ascii')]
    value=canonical(row)+b'\n'
    if len(value)>MAX_LINE:raise ValueError('expanded event row exceeds bound')
    return value


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
        if not isinstance(header,dict) or header.get('format') not in (FORMAT,FORMAT_V2):
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
            elif record.startswith(b'E') and header['format']==FORMAT_V2:
                values=(restore_addition(record),)
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


def freeze_delta(store, filename, *, compression_preset=3):
    from .row_snapshot import FORMAT as ROW_FORMAT, inspect, ranking_lines, lines, validate_compression_preset
    validate_compression_preset(compression_preset)
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
            encoder=lzma.LZMACompressor(format=lzma.FORMAT_XZ,preset=compression_preset)
            written=0
            def write(packed):
                nonlocal written
                task.check(len(packed)+65536,minimum_used=initial_used+written)
                task.check_memory();output.write(packed);written+=len(packed)
            def emit(value):
                packed=encoder.compress(value)
                if packed:
                    write(packed)
            emit(canonical({'format':FORMAT_V2,'base':filename})+b'\n')
            old=lines(base_path,task);new=ranking_lines(store)
            operation=None;run=0
            def flush():
                nonlocal run
                if run:emit(f'{operation} {run}\n'.encode());run=0
            for op,value in commands(iter(observed(old,old_hash)),iter(observed(new,new_hash,True))):
                if op=='A':
                    flush();operation=None;emit(addition_record(value))
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
        actual_format=ROW_FORMAT
        if chosen!=full:
            from .row_snapshot import _packed_lines
            records=_packed_lines(chosen,task)
            try:actual_format=json.loads(next(records))['format']
            finally:records.close()
        result={'sha256':checksum,'filename':checksum+'.rows','bytes':total,
            'packed_bytes':chosen.stat().st_size,'format':actual_format,
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


def repack_delta(store, filename):
    """Losslessly upgrade a retained /1 archive without changing its input ID.

    Caller owns CareerStore's writer exclusion, including frozen readers. The
    predecessor remains until complete source/target reconstruction succeeds.
    Public historical metadata describes its original storage measurement;
    logical filename/hash and the retained full baseline remain authoritative.
    """
    from .frozen_input import snapshot_path, file_hash
    from .row_snapshot import _packed_lines, MAX_LINE
    path=snapshot_path(store,filename).with_suffix('.rows.delta.xz')
    if not filename.endswith('.rows') or not path.is_file():
        raise ValueError('retained row delta required')
    if store.db.in_transaction:raise ValueError('cannot repack unfinished transaction')
    task=store.task;initial_used=task.check(1024**2);task.check_memory()
    expected=inspect_delta(path,task)
    if expected[0]!=filename[:64]:raise ValueError('retained delta identity mismatch')
    before=path.stat().st_size;original_hash=file_hash(path)
    records=iter(_packed_lines(path,task,max_line=MAX_LINE+1))
    temporary=path.with_suffix('.xz.repack.partial');created=False
    try:
        header=json.loads(next(records))
        if header['format']==FORMAT_V2:
            return {'filename':filename,'sha256':expected[0],'bytes':expected[1],
                    'already_repacked':True,'saved_bytes':0,'packed_bytes':before}
        if header['format']!=FORMAT:raise ValueError('unsupported retained delta')
        header['format']=FORMAT_V2
        with temporary.open('xb') as output:
            created=True;written=0
            encoder=lzma.LZMACompressor(format=lzma.FORMAT_XZ,preset=3)
            def write(value):
                nonlocal written
                task.check(len(value)+65536,minimum_used=initial_used+written)
                task.check_memory();output.write(value);written+=len(value)
            def emit(value):
                encoded=encoder.compress(value)
                if encoded:write(encoded)
            emit(canonical(header)+b'\n')
            for record in records:
                emit(addition_record(record[1:]) if record.startswith(b'A') else record)
            write(encoder.flush());output.flush();os.fsync(output.fileno())
        records.close()
        if inspect_delta(temporary,task)!=expected:
            raise ValueError('repacked delta reconstruction mismatch')
        if file_hash(path)!=original_hash:raise ValueError('retained delta changed during repack')
        after=temporary.stat().st_size
        if after>=before:
            return {'filename':filename,'sha256':expected[0],'bytes':expected[1],
                    'already_repacked':False,'retained_predecessor':True,
                    'saved_bytes':0,'packed_bytes':before}
        os.replace(temporary,path)
        return {'filename':filename,'sha256':expected[0],'bytes':expected[1],
                'already_repacked':False,'saved_bytes':before-after,
                'previous_packed_bytes':before,'packed_bytes':after,
                'previous_storage_sha256':original_hash,'storage_sha256':file_hash(path),
                'logical_identity_unchanged':True,'format':FORMAT_V2}
    finally:
        records.close()
        if created:temporary.unlink(missing_ok=True)
