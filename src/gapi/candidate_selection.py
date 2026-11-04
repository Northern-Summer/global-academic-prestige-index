"""Immutable candidate selections in the acquisition database, without ID copies.

The discovery queue owns candidate identity and provenance. A frozen rowid
prefix and its source hashes identify one finite processing scope; subsequent
discovery never expands that scope. Counts are recomputed by CareerStore.
"""
import json
import re

from .task import canonical, digest

SCHEMA = 'gapi-candidate-selection/1'
TABLE = 'candidate_selections'
DDL = 'CREATE TABLE IF NOT EXISTS candidate_selections(id TEXT PRIMARY KEY,payload TEXT NOT NULL)'


def candidate_rowid_filter(upper):
    if upper is None:
        return '', ()
    if type(upper) is not int or not 0 <= upper <= 2**63-1:
        raise ValueError('invalid candidate selection rowid boundary')
    return ' AND d.rowid<=?', (upper,)


def _sources(progress):
    return [{key: row[key] for key in ('source','candidates','selection_sha256')}
            for row in progress['sources']]


def _progress(store, upper):
    progress=store.candidate_progress(max_candidate_rowid=upper)
    total=store.db.execute('SELECT count(*) FROM discovery d WHERE d.rowid<=?',(upper,)).fetchone()[0]
    if (sum(row['candidates'] for row in progress['sources'])!=total
            or any(row['source'] not in store.task.config['sources'] for row in progress['sources'])):
        raise ValueError('candidate selection has an unsupported or unregistered queue source')
    store.task.check_memory()
    return progress


def freeze(store):
    """Caller owns the usual writer lock; selection and publication are atomic."""
    store.task.require_frozen()
    if store.db.in_transaction:
        raise ValueError('candidate freeze requires a settled transaction')
    with store.member('candidate-selection','freeze',max_growth_bytes=8*1024**2):
        upper=max(0,store.db.execute('SELECT COALESCE(max(rowid),0) FROM discovery').fetchone()[0])
        progress=_progress(store,upper)
        value={'schema_version':SCHEMA,'config_sha256':digest(store.task.config),
               'max_candidate_rowid':upper,'sources':_sources(progress)}
        identifier=digest(value)
        store.db.execute(DDL)
        added=store.db.execute('INSERT OR IGNORE INTO '+TABLE+' VALUES(?,?)',
                              (identifier,canonical(value).decode('ascii'))).rowcount
        # An existing key must identify exactly the original canonical record.
        actual=store.db.execute('SELECT payload FROM '+TABLE+' WHERE id=?',(identifier,)).fetchone()[0]
        if actual != canonical(value).decode('ascii'):
            raise ValueError('candidate selection record conflicts with its identity')
        store.task.check_memory()
    return {'selection_id':identifier,'created':bool(added),'selection':value,
            'candidate_ids_copied':0,'complete_global_census':False}


def _manifest(store, identifier, config_sha256):
    if not isinstance(identifier,str) or not re.fullmatch('[0-9a-f]{64}',identifier):
        raise ValueError('invalid candidate selection identity')
    if not store.db.execute("SELECT 1 FROM sqlite_master WHERE name=? AND type='table'",(TABLE,)).fetchone():
        raise ValueError('candidate selection does not exist')
    size=store.db.execute('SELECT length(payload) FROM '+TABLE+' WHERE id=?',(identifier,)).fetchone()
    if not size or not 0 < size[0] <= 16384:
        raise ValueError('candidate selection is missing or exceeds its bound')
    text=store.db.execute('SELECT payload FROM '+TABLE+' WHERE id=?',(identifier,)).fetchone()[0]
    value=json.loads(text)
    if (not isinstance(value,dict) or set(value)!={'schema_version','config_sha256','max_candidate_rowid','sources'}
            or value['schema_version']!=SCHEMA or value['config_sha256']!=config_sha256
            or canonical(value).decode('ascii')!=text or digest(value)!=identifier):
        raise ValueError('candidate selection content or frozen configuration changed')
    candidate_rowid_filter(value['max_candidate_rowid'])
    if value['max_candidate_rowid'] is None:
        raise ValueError('candidate selection requires a finite boundary')
    return value


def verify(store, identifier):
    """Verify the stored manifest and unchanged prefix before using its boundary."""
    store.task.require_frozen()
    value=_manifest(store,identifier,digest(store.task.config))
    progress=_progress(store,value['max_candidate_rowid'])
    if _sources(progress)!=value['sources']:
        raise ValueError('frozen candidate identity or provenance changed')
    return value, progress


def rebind_storage_roots(store, identifier, previous_freeze):
    """Inherit exactly one stored prefix after a settled resource-root transition.

    The caller retains the old source freeze and owns the existing writer lock.
    This changes neither source policy nor resource allowances. It is not the
    multi-file configuration transition itself. Retry with the same predecessor
    and freeze; both immutable selection records remain available for recovery.
    """
    store.task.require_frozen()
    if store.db.in_transaction:
        raise ValueError('candidate rebind requires a settled transaction')
    if (not isinstance(previous_freeze,dict)
            or set(previous_freeze)!={'schema_version','config','config_sha256'}
            or previous_freeze['schema_version']!='gapi-source-freeze/v1'
            or not isinstance(previous_freeze['config'],dict)
            or digest(previous_freeze['config'])!=previous_freeze['config_sha256']):
        raise ValueError('previous source freeze is invalid')
    old,new=previous_freeze['config'],store.task.config
    allowed={'additional_storage_roots','resource_freeze_sha256'}
    if ({k:v for k,v in old.items() if k not in allowed}
            !={k:v for k,v in new.items() if k not in allowed}):
        raise ValueError('candidate rebind permits only storage roots and owner pin changes')
    roots=store.task.additional_storage_roots
    if not set(roots(old)).issubset(roots(new)):
        raise ValueError('candidate rebind cannot remove accounted storage roots')
    with store.member('candidate-selection','storage-root-rebind',max_growth_bytes=8*1024**2):
        previous=_manifest(store,identifier,previous_freeze['config_sha256'])
        progress=_progress(store,previous['max_candidate_rowid'])
        if _sources(progress)!=previous['sources']:
            raise ValueError('frozen candidate identity or provenance changed')
        value={**previous,'config_sha256':digest(new)}
        successor=digest(value)
        added=store.db.execute('INSERT OR IGNORE INTO '+TABLE+' VALUES(?,?)',
                              (successor,canonical(value).decode('ascii'))).rowcount
        actual=_manifest(store,successor,digest(new))
        if actual!=value:
            raise ValueError('candidate selection record conflicts with its identity')
        store.task.check_memory()
    return {'selection_id':successor,'previous_selection_id':identifier,
            'created':bool(added),'selection':value,'candidate_ids_copied':0,
            'complete_global_census':False}


def processing(store, identifier):
    value,progress=verify(store,identifier)
    progress={**progress,'sources':[{**row,
        'processed_fraction':row['processed']/row['candidates'] if row['candidates'] else None,
        'observed_fraction':row['observed_members']/row['candidates'] if row['candidates'] else None}
        for row in progress['sources']]}
    return {**progress,'scope':'frozen-candidate-selection','selection_id':identifier,
            'max_candidate_rowid':value['max_candidate_rowid'],
            'processing_complete':all(row['pending']==0 and row['rejected_members']==0
                                      for row in progress['sources'])}
