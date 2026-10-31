"""Compare candidate progress with a pinned predecessor on one RAM snapshot.

Reads no raw source bodies, creates no durable database and publishes aggregates
only. The predecessor method is extracted from the explicit local Git revision.
"""
import argparse
import ast
import hashlib
import json
from pathlib import Path
import subprocess
import time
import sqlite3
import sys
from datetime import datetime, timezone
from unittest.mock import patch

from gapi.career_store import CareerStore, CANDIDATE_SOURCES
from gapi.monitoring import snapshot_report
from gapi.task import Task, canonical


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--baseline',default='8c2636d')
    parser.add_argument('--task',type=Path,default=Path('configs/streaming.json'))
    parser.add_argument('--per-source',type=int,default=1000)
    parser.add_argument('--out',type=Path,required=True)
    args=parser.parse_args()
    if not 1<=args.per_source<=2000:
        parser.error('bounded differential sample must contain 1..2000 candidates per source')
    revision=subprocess.check_output(['git','rev-parse','--verify',args.baseline+'^{commit}'],text=True).strip()
    source=subprocess.check_output(['git','show',revision+':src/gapi/career_store.py'],text=True)
    cls=next(n for n in ast.parse(source).body if isinstance(n,ast.ClassDef) and n.name=='CareerStore')
    method=next(n for n in cls.body if isinstance(n,ast.FunctionDef) and n.name=='candidate_progress')
    tree=ast.fix_missing_locations(ast.Module(body=[method],type_ignores=[]))
    namespace=dict(hashlib=hashlib,canonical=canonical,CANDIDATE_SOURCES=CANDIDATE_SOURCES)
    exec(compile(tree,'pinned-local-predecessor','exec'),namespace)
    legacy=namespace['candidate_progress']
    current=CareerStore.candidate_progress
    def measured(call,view):
        started=time.perf_counter();result=call(view)
        return result,time.perf_counter()-started
    def compare(view):
        full,full_seconds=measured(current,view)
        # Mutate only the in-memory snapshot after the full current calculation.
        for command in CANDIDATE_SOURCES:
            view.db.execute('''DELETE FROM discovery WHERE source=? AND identifier NOT IN
                (SELECT identifier FROM discovery WHERE source=? ORDER BY identifier LIMIT ?)''',
                (command,command,args.per_source))
        view.db.commit()
        expected,old_seconds=measured(legacy,view)
        actual,new_seconds=measured(current,view)
        repeat,_=measured(current,view)
        if expected!=actual or actual!=repeat:
            raise RuntimeError('same-snapshot progress output changed from pinned predecessor')
        return dict(checked_at=datetime.now(timezone.utc).isoformat(),
            python_version=sys.version.split()[0],sqlite_version=sqlite3.sqlite_version,
            baseline_revision=revision,scope='same-RAM-snapshot candidate progress; no ranking change',
            full_current_candidates=sum(r['candidates'] for r in full['sources']),
            full_current_seconds=full_seconds,full_current_progress_sha256=hashlib.sha256(canonical(full)).hexdigest(),
            compared_candidates=sum(r['candidates'] for r in actual['sources']),
            old_seconds=old_seconds,new_seconds=new_seconds,measured_speedup=old_seconds/new_seconds,
            equal_aggregate_and_selection_hash=True,equal_repeat=True,
            compared_output_sha256=hashlib.sha256(canonical(actual)).hexdigest(),
            raw_files_written=0,durable_database_copies=0)
    task=Task(args.task)
    with patch.object(CareerStore,'report',compare):
        result=snapshot_report(task,max_seconds=60)
    args.out.write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(result,indent=2))


if __name__=='__main__':main()
