"""Single owner for the bounded task configuration, storage and source freeze."""
from __future__ import annotations

import hashlib
import json
import os
import shutil
from pathlib import Path
from typing import Any

from .storage import project_root, require_external_cache


def canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


class BudgetStop(RuntimeError):
    pass


class Task:
    def __init__(self, path: Path):
        self.config = json.loads(path.read_text(encoding="utf-8"))
        c = self.config
        if c.get("schema_version") != "gapi-task/v1":
            raise ValueError("unsupported task contract")
        self.root = require_external_cache(Path(c["work_dir"]))
        if os.name == "nt" and self.root.drive.upper() != "D:":
            raise ValueError("this task must use the D drive")
        if not 0 < c["budget_bytes"] <= 2 * 1024**3:
            raise ValueError("budget must be within the authorized 2 GiB")
        if not 0 < c["reserve_bytes"] < c["budget_bytes"]:
            raise ValueError("invalid journal/export reserve")
        if not 0 < c["batch_events"] <= 1000 or not 0 < c["memory_bytes"] <= 512 * 1024**2:
            raise ValueError("batch or memory budget exceeds contract")
        if not 0 < c["max_member_bytes"] <= 16 * 1024**2 or not 0 < c["max_events_per_member"] <= 4096:
            raise ValueError("member limits exceed contract")
        if c["http_timeout"] <= 0 or not 0 <= c["max_retries"] <= 5 or c["request_interval"] < 1:
            raise ValueError("unsafe HTTP limits")
        self.root.mkdir(parents=True, exist_ok=True)
        self.check()

    def activate_runtime(self):
        """CLI-scoped temporary settings, restored before returning to the caller."""
        import tempfile
        self.old_temp = ({k:os.environ.get(k) for k in ("TMP", "TEMP", "TMPDIR")}, tempfile.tempdir)
        for key in ("TMP", "TEMP", "TMPDIR"):
            os.environ[key] = str(self.root)
        tempfile.tempdir = str(self.root)

    def restore_runtime(self):
        import tempfile
        previous, tempdir = self.old_temp
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        tempfile.tempdir = tempdir

    def used(self) -> int:
        total = 0
        for folder, dirs, files in os.walk(self.root, followlinks=False):
            for name in [*dirs, *files]:
                path = Path(folder) / name
                if path.is_symlink() or getattr(path, "is_junction", lambda: False)():
                    raise ValueError("links are forbidden in the task storage tree")
            total += sum((Path(folder) / name).stat().st_size for name in files)
        # Count new release/build outputs too; existing real input snapshots are not copied.
        for directory in (project_root() / "outputs", project_root() / "dist"):
            if directory.exists():
                total += sum(p.stat().st_size for p in directory.rglob("*") if p.is_file())
        return total

    def check(self, anticipated: int = 0) -> int:
        used = self.used()
        needed = self.config["reserve_bytes"] + anticipated
        if used + needed > self.config["budget_bytes"] or shutil.disk_usage(self.root).free < needed:
            raise BudgetStop(f"storage admission stopped: used={used}, anticipated={anticipated}, reserve={needed-anticipated}")
        return used

    def check_memory(self):
        if os.name == "nt":
            import ctypes
            from ctypes import wintypes
            class Counters(ctypes.Structure):
                _fields_ = [("cb",wintypes.DWORD),("PageFaultCount",wintypes.DWORD)] + [(n,ctypes.c_size_t) for n in ("PeakWorkingSetSize","WorkingSetSize","QuotaPeakPagedPoolUsage","QuotaPagedPoolUsage","QuotaPeakNonPagedPoolUsage","QuotaNonPagedPoolUsage","PagefileUsage","PeakPagefileUsage")]
            c = Counters()
            c.cb = ctypes.sizeof(c)
            kernel = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel.GetCurrentProcess.restype = wintypes.HANDLE
            measure = ctypes.WinDLL("psapi").GetProcessMemoryInfo
            measure.argtypes = [wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD]
            measure.restype = wintypes.BOOL
            if not measure(kernel.GetCurrentProcess(),ctypes.byref(c),ctypes.sizeof(c)):
                raise RuntimeError("memory sampling unavailable; expansion stopped")
            if c.WorkingSetSize > self.config["memory_bytes"]:
                raise BudgetStop("process working set exceeds parser memory budget")
            return c.WorkingSetSize
        # Linux tests/CI: resident pages, without another dependency.
        statm = Path("/proc/self/statm")
        if statm.exists():
            size = int(statm.read_text().split()[1]) * os.sysconf("SC_PAGE_SIZE")
            if size > self.config["memory_bytes"]:
                raise BudgetStop("process resident memory exceeds parser budget")
            return size
        raise RuntimeError("memory sampling unavailable on this platform")

    def freeze(self) -> dict:
        payload = {"schema_version": "gapi-source-freeze/v1", "config": self.config,
                   "config_sha256": digest(self.config)}
        path = self.root / "sources.lock.json"
        if path.exists():
            if path.read_bytes() != canonical(payload):
                raise ValueError("source/config changed: use a separately budgeted task directory; refusing mixed resume")
        else:
            self.check(len(canonical(payload)) * 2)
            path.write_bytes(canonical(payload))
        return payload

    def require_frozen(self) -> dict:
        path = self.root / "sources.lock.json"
        if not path.exists() or json.loads(path.read_bytes())["config_sha256"] != digest(self.config):
            raise ValueError("run sources freeze first; configuration must match the frozen source registry")
        return json.loads(path.read_bytes())
