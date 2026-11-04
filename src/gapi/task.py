"""Single owner for the bounded task configuration, storage and source freeze."""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
from pathlib import Path
from typing import Any

from .storage import project_root, require_external_cache

_WINDOWS_MEMORY_API = None
MAX_STORAGE_BYTES = 32_000_000_000
MIN_RESERVE_BYTES = 256 * 1024**2


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
        self._resource_root = self.root
        if c.get('resource_root') is not None:
            self._resource_root = require_external_cache(Path(c['resource_root']))
            if self.resource_root == self.root or not self.root.is_relative_to(self.resource_root):
                raise ValueError('shared resource root must contain the separate task directory')
            self.require_resource_contract()
        if os.name == "nt" and self.root.drive.upper() != "D:":
            raise ValueError("this task must use the D drive")
        if type(c["budget_bytes"]) is not int or not 0 < c["budget_bytes"] <= MAX_STORAGE_BYTES:
            raise ValueError("budget must be within the authorized 32 GB")
        if (type(c["reserve_bytes"]) is not int
                or not MIN_RESERVE_BYTES <= c["reserve_bytes"] < c["budget_bytes"]):
            raise ValueError("invalid journal/export reserve")
        if not 0 < c["batch_events"] <= 1000 or not 0 < c["memory_bytes"] <= 512 * 1024**2:
            raise ValueError("batch or memory budget exceeds contract")
        if not 0 < c["max_member_bytes"] <= 16 * 1024**2 or not 0 < c["max_events_per_member"] <= 4096:
            raise ValueError("member limits exceed contract")
        if c["http_timeout"] <= 0 or not 0 <= c["max_retries"] <= 5 or c["request_interval"] < 1:
            raise ValueError("unsafe HTTP limits")
        self.root.mkdir(parents=True, exist_ok=True)
        self.check()

    @property
    def resource_root(self):
        return getattr(self, '_resource_root', self.root)

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
        def tree_size(root, *, strict):
            total = 0
            pending = [root]
            while pending:
                folder = pending.pop()
                try:
                    with os.scandir(folder) as entries:
                        for entry in entries:
                            try:
                                info = entry.stat(follow_symlinks=False)
                                linked = stat.S_ISLNK(info.st_mode) or bool(
                                    getattr(info, "st_file_attributes", 0) & 0x400)
                                if strict and linked:
                                    raise ValueError("links are forbidden in the task storage tree")
                                if linked:
                                    # Release file links retain their previous target-size
                                    # accounting. Never traverse linked directories.
                                    target = entry.stat(follow_symlinks=True)
                                    if stat.S_ISREG(target.st_mode):total += target.st_size
                                elif stat.S_ISDIR(info.st_mode):pending.append(entry.path)
                                elif stat.S_ISREG(info.st_mode):total += info.st_size
                            except FileNotFoundError:
                                # A committed journal can vanish during observation.
                                # Permission and other observation failures escape.
                                continue
                except FileNotFoundError:
                    continue
            return total
        # No cache: every admission still scans the complete owned trees. DirEntry
        # reuses OS enumeration metadata instead of probing each path repeatedly.
        total = tree_size(self.resource_root, strict=True)
        # Count new release/build outputs too; existing real input snapshots are not copied.
        for directory in (project_root() / "outputs", project_root() / "dist", project_root() / "work"):
            total += tree_size(directory, strict=False)
        return total

    def check(self, anticipated: int = 0, *, minimum_used: int = 0) -> int:
        self.require_resource_contract()
        # Windows directory enumeration may retain a writer's pre-open size.
        # Streaming writers supply their initial usage plus bytes written as
        # a floor; fresh enumeration still accounts for all other artifacts.
        used = max(self.used(), minimum_used)
        needed = self.config["reserve_bytes"] + anticipated
        if used + needed > self.config["budget_bytes"] or shutil.disk_usage(self.root).free < needed:
            raise BudgetStop(f"storage admission stopped: used={used}, anticipated={anticipated}, reserve={needed-anticipated}")
        return used

    def require_resource_contract(self):
        """A nested source task shares its frozen owner's budget and writer lock.

        A separate source database is not a new allowance. Pin the existing
        resource owner's lock, including all sibling files in every admission.
        """
        if self.resource_root == self.root:
            return
        raw = (self.resource_root/'sources.lock.json').read_bytes()
        if hashlib.sha256(raw).hexdigest() != self.config.get('resource_freeze_sha256'):
            raise ValueError('shared resource owner freeze changed or is not pinned')
        frozen = json.loads(raw)
        owner = frozen['config']
        if (frozen.get('schema_version') != 'gapi-source-freeze/v1'
                or frozen.get('config_sha256') != digest(owner)
                or owner.get('resource_root') is not None
                or Path(owner['work_dir']).resolve() != self.resource_root
                or any(self.config[k] != owner[k] for k in ('budget_bytes','reserve_bytes','memory_bytes'))):
            raise ValueError('shared resource contract differs from its frozen owner')

    def available_growth(self, requested: int) -> int:
        """Admit enforceable SQLite growth while preserving reserve and 1 MiB.

        The caller must enforce the returned allowance with max_page_count;
        this is not permission for an unbounded writer to use less reservation.
        Commit checks still include journals and concurrent owned outputs.
        """
        if type(requested) is not int or requested < 0:
            raise ValueError('invalid growth request')
        minimum=1024**2
        used=self.check(minimum)
        remaining=min(self.config['budget_bytes']-self.config['reserve_bytes']-used,
                      shutil.disk_usage(self.root).free-self.config['reserve_bytes'])
        return min(requested,max(0,remaining-minimum))

    def check_memory(self):
        if os.name == "nt":
            import ctypes
            from ctypes import wintypes
            global _WINDOWS_MEMORY_API
            if _WINDOWS_MEMORY_API is None:
                class Counters(ctypes.Structure):
                    _fields_ = [("cb",wintypes.DWORD),("PageFaultCount",wintypes.DWORD)] + [(n,ctypes.c_size_t) for n in ("PeakWorkingSetSize","WorkingSetSize","QuotaPeakPagedPoolUsage","QuotaPagedPoolUsage","QuotaPeakNonPagedPoolUsage","QuotaNonPagedPoolUsage","PagefileUsage","PeakPagefileUsage")]
                kernel = ctypes.WinDLL("kernel32", use_last_error=True)
                kernel.GetCurrentProcess.restype = wintypes.HANDLE
                measure = ctypes.WinDLL("psapi").GetProcessMemoryInfo
                measure.argtypes = [wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD]
                measure.restype = wintypes.BOOL
                _WINDOWS_MEMORY_API = (Counters,kernel,measure)
            Counters,kernel,measure = _WINDOWS_MEMORY_API
            c = Counters()
            c.cb = ctypes.sizeof(c)
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
        self.require_resource_contract()
        path = self.root / "sources.lock.json"
        if not path.exists() or json.loads(path.read_bytes())["config_sha256"] != digest(self.config):
            raise ValueError("run sources freeze first; configuration must match the frozen source registry")
        return json.loads(path.read_bytes())
