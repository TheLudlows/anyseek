"""Timestamped evidence from an owned, loopback-only AnySeek process."""

from contextlib import contextmanager
import http.client
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import threading
import time

from shared_record_http import LocalServer, MASTER, SECRET


TENANT = "fixture237"
NS_PER_MS = 1_000_000
SYSCALL = re.compile(r"^(\d+)\.(\d+) (.+?)\s+<([0-9.]+)>$")
PHASE = re.compile(r"record_maintenance phase=(\w+) seq=(\d+) event=(enter|exit)")
WORKER = re.compile(r"record_maintenance event=worker_finished outcome=(\w+) seq=(\d+)")
WORKER_STARTED = re.compile(r"record_maintenance event=worker_started seq=(\d+)")
SCHEDULE = re.compile(r'record_maintenance event=scheduled seq=(\d+) cause=Some\(\\"([^"\\]+)\\"\)')
BACKPRESSURE = re.compile(r"record_maintenance event=backpressure cause=(\w+) seq=(\d+)")
RESOURCE_ADMISSION = re.compile(r"resource (subject|global|collection) admission exhausted")
PHASES = {"anchor_prepare", "cache_merge_capture_locked", "cache_merge_build_unlocked",
          "cache_merge_install_locked", "cache_merge_cleanup_locked"}


def time_ns(seconds):
    whole, _, fraction = seconds.partition(".")
    return int(whole) * 1_000_000_000 + int(fraction.ljust(9, "0"))


def server_pids(binary, data):
    # A managed executor can mount its host's /proc. Read NSpid for
    # signaling in our namespace and the host PID for resource samples.
    wanted = os.fsencode(f"index_path={data}")
    matches = []
    for proc in Path("/proc").glob("[0-9]*"):
        try:
            args = (proc / "cmdline").read_bytes().split(b"\0")
            if args[0] != os.fsencode(binary) or wanted not in args:
                continue
            status = (proc / "status").read_text()
            namespace = re.search(r"^NSpid:\s+(.*)$", status, re.MULTILINE)
            signaled = int(namespace[1].split()[-1]) if namespace else int(proc.name)
            matches.append((int(proc.name), signaled))
        except (OSError, ValueError):
            continue
    if len(matches) != 1:
        raise RuntimeError(f"could not uniquely identify owned traced server: {matches}")
    return matches[0]


class TracedServer(LocalServer):
    tenant = TENANT

    def __init__(self, binary, folder, cpus):
        self.local = threading.local()
        temporary = folder / "tmp"
        temporary.mkdir(exist_ok=True)
        env = {name: os.environ[name] for name in ("PATH", "LANG", "TZ") if name in os.environ}
        env.update(TMPDIR=str(temporary), MASTER_KEY_SECRET=SECRET, ANYSEEK_STORAGE_BACKEND="local",
                   TOKIO_WORKER_THREADS="2", RAYON_NUM_THREADS="2",
                   RUST_LOG="anyseek::record::maintenance=debug,anyseek::resources=warn")
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        self.proc_pid = self.signal_pid = None
        with (folder / "server.log").open("ab") as log:
            self.p = subprocess.Popen(
                ["strace", "--kill-on-exit", "-ff", "-ttt", "-T", "-yy", "-s", "768",
                 "-e", "trace=write,writev,rename,renameat,renameat2,unlink,unlinkat,rmdir",
                 "-o", str(folder / "syscalls"), "taskset", "-c", cpus, str(binary),
                 f"index_path={folder / 'live'}", f"local_port={self.port}", "local_ip=127.0.0.1"],
                env=env, stdout=log, stderr=log, start_new_session=True)
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline and self.p.poll() is None:
                try:
                    self.call("GET", "/api/v1/live", key=MASTER)
                except (OSError, http.client.HTTPException):
                    time.sleep(.05)
                else:
                    self.proc_pid, self.signal_pid = server_pids(binary, folder / "live")
                    return
            raise RuntimeError(f"traced server did not start; see {folder / 'server.log'}")
        except BaseException:
            self.close()
            raise

    def close(self, crash=False):
        if self.p.poll() is None:
            if self.signal_pid is None:
                os.killpg(self.p.pid, signal.SIGKILL)
            else:
                os.kill(self.signal_pid, signal.SIGKILL if crash else signal.SIGTERM)
            try:
                self.p.wait(timeout=20)
            except subprocess.TimeoutExpired:
                os.killpg(self.p.pid, signal.SIGKILL)
                self.p.wait(timeout=5)
                raise
        expected = -signal.SIGKILL if crash else 0
        if self.signal_pid is not None and self.p.returncode != expected:
            raise RuntimeError(f"traced server exited {self.p.returncode}, expected {expected}")


@contextmanager
def resources(server, folder):
    samples = []
    stop = threading.Event()

    def sample():
        while not stop.is_set():
            status = Path(f"/proc/{server.proc_pid}/status").read_text()
            memory = {key: int(re.search(rf"^{key}:\s+(\d+)", status, re.MULTILINE)[1]) * 1024
                      for key in ("VmRSS", "VmHWM")}
            disk = {}
            for name in ("live", "tmp"):
                disk[name] = 0
                for root, dirs, files in os.walk(folder / name):
                    for file in dirs + files:
                        try:
                            disk[name] += (Path(root) / file).stat().st_blocks * 512
                        except FileNotFoundError:
                            continue
            samples.append({"wall_ns": time.time_ns(), "memory_bytes": memory, "allocated_bytes": disk})
            stop.wait(.25)

    thread = threading.Thread(target=sample)
    thread.start()
    try:
        yield samples
    finally:
        stop.set()
        thread.join()


def read_trace(folder, old_generation, current_generation, after_ns):
    events = []
    retired = re.compile(rf"/hot/\.retired-(?:retry-)?{old_generation}(?:[/\">])")
    cache = re.compile(rf"/hot/{current_generation}/cache-[0-9a-f]{{32}}/")
    for path in sorted(folder.glob("syscalls.*")):
        with path.open(errors="replace") as stream:
            for line_number, line in enumerate(stream, 1):
                if not any(word in line for word in ("record_maintenance", "admission exhausted", ".retired-", "cache-")):
                    continue
                match = SYSCALL.match(line)
                if match is None:
                    continue
                seconds, fraction, syscall, duration = match.groups()
                start = time_ns(f"{seconds}.{fraction}")
                if start < after_ns:
                    continue
                event = {"start_ns": start, "end_ns": start + time_ns(duration),
                         "source": f"{path.name}:{line_number}"}
                phase = PHASE.search(syscall)
                worker = WORKER.search(syscall)
                worker_started = WORKER_STARTED.search(syscall)
                scheduled = SCHEDULE.search(syscall)
                pressure = BACKPRESSURE.search(syscall)
                admission = RESOURCE_ADMISSION.search(syscall)
                if phase and phase[1] in PHASES:
                    event.update(kind=phase[1], seq=int(phase[2]), phase=phase[3], thread=path.suffix)
                elif worker:
                    event.update(kind="worker_finished", seq=int(worker[2]), outcome=worker[1])
                elif worker_started:
                    event.update(kind="worker_started", seq=int(worker_started[1]))
                elif scheduled:
                    event.update(kind="scheduled", seq=int(scheduled[1]), cause=scheduled[2])
                elif pressure:
                    event.update(kind="backpressure", seq=int(pressure[2]), cause=pressure[1])
                elif admission:
                    event.update(kind="resource_admission", scope=admission[1])
                elif retired.search(syscall) and syscall.endswith(" = 0"):
                    if syscall.startswith(("unlink(", "unlinkat(", "rmdir(")):
                        event["kind"] = "retired_remove"
                    elif syscall.startswith(("rename(", "renameat(", "renameat2(")):
                        event["kind"] = "retired_rename"
                elif cache.search(syscall) and syscall.startswith(("write(", "writev(")) and re.search(r" = [1-9]\d*$", syscall):
                    event["kind"] = "hot_cache_write"
                if "kind" in event:
                    events.append(event)
    return sorted(events, key=lambda event: event["start_ns"])


def phase_windows(events):
    opened = {}
    windows = []
    for event in events:
        if "phase" not in event:
            continue
        key = event["kind"], event["seq"]
        if event["phase"] == "enter":
            if key in opened:
                raise RuntimeError(f"ambiguous maintenance phase: {key}")
            opened[key] = event
        elif (start := opened.pop(key, None)) is not None:
            # Exclude logging syscalls from the phase's interior.
            windows.append({"kind": event["kind"], "seq": event["seq"],
                            "start_ns": start["end_ns"], "end_ns": event["start_ns"],
                            "sources": [start["source"], event["source"]]})
    return sorted(windows, key=lambda window: window["start_ns"])
