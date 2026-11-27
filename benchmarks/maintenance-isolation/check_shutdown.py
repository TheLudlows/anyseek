#!/usr/bin/env python3
"""Deterministically interrupt the fixture while a private service is blocked."""

import argparse
import json
import os
from pathlib import Path
import select
import signal
import socket
import subprocess
import sys
import tempfile
import time
from unittest.mock import patch

import measure
import evidence


CASES = ("startup_exception", "blocked_request", "shutdown_interruptions")


def checkpoint(tracer, server):
    print(json.dumps([tracer, server]), flush=True)
    if sys.stdin.readline() != "continue\n":
        raise RuntimeError("lost test controller")


def service_pid(server):
    return evidence.server_pids(server.binary, server.folder / "live")[1]


def run_child(case, binary, folder, cpus):
    if case == "blocked_request":
        def seed_one(server, epoch, collection, _count, old=False):
            seed(server, epoch, collection, 1, old)

        def blocked_lane(server, *_):
            signal.pidfd_send_signal(server.server_fd, signal.SIGSTOP)
            with socket.create_connection(("127.0.0.1", server.port), timeout=20) as client:
                client.sendall(b"GET /api/v1/live HTTP/1.0\r\n\r\n")
                checkpoint(server.p.pid, service_pid(server))
                # The stopped server can release this read only by being closed.
                client.recv(1)

        seed = measure.seed
        with patch.object(measure, "seed", seed_one), patch.object(measure, "run_lane", blocked_lane):
            measure.run_case(binary, folder, cpus, int(time.time() // 86400),
                             "control", [[]])
        return

    folder.mkdir()
    server = evidence.TracedServer(binary, folder, cpus)
    if case == "startup_exception":
        identify = evidence.server_pids

        def fail_after_ready(*args):
            _, pid = identify(*args)
            checkpoint(server.p.pid, pid)
            raise RuntimeError("startup regression sentinel")

        with patch.object(evidence, "server_pids", fail_after_ready), server:
            raise AssertionError("unexpected startup success")
        return

    with server:
        pid = service_pid(server)
        signal.pidfd_send_signal(server.server_fd, signal.SIGSTOP)
        graceful_signal = signal.pidfd_send_signal
        wait = server.p.wait

        def mark_graceful(fd, signum, *args):
            graceful_signal(fd, signum, *args)
            if signum == signal.SIGTERM:
                checkpoint(server.p.pid, pid)

        def mark_forced_wait(*args, **kwargs):
            checkpoint(server.p.pid, pid)
            return wait(*args, **kwargs)

        # The handler must defer a SIGINT both during graceful shutdown and
        # during the final reap, regardless of how fast the tracer exits.
        with patch.object(signal, "pidfd_send_signal", mark_graceful), patch.object(server.p, "wait", mark_forced_wait):
            server.close()


def child_checkpoint(child):
    if not select.select([child.stdout], [], [], 8)[0]:
        raise TimeoutError("child did not reach its shutdown checkpoint")
    return json.loads(child.stdout.readline())


def verify(case, binary, folder, cpus, sibling):
    with (folder.parent / f"{case}.log").open("w+") as log:
        child = subprocess.Popen([sys.executable, __file__, "--server", str(binary), "--cpus", cpus,
                                  "--child", case, str(folder)],
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log,
                                 start_new_session=True)
        tracer = None
        handles = []
        try:
            tracer, service = child_checkpoint(child)
            handles.append(os.pidfd_open(tracer))
            handles.append(os.pidfd_open(service))
            if case != "startup_exception":
                child.send_signal(signal.SIGINT)
            child.stdin.write(b"continue\n")
            child.stdin.flush()
            if case == "shutdown_interruptions":
                if child_checkpoint(child) != [tracer, service]:
                    raise AssertionError("shutdown switched process identity")
                child.send_signal(signal.SIGINT)
                child.stdin.write(b"continue\n")
                child.stdin.flush()
            expected = 1 if case == "startup_exception" else -signal.SIGINT
            status = child.wait(timeout=8)
            log.seek(0)
            if status != expected or case == "startup_exception" and "startup regression sentinel" not in log.read():
                raise AssertionError(f"{case}: expected exit {expected}, got {status}")
            if len(select.select(handles, [], [], 0)[0]) != len(handles):
                raise AssertionError(f"{case}: tracer or service still running after runner exit")
            sibling.call("GET", "/api/v1/live")
            print(f"{case}: exit {status}, owned tracer and service exited, sibling HTTP live", flush=True)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=5)
            # Test-only fallback if the regression is present. Pidfds still
            # identify only the processes handed off by this child.
            for fd in handles:
                try:
                    signal.pidfd_send_signal(fd, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                finally:
                    os.close(fd)
            child.stdin.close()
            child.stdout.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", required=True, type=Path)
    parser.add_argument("--cpus", required=True)
    parser.add_argument("--child", nargs=2, help=argparse.SUPPRESS)
    args = parser.parse_args()
    binary = args.server.resolve(strict=True)
    if args.child:
        case, folder = args.child
        run_child(case, binary, Path(folder), args.cpus)
        return

    with tempfile.TemporaryDirectory(prefix="anyseek-237-shutdown-") as temporary:
        root = Path(temporary)
        sibling = root / "sibling"
        sibling.mkdir()
        with evidence.TracedServer(binary, sibling, args.cpus) as live:
            for name in CASES:
                verify(name, binary, root / name, args.cpus, live)


if __name__ == "__main__":
    main()
