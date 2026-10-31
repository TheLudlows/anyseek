#!/usr/bin/env python3
"""Run an explicit small cross-platform regression set; reject missing tests."""
from contextlib import nullcontext
import json
import os
from pathlib import Path
import subprocess
import tempfile


def main():
    root = Path(__file__).resolve().parents[1]
    names = json.loads((root / "scripts/ci_core_cases.json").read_text())
    assert names and len(names) == len(set(names))
    # One feature/profile build, then invoke its test binary without more cargo builds.
    with tempfile.TemporaryFile(mode="w+t", encoding="utf-8") as output:
        subprocess.run(["cargo", "test", "-p", "anyseek", "--lib", "--locked",
                        "--features", "server", "--no-run", "--message-format=json"],
                       cwd=root, stdout=output, check=True)
        output.seek(0)
        binaries = []
        for line in output:
            item = json.loads(line)
            if (item.get("reason") == "compiler-artifact" and item.get("executable")
                    and item.get("profile", {}).get("test")
                    and item.get("target", {}).get("name") == "anyseek"):
                binaries.append(item["executable"])
    assert len(binaries) == 1, binaries
    binary = binaries[0]
    listing = subprocess.check_output([binary, "--list"], text=True, cwd=root)
    available = {line.removesuffix(": test") for line in listing.splitlines()
                 if line.endswith(": test")}
    assert set(names) <= available, sorted(set(names) - available)
    for name in names:
        print(f"START {name}", flush=True)
        trace_case = name == "record::maintenance::tests::receipt_gc_remote_checkpoint_cold_recovery_preserves_time_prefix"
        with (tempfile.TemporaryDirectory(prefix="anyseek-maintenance-trace-") if trace_case else nullcontext()) as directory:
            trace = Path(directory) / "phases.log" if directory else None
            env = os.environ.copy() if trace else None
            if trace:
                env["ANYSEEK_TEST_PHASE_TIMINGS"] = str(trace)
            result = subprocess.run(
                [binary, name, "--exact", "--include-ignored", "--test-threads=1", "--nocapture"],
                cwd=root, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            print(result.stdout, flush=True)
            if trace and trace.exists():
                print(trace.read_text(encoding="utf-8"), flush=True)
        result.check_returncode()
        assert "test result: ok. 1 passed; 0 failed; 0 ignored;" in result.stdout, name
    print(f"PASS {len(names)} exact core cases", flush=True)


if __name__ == "__main__":
    main()
