#!/usr/bin/env python3
"""Run exact write-path regressions in isolated processes, including fault cuts."""
import json
import os
from pathlib import Path
import subprocess
import tempfile


def main():
    root = Path(__file__).resolve().parents[1]
    names = json.loads((root / "scripts/ci_write_path_cases.json").read_text())
    assert names and len(names) == len(set(names))
    with tempfile.TemporaryFile(mode="w+t", encoding="utf-8") as output:
        build = subprocess.run(["cargo", "test", "-j", "1", "-p", "anyseek", "--lib", "--locked",
                        "--features", "fault-injection", "--no-run", "--message-format=json"],
                       cwd=root, stdout=output)
        if build.returncode:
            output.seek(0)
            for line in output:
                item = json.loads(line)
                if item.get("reason") == "compiler-message":
                    print(item["message"].get("rendered", item["message"]["message"]), flush=True)
            build.check_returncode()
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
        env = os.environ.copy()
        if "small_shared_budget" in name:
            env.update(ANYSEEK_BUILD_MEMORY_BYTES="67108864",
                       ANYSEEK_BUILD_SHARED_MEMORY_BYTES="67108864")
        result = subprocess.run(
            [binary, name, "--exact", "--include-ignored", "--test-threads=1", "--nocapture"],
            cwd=root, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, timeout=180)
        print(result.stdout, flush=True)
        result.check_returncode()
        assert "test result: ok. 1 passed; 0 failed; 0 ignored;" in result.stdout, name
    print(f"PASS {len(names)} exact write-path cases", flush=True)


if __name__ == "__main__":
    main()
