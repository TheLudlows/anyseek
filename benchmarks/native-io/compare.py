#!/usr/bin/env python3
"""Alternate prebuilt diagnostic binaries; never build during measurements."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    output = args.output.resolve()
    binaries = {name: path.resolve() for name, path in
                (("baseline", args.baseline), ("candidate", args.candidate))}
    name = "record::lexical::directory::diagnostic::directory_io_contention"
    provenance = {label: {"binary": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
                  for label, path in binaries.items()}
    (output / "binaries.json").write_text(json.dumps(provenance, indent=2) + "\n")
    for pair in range(5):
        for workers in (1, 4):
            order = ("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")
            for label in order:
                env = dict(os.environ, ANYSEEK_IO_WORKERS=str(workers), TMPDIR=str(output))
                result = subprocess.run(
                    [str(binaries[label]), name, "--exact", "--ignored", "--nocapture"],
                    env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                (output / f"paired-{label}-w{workers}-{pair}.log").write_text(result.stdout)
                result.check_returncode()
                if "test result: ok. 1 passed" not in result.stdout:
                    raise RuntimeError("diagnostic did not execute")
    # strace changes timing: keep these runs separate from the comparison above.
    for label, binary in binaries.items():
        env = dict(os.environ, ANYSEEK_IO_WORKERS="4", TMPDIR=str(output))
        result = subprocess.run(
            ["strace", "-f", "-w", "-c", "-e", "trace=fsync,fdatasync", "-o",
             str(output / f"{label}-sync-wall.txt"), str(binary), name,
             "--exact", "--ignored", "--nocapture"], env=env, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        (output / f"{label}-traced-wall.log").write_text(result.stdout)
        result.check_returncode()


if __name__ == "__main__":
    main()
