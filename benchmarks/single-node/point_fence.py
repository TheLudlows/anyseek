"""Controlled point-read availability checks for the single-node HTTP harness."""

import gzip
import http.client
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from measure import (
    Evidence,
    activity,
    document,
    file_digest,
    load,
    mixed_workload,
    output_json,
    path_for,
    read,
    service,
)

CASES = {
    "read_only": (False, False),
    "writer_without_checkpoint": (True, False),
    "checkpoint_without_writer": (False, True),
    "checkpoint_with_writer": (True, True),
}
COUNTS = {"hot": 512, "peer": 512, "cold": 64}
ITERATIONS = 4


def progress(evidence, server, phase, collection):
    response, attempt = evidence.call(server, phase, "health_progress", "GET",
                                      path_for(collection) + "/ingest", detail=True)
    return {"status": attempt["status"], "start_s": attempt["start_s"], "end_s": attempt["end_s"],
            "recovery_required": response.get("recovery_required"), "response": response}


def read_again(evidence, server, phase, collection, index):
    identity = f"r{index:09d}"
    response, attempt = evidence.call(server, phase, "retry_point", "GET",
                                      f"{path_for(collection)}/doc/{identity}", detail=True)
    same = (response.get("record_id"), response.get("version"), response.get("document")) == (
        identity, 1, document(index)
    )
    if attempt["status"] == 200 and not same:
        raise RuntimeError(f"point retry changed identity, version, or document at {attempt['path']}")
    return {"status": attempt["status"], "start_s": attempt["start_s"], "end_s": attempt["end_s"],
            "code": response.get("code"), "same_record_version_and_body": same}


def overlaps(a, b):
    return a["start_s"] < b["end_s"] and b["start_s"] < a["end_s"]


def healthy(samples):
    return all(sample["status"] == 200 and sample["recovery_required"] is False
               and sample["response"].get("index_error") is None for sample in samples)


def scenario(binary, out, cpus, epoch, evidence, case, result):
    writer, maintenance = CASES[case]
    failures = []
    lock = threading.Lock()
    root = out / "live"
    with service(binary, root, out, "load_and_measure", cpus, evidence, result) as (server, phase):
        result["server_pid"] = server.p.pid
        for name, count in COUNTS.items():
            phase[0] = "load"
            load(evidence, server, name, count, epoch, result["load"])
            if name == "hot":
                read(evidence, server, "warmup", name, count, 0, ITERATIONS)
        phase[0] = "write_only"
        activity(evidence, server, epoch, ITERATIONS, "write_only")
        result["health_before"] = {name: progress(evidence, server, "before_mixed", name)
                                   for name in ("hot", "peer")}

        with ThreadPoolExecutor(max_workers=2) as observers:
            def observe(collection, index, attempt):
                # The progress route waits for Inner. Its response can sample
                # health after a short in-flight fence has already cleared.
                retry = read_again(evidence, server, "near_503", collection, index)
                return {"first_retry": retry, "health_near_503": progress(
                    evidence, server, "near_503", collection), "original": attempt}

            def point_failure(collection, index, attempt):
                observation = observers.submit(observe, collection, index, attempt)
                with lock:
                    failures.append((collection, index, observation))

            phase[0] = "mixed"
            mixed_workload(evidence, server, COUNTS, ITERATIONS, epoch, writer=writer,
                           maintenance=maintenance, point_failure=point_failure)

        rows = evidence.samples.get(("mixed", "point"), [])
        checkpoints = evidence.samples.get(("mixed", "hot_checkpoint"), []) + evidence.samples.get(
            ("mixed", "peer_checkpoint"), [])
        writes = [row for operation in ("group_replace", "async_write", "record_delete")
                  for row in evidence.samples.get(("mixed", operation), [])]
        result["point_http_status"] = dict(Counter(str(r["status"]) for r in rows))
        result["successful_point_reads"] = {
            name: sum(row["status"] == 200 and row["path"].startswith(path_for(name) + "/doc/")
                      for row in rows) for name in ("hot", "peer")
        }
        result["mixed_http_429"] = sum(row["status"] == 429
                                      for (p, _), calls in evidence.samples.items() if p == "mixed"
                                      for row in calls)
        result["checkpoint_windows"] = [
            {"request": check, "points_overlapped": sum(overlaps(row, check) for row in rows),
             "same_collection_points_overlapped": sum(
                 overlaps(row, check) and row["path"].startswith(check["path"] + "/doc/")
                 for row in rows)} for check in checkpoints
        ]
        for collection, index, observation in failures:
            failure = observation.result()
            failure["checkpoint_overlap"] = [check["operation"] for check in checkpoints
                                              if overlaps(failure["original"], check)]
            failure["write_overlap"] = sorted({write["operation"] for write in writes
                                               if overlaps(failure["original"], write)})
            failure["same_process_after_mixed"] = read_again(evidence, server, "after_mixed", collection, index)
            result["point_503"].append(failure)
        result["health_after"] = {name: progress(evidence, server, "after_mixed", name)
                                  for name in ("hot", "peer")}
        pending = [(collection, index, failure) for (collection, index, _), failure in zip(failures, result["point_503"], strict=True)
                   if not failure["same_process_after_mixed"]["same_record_version_and_body"]]
        if pending or not healthy(result["health_after"].values()):
            time.sleep(1)
            for collection, index, failure in pending:
                failure["same_process_after_quiet"] = read_again(evidence, server, "after_quiet", collection, index)
            pending = [(collection, index, failure) for collection, index, failure in pending
                       if not failure["same_process_after_quiet"]["same_record_version_and_body"]]
            result["health_after_quiet"] = {name: progress(evidence, server, "after_quiet", name)
                                            for name in ("hot", "peer")}
        need_reopen = bool(pending) or not healthy(result.get("health_after_quiet", result["health_after"]).values())

    result["reopened"] = need_reopen
    if need_reopen:
        with service(binary, root, out, "reopen_probe", cpus, evidence, result) as (server, _):
            result["reopen_pid"] = server.p.pid
            for collection, index, failure in pending:
                failure["after_reopen"] = read_again(evidence, server, "after_reopen", collection, index)
            result["health_after_reopen"] = {name: progress(evidence, server, "after_reopen", name)
                                             for name in ("hot", "peer")}


def run_probe(args, out, cpus, media, epoch):
    report = {
        "scope": "synthetic small-store smoke", "result": "running", "revision": args.revision,
        "binary_sha256": file_digest(args.binary), "cpus": cpus, "media": media,
        "counts": COUNTS, "iterations_per_reader": ITERATIONS, "rounds": args.probe_rounds,
        "harness_sha256": {name: file_digest(Path(__file__).with_name(name))
                           for name in ("measure.py", "point_fence.py")},
        "cases": {},
    }
    path = out / "report.json"
    output_json(path, report)
    usable = {}
    fail = False
    for run in range(1, args.probe_rounds + 1):
        for case, (_, maintenance) in CASES.items():
            name = f"{run:02d}-{case}"
            directory = out / name
            directory.mkdir()
            result = {"case": case, "run": run, "load": {}, "point_503": [],
                      "trace": f"{name}/requests.jsonl.gz", "server_log": f"{name}/load_and_measure.log"}
            report["cases"][name] = result
            try:
                with gzip.open(directory / "requests.jsonl.gz", "wt", encoding="utf-8") as raw:
                    evidence = Evidence(directory, raw)
                    result["request_clock_origin_unix_ns"] = time.time_ns()
                    scenario(args.binary, directory, cpus, epoch, evidence, case, result)
                checkpoint_covered = not maintenance or any(
                    row["request"]["status"] == 200 and row["same_collection_points_overlapped"] > 0
                    for row in result["checkpoint_windows"] if row["request"]["operation"] == "hot_checkpoint"
                )
                qualifies = checkpoint_covered and all(result["successful_point_reads"].values())
                usable[case] = usable.get(case, 0) + int(qualifies)
                fail |= result["reopened"] or any(
                    status not in {"200", "429"} for status in result["point_http_status"]
                )
            except (AssertionError, KeyError, OSError, RuntimeError, ValueError, http.client.HTTPException) as error:
                result["error"] = f"{type(error).__name__}: {error}"
            finally:
                output_json(path, report)
    incomplete = [case for case in CASES if not usable.get(case)]
    errors = sum("error" in result for result in report["cases"].values())
    report.update(usable_rounds=usable, cases_without_usable_rounds=incomplete, failed_cases=errors)
    report["result"] = "availability_failure" if fail else "incomplete" if incomplete or errors else "passed"
    output_json(path, report)
    return {"availability_failure": 1, "incomplete": 2, "passed": 0}[report["result"]]
