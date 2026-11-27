#!/usr/bin/env python3
"""Small, traced multi-collection HTTP comparison for AnySeek #237."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import gzip
import hashlib
import http.client
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shared_record_http import digest, oid, quantiles  # noqa: E402
from evidence import NS_PER_MS, TracedServer, phase_windows, read_trace, resources  # noqa: E402


API = "/api/v1"
CASES = ("control", "index_build", "cache_compaction", "reclamation")
HOT, NEIGHBOR = "hot", "neighbor"
COLLECTIONS = (HOT, NEIGHBOR)
SEED_SIZES = {HOT: 1024, NEIGHBOR: 64}
QUERY = {"length": 4, "fields": ["body"], "query": "sentinel"}
VECTOR = [1.0, .25, 0.0, .25, .5, 0.0, .5, .25]
STATUSES = tuple(range(100, 600))


def endpoint(collection):
    return f"{API}/index/{collection}"


def document(topic):
    return {"body": topic + " synthetic fixture237", "vector": VECTOR}


def checked(condition, message, value):
    if not condition:
        raise RuntimeError(f"{message}: {value!r}")


def call(server, method, path, args=None):
    response, _ = server.call(method, path, args)
    return response


def write_command(epoch, collection, record_id, topic):
    return {"operation_id": oid(epoch, f"{collection}-{record_id}"), "record_id": record_id,
            "expected": "Absent", "document": document(topic)}


def batch_checked(response, commands):
    rows = response["results"]
    checked(len(rows) == len(commands), "batch length", response)
    for result, command in zip(rows, commands):
        checked(result["operation_id"] == command["operation_id"], "batch operation id", result)
        if result["code"] == "applied":
            receipt = result["receipt"]
            checked((receipt["record_id"], receipt["version"]) == (command["record_id"], 1),
                    "write receipt identity/version", receipt)
    return rows


def seed(server, epoch, collection, count, old=False):
    config = {"index_name": collection, "tokenizer": "TantivySimpleV1", "schema": [
        {"field": "body", "field_type": "Text", "store": True, "index_lexical": True},
        {"field": "vector", "field_type": "Json", "store": True, "index_vector": True},
    ], "vector": {"dimensions": len(VECTOR)}}
    incarnation = "old" if old else "current"
    call(server, "POST", f"{API}/index", {"collection_id": collection, "config": config,
                                         "operation_id": oid(epoch, f"create-{collection}-{incarnation}")})
    for begin in range(0, count, 64):
        writes = [write_command(epoch, collection, f"{incarnation}-{i:04d}",
                                "retired" if old else "sentinel" if i == 0 else "background")
                  for i in range(begin, min(begin + 64, count))]
        rows = batch_checked(call(server, "POST", endpoint(collection) + "/batch", {"writes": writes}), writes)
        checked(all(row["code"] == "applied" for row in rows), "seed write rejected", rows)


def verify_point(response, record_id="current-0000", topic="sentinel"):
    actual = response["record_id"], response["version"], response["document"]
    checked(actual == (record_id, 1, document(topic)), "point identity/version/document", actual)


def setup(server, folder, epoch, case):
    seed(server, epoch, HOT, 512, old=True)
    hot_path = folder / "live" / "collections" / server.tenant / HOT
    old_path, = (path for path in hot_path.iterdir() if path.is_dir() and len(path.name) == 16)
    backup = call(server, "POST", endpoint(HOT) + "/backup",
                  {"operation_id": oid(epoch, "pin-old-hot")})
    delete = {"operation_id": oid(epoch, "delete-old-hot")}
    old_receipt = call(server, "DELETE", endpoint(HOT), delete)
    for collection, count in SEED_SIZES.items():
        seed(server, epoch, collection, count)
        verify_point(call(server, "GET", endpoint(collection) + "/doc/current-0000"))
    if case != "cache_compaction":
        # Same authoritative rows. Leave the compaction case with native mutable
        # components so the common append plan crosses the actual merge threshold.
        response = call(server, "PATCH", endpoint(HOT))
        checked("indexed_seq" in response, "setup index reply", response)
    checked(old_path.exists(), "backup did not retain old incarnation", old_path)
    current, = (path for path in hot_path.iterdir() if path.is_dir() and len(path.name) == 16 and path != old_path)
    return old_path, current.name, backup["backup_id"], delete, old_receipt


def offered_load():
    lanes = []
    for collection in COLLECTIONS:
        for lane in range(2):
            tasks = []
            for index in range(480):
                offset = 80 + 10 * index + 3 * lane + (1 if collection == NEIGHBOR else 0)
                operation = "point" if (index + lane) % 2 == 0 else "lexical"
                tasks.append((offset, collection, operation, index))
            lanes.append(tasks)
        # Keep the neighbor below a tail roll. The hot append plan crosses
        # the merge threshold in the uncheckpointed fixture only.
        count = 36 if collection == HOT else 24
        lanes.append([(400 + 120 * i, collection, "append", i) for i in range(count)])
    return lanes


def invoke(server, method, path, body, start_at_ns, offset_ms, collection, operation, number, stop=None):
    planned = start_at_ns + offset_ms * NS_PER_MS
    delay = (planned - time.monotonic_ns()) / 1e9
    if stop is not None:
        if stop.wait(max(0, delay)):
            return None, None
    elif delay > 0:
        time.sleep(delay)
    actual = time.monotonic_ns()
    start = time.time_ns()
    response = {}
    status = elapsed = error = None
    server.last_status = None
    try:
        response, elapsed = server.call(method, path, body, allowed=STATUSES)
        status = server.last_status
    except (OSError, http.client.HTTPException, ValueError) as exc:
        status = server.last_status
        error = f"{type(exc).__name__}: {exc}"
    end = time.time_ns()
    end_mono = time.monotonic_ns()
    # The shared helper times send through body read; JSON encoding/decoding
    # are inside the outer brackets only. Their uncertainty can only shrink
    # the interval used to claim an actual overlap.
    core = (end - round(elapsed * NS_PER_MS), start + round(elapsed * NS_PER_MS)) if elapsed is not None else (end, start)
    record = {"collection": collection, "operation": operation, "number": number,
              "method": method, "path": path, "planned_ms": offset_ms, "lateness_ms": (actual - planned) / NS_PER_MS,
              "start_ns": start, "end_ns": end, "http_core_ns": core,
              "wall_minus_mono_ns": (start - actual, end - end_mono),
              "status": status, "http_ms": elapsed, "transport_error": error,
              "code": response.get("code", (response.get("error") or {}).get("code")),
              "success": status == 200 and error is None}
    if not record["success"] or operation in ("build", "unpin"):
        record["response"] = response
    return record, response


def run_lane(server, epoch, plan, start_at_ns, stop):
    records = []
    for offset, collection, operation, number in plan:
        path = endpoint(collection)
        if operation == "point":
            method, path, body = "GET", path + "/doc/current-0000", None
        elif operation == "lexical":
            method, path, body = "POST", path + "/query", QUERY
        else:
            count = 8 if collection == HOT else 1
            writes = [write_command(epoch, collection, f"append-{number:03d}-{i}", "append")
                      for i in range(count)]
            method, path, body = "POST", path + "/batch", {"writes": writes}
        row, response = invoke(server, method, path, body, start_at_ns, offset, collection, operation, number, stop)
        if row is None:
            break
        if row["success"] and operation == "point":
            verify_point(response)
        elif row["success"] and operation == "lexical":
            hits = response["hits"]
            checked(len(hits) == 1, "query sentinel hit count", response)
            record = hits[0]["record"]
            checked((record["record_id"], record["version"], record["document"]["body"]) ==
                    ("current-0000", 1, document("sentinel")["body"]), "query identity/version/body", record)
        elif row["success"] and operation == "append":
            items = batch_checked(response, writes)
            row["item_codes"] = dict(Counter(item["code"] for item in items))
            row["receipts"] = [item["receipt"] for item in items if item["code"] == "applied"]
            row["success"] = len(row["receipts"]) == len(writes)
            checked(all(item["code"] in {"applied", "busy", "not_attempted"} for item in items),
                    "unexpected append item", items)
        records.append(row)
    return records


def overlap(row, windows):
    start, end = row["http_core_ns"]
    return any(max(start, window["start_ns"]) < min(end, window["end_ns"]) for window in windows)


def summarize(rows):
    result = {}
    for collection in COLLECTIONS:
        result[collection] = {}
        for operation in ("point", "lexical", "append"):
            samples = [row for row in rows if (row["collection"], row["operation"]) == (collection, operation)]
            success = [row["http_ms"] for row in samples if row["success"]]
            result[collection][operation] = {
                "attempts": len(samples), "successful": len(success),
                "success_rate": len(success) / len(samples) if samples else None,
                "p99_success_ms": quantiles(success)["p99_ms"] if success else None,
                "p99_launch_lateness_ms": quantiles([row["lateness_ms"] for row in samples])["p99_ms"] if samples else None,
                "http_429": sum(row["status"] == 429 for row in samples),
                "busy_items": sum(row.get("item_codes", {}).get("busy", 0) for row in samples),
                "http_503_codes": dict(Counter(row["code"] for row in samples if row["status"] == 503)),
                "other_http_errors": dict(Counter(row["status"] for row in samples if row["status"] not in (None, 200, 429, 503))),
                "transport_errors": sum(row["transport_error"] is not None for row in samples),
            }
    return result


def check_windows(case, events, action, verified_reclaimed):
    phases = phase_windows(events)
    if case == "index_build":
        if not action["success"] or "indexed_seq" not in action["response"]:
            return [], {"reason": "index request did not complete"}
        windows = [window for window in phases if window["kind"] == "anchor_prepare"
                   and window["seq"] == action["response"]["indexed_seq"] and overlap(action, [window])]
        return windows, {"request_to_phase_ms": (windows[0]["start_ns"] - action["start_ns"]) / NS_PER_MS if windows else None}
    if case == "cache_compaction":
        windows = []
        evidence = []
        for build in [phase for phase in phases if phase["kind"] == "cache_merge_build_unlocked"]:
            capture = [phase for phase in phases if phase["kind"] == "cache_merge_capture_locked"
                       and phase["seq"] == build["seq"] and phase["end_ns"] <= build["start_ns"]]
            next_capture = min((phase["start_ns"] for phase in phases if phase["kind"] == "cache_merge_capture_locked"
                                and phase["start_ns"] > build["start_ns"]), default=float("inf"))
            installs = [phase for phase in phases if phase["kind"] == "cache_merge_install_locked"
                        and build["end_ns"] <= phase["start_ns"] < next_capture]
            if not capture or len(installs) != 1:
                continue
            install, = installs
            cache_writes = [event["source"] for event in events if event["kind"] == "hot_cache_write"
                            and build["start_ns"] <= event["start_ns"] < build["end_ns"]]
            finishes = [event for event in events if event["kind"] == "worker_finished"
                        and install["end_ns"] <= event["start_ns"] < next_capture]
            scheduled = [event for event in events if event["kind"] == "scheduled" and event["cause"] == "derived_cache_merge"
                         and event["seq"] == build["seq"] and event["start_ns"] <= capture[-1]["start_ns"]]
            started = [event for event in events if event["kind"] == "worker_started"
                       and event["start_ns"] <= capture[-1]["start_ns"]]
            if cache_writes and len(finishes) == 1 and finishes[0]["outcome"] == "installed":
                windows.extend([build, install])
                evidence.append({"cache_write_count": len(cache_writes), "first_cache_write": cache_writes[0],
                                 "worker_finished": finishes[0]["source"],
                                 "worker_start_to_capture_ms": (capture[-1]["start_ns"] - started[-1]["start_ns"]) / NS_PER_MS if started else None,
                                 "scheduled_to_capture_ms": (capture[-1]["start_ns"] - scheduled[-1]["start_ns"]) / NS_PER_MS if scheduled else None})
        return windows, {"installed_merges": evidence}
    if case == "reclamation":
        removes = [event for event in events if event["kind"] == "retired_remove"
                   and action["start_ns"] <= event["start_ns"]]
        renames = [event["source"] for event in events if event["kind"] == "retired_rename"]
        return removes if action["success"] and verified_reclaimed and renames else [], {
            "unpin_request_to_first_remove_ms": (removes[0]["start_ns"] - action["start_ns"]) / NS_PER_MS if removes else None,
            "unpin_reply_to_first_remove_ms": (removes[0]["start_ns"] - action["end_ns"]) / NS_PER_MS if removes else None,
            "rename_sources": renames, "verified_removed": verified_reclaimed}
    return [], {}


def retired_absent(old):
    candidates = (old, old.with_name(".retired-" + old.name), old.with_name(".retired-retry-" + old.name),
                  old.with_name(".initial-resume-" + old.name))
    return all(not path.exists() for path in candidates)


def wait_retired(old, timeout_s):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if retired_absent(old):
            return True
        time.sleep(.02)
    return retired_absent(old)


def dump(path, data):
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def verify_post_run(server, rows):
    for collection in COLLECTIONS:
        verify_point(call(server, "GET", endpoint(collection) + "/doc/current-0000"))
        applied = [receipt for row in rows if row["collection"] == collection for receipt in row.get("receipts", [])]
        checked(applied, "no append receipts to verify", collection)
        record_id = applied[-1]["record_id"]
        verify_point(call(server, "GET", endpoint(collection) + "/doc/" + record_id), record_id, "append")


def run_case(binary, folder, cpus, epoch, case, lanes):
    folder.mkdir()
    stop = threading.Event()
    pool = ThreadPoolExecutor(max_workers=len(lanes) + 1)
    action = None
    try:
        with TracedServer(binary, folder, cpus) as server:
            old, current, backup, _, _ = setup(server, folder, epoch, case)
            with resources(server, folder) as samples:
                try:
                    after_ns = time.time_ns()
                    after_mono = time.monotonic_ns()
                    start_at_ns = after_mono + 300 * NS_PER_MS
                    future = [pool.submit(run_lane, server, epoch, lane, start_at_ns, stop) for lane in lanes]
                    if case == "index_build":
                        action, _ = invoke(server, "PATCH", endpoint(HOT), None, start_at_ns, 505, HOT, "build", 0)
                    if case == "reclamation":
                        action, _ = invoke(server, "DELETE", f"{API}/backups/{backup}",
                                           {"operation_id": oid(epoch, "unpin")}, start_at_ns, 505, HOT, "unpin", 0)
                    rows = [row for lane in future for row in lane.result()]
                    clock_delta_ms = max(abs(offset - (after_ns - after_mono))
                                         for row in rows + ([action] if action else [])
                                         for offset in row["wall_minus_mono_ns"]) / NS_PER_MS
                    gone = wait_retired(old, 15) if case == "reclamation" else retired_absent(old)
                finally:
                    stop.set()
            witness_error = None
            try:
                verify_post_run(server, rows)
            except (RuntimeError, OSError, http.client.HTTPException, ValueError) as exc:
                witness_error = str(exc)
    finally:
        # The private server must already be closed so blocked HTTP reads can
        # unwind without waiting for their client timeout.
        pool.shutdown(wait=False, cancel_futures=True)
    events = read_trace(folder, old.name, current, after_ns)
    windows, maintenance = check_windows(case, events, action, gone)
    overlap_rows = [row for row in rows if overlap(row, windows)]
    unexpected = {event["kind"] for event in events} & {
        "anchor_prepare", "cache_merge_build_unlocked", "retired_rename", "retired_remove"}
    qualified = not unexpected if case == "control" else (
        bool(windows) and all(any(row["collection"] == collection and row["operation"] in {"point", "lexical"}
                                  for row in overlap_rows) for collection in COLLECTIONS)
    )
    expected_physical_state = gone if case == "reclamation" else old.exists()
    qualified = qualified and expected_physical_state and clock_delta_ms <= 2 and bool(samples)
    with gzip.open(folder / "requests.jsonl.gz", "wt") as stream:
        for row in sorted(rows + ([action] if action else []), key=lambda row: row["start_ns"]):
            row["overlaps_confirmed_maintenance"] = overlap(row, windows)
            stream.write(json.dumps(row) + "\n")
    dump(folder / "events.json", events)
    dump(folder / "resources.json", samples)
    functional_errors = int(witness_error is not None) + sum(row["transport_error"] is not None or row["status"] not in (200, 429, 503)
                            for row in rows)
    report = {"case": case, "coverage_complete": qualified, "unexpected_request_failures": functional_errors,
              "post_measurement_witness_error": witness_error, "starting_active_records": SEED_SIZES,
              "window_duration_ms": sum(window["end_ns"] - window["start_ns"] for window in windows) / NS_PER_MS,
              "max_sampled_clock_delta_ms": clock_delta_ms,
              "resource_admission_events": [event for event in events if event["kind"] == "resource_admission"],
              "maintenance_backpressure_events": [event for event in events if event["kind"] == "backpressure"],
              "maintenance": maintenance, "action": action, "confirmed_windows": windows,
              "control_unexpected_activity": sorted(unexpected) if case == "control" else None,
              "original_incarnation_present": old.exists(), "all_requests": summarize(rows),
              "overlapped_requests": summarize(overlap_rows), "resource_sample_count": len(samples),
              "peak_rss_hwm_bytes": max((sample["memory_bytes"]["VmHWM"] for sample in samples), default=None),
              "peak_allocated_bytes": {key: max((sample["allocated_bytes"][key] for sample in samples), default=None)
                                       for key in ("live", "tmp")}}
    dump(folder / "report.json", report)
    return report


def control_cohort(out, cases):
    def request_rows(name):
        with gzip.open(out / name / "requests.jsonl.gz", "rt") as stream:
            return [json.loads(line) for line in stream]

    def slot(row):
        return row["collection"], row["operation"], row["planned_ms"]

    control_by_slot = {slot(row): row for row in request_rows("control")}
    cohorts = {}
    for name in cases:
        if name == "control":
            continue
        matching = [control_by_slot[slot(row)] for row in request_rows(name)
                    if row["operation"] not in ("build", "unpin") and row["overlaps_confirmed_maintenance"]]
        cohorts[name] = summarize(matching)
    return cohorts


def crash_while_pinned(binary, folder, cpus, epoch):
    folder.mkdir()
    with TracedServer(binary, folder, cpus) as server:
        old, _, backup, delete, old_receipt = setup(server, folder, epoch, "control")
        witness = write_command(epoch, NEIGHBOR, "crash-witness", "durable")
        applied, = batch_checked(call(server, "POST", endpoint(NEIGHBOR) + "/batch",
                                      {"writes": [witness]}), [witness])
        checked(applied["code"] == "applied", "neighbor witness not acknowledged", applied)
        server.close(crash=True)

    with TracedServer(binary, folder, cpus) as server:
        checked(old.exists(), "restart lost protected old incarnation", old)
        checked(call(server, "DELETE", endpoint(HOT), delete) == old_receipt,
                "replay changed the old deletion receipt", old_receipt)
        replay, = batch_checked(call(server, "POST", endpoint(NEIGHBOR) + "/batch",
                                     {"writes": [witness]}), [witness])
        checked(replay.get("receipt") == applied["receipt"], "restart changed neighbor write receipt", replay)
        verify_point(call(server, "GET", endpoint(NEIGHBOR) + "/doc/crash-witness"), "crash-witness", "durable")
        verify_point(call(server, "GET", endpoint(HOT) + "/doc/current-0000"))
        unpinned = call(server, "DELETE", f"{API}/backups/{backup}", {"operation_id": oid(epoch, "fault-unpin")})
        checked(wait_retired(old, 15), "unpin after restart did not reclaim old incarnation", old)
        verify_point(call(server, "GET", endpoint(HOT) + "/doc/current-0000"))
        verify_point(call(server, "GET", endpoint(NEIGHBOR) + "/doc/crash-witness"), "crash-witness", "durable")
    result = {"signal": "SIGKILL", "cut": "old hot deleted and protected by backup, neighbor write acknowledged",
              "old_generation": old.name, "old_delete_receipt": old_receipt, "neighbor_receipt": applied["receipt"],
              "unpin_response": unpinned, "verified_reclaimed": True}
    dump(folder / "report.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path, required=True, help="owned Linux Release anyseek_server")
    parser.add_argument("--engine-sha", required=True, help="Git commit used to build --server")
    parser.add_argument("--out", type=Path, required=True, help="new output directory; keeps disposable databases and traces")
    parser.add_argument("--cpus", required=True, help="taskset CPU list for the server, e.g. 0,1")
    parser.add_argument("--case", action="append", choices=CASES, help="one or more exploratory cases; default runs all four")
    parser.add_argument("--with-crash", action="store_true", help="also test restart while old hot is backup-pinned")
    args = parser.parse_args()
    binary = args.server.resolve(strict=True)
    out = args.out.resolve()
    out.mkdir()
    lanes = offered_load()
    epoch = int(time.time() // 86400)
    with binary.open("rb") as stream:
        binary_hash = hashlib.file_digest(stream, "sha256").hexdigest()
    strace_version = subprocess.check_output(["strace", "-V"], text=True).splitlines()[0]
    stat = subprocess.check_output(["stat", "-f", "-c", "%T", str(out)], text=True).strip()
    report = {"engine_sha": args.engine_sha, "binary_sha256": binary_hash, "argv": sys.argv,
              "harness_sha": subprocess.check_output(
                  ["git", "-C", str(Path(__file__).resolve().parents[2]), "rev-parse", "HEAD"],
                  text=True).strip(),
              "source_sha256": {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in
                                (Path(__file__), Path(__file__).with_name("evidence.py"),
                                 Path(__file__).parents[1] / "shared_record_http.py")},
              "workload_plan_sha256": digest(lanes), "filesystem": stat, "kernel": platform.release(),
              "strace": strace_version, "server_cpus": args.cpus, "driver_affinity": sorted(os.sched_getaffinity(0)),
              "operation_epoch": epoch, "cases": {}}
    dump(out / "report.json", report)
    cases = args.case or CASES
    for name in cases:
        case = run_case(binary, out / name, args.cpus, epoch, name, lanes)
        report["cases"][name] = {"coverage_complete": case["coverage_complete"],
                                 "unexpected_request_failures": case["unexpected_request_failures"],
                                 "report": f"{name}/report.json"}
        dump(out / "report.json", report)
        print(f"{name}: coverage_complete={case['coverage_complete']}", flush=True)
    if "control" in cases:
        report["control_same_slots_as_treatment_overlap"] = control_cohort(out, cases)
        dump(out / "report.json", report)
    if args.with_crash:
        crash_while_pinned(binary, out / "crash_while_pinned", args.cpus, epoch)
        report["crash_while_pinned"] = "crash_while_pinned/report.json"
        dump(out / "report.json", report)
        print("crash_while_pinned: verified_reclaimed=True", flush=True)
    return 0 if all(case["coverage_complete"] and case["unexpected_request_failures"] == 0
                    for case in report["cases"].values()) else 1


if __name__ == "__main__":
    sys.exit(main())
