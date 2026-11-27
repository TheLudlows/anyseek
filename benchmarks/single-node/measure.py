#!/usr/bin/env python3
"""Measure a synthetic single-node Record workload via a disposable Release HTTP server."""
import argparse
import contextlib
import gzip
import hashlib
import json
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shared_record_http import LocalServer, digest, oid, quantiles

SCALES = (100_000, 1_000_000, 3_000_000)
SEED = "anyseek-229-v1"
BASELINE = "74d48d92db27d4fa155ce38004ac079f3bf92835"
API = "/api/v1"
DIMENSIONS = 128
TOP_K = 16
INGEST_BATCH = 64
BODY_BYTES = (128, 128, 256, 512)
SUCCESS = (200,)
OBSERVED = (200, 400, 404, 409, 413, 429, 500, 503, 507)
MIN_FREE_BYTES = 512 * 1024**2


class Server(LocalServer):
    tenant = "bench229"
    log_filter = ("anyseek::record::anchor=info,anyseek::record::indexer=info,"
                  "anyseek::catalog::service_snapshot=debug,anyseek_229_fence=info")

    def close(self):
        # Match the shared lifecycle for normal shutdown. A deliberate SIGKILL
        # belongs to the recovery probe and still closes its log.
        if self.p.returncode == -signal.SIGKILL:
            self.log.close()
            return
        super().close()


def file_digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def output_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def optional_text(path):
    file = Path(path)
    return file.read_text().strip() if file.exists() else None


def disk_bytes(*roots):
    allocated = 0
    for root in roots:
        for directory, _, names in os.walk(root, followlinks=False):
            for name in names:
                try:
                    allocated += (Path(directory) / name).lstat().st_blocks * 512
                except FileNotFoundError:
                    pass
    return allocated


def mount(path):
    row = json.loads(subprocess.check_output(
        ["findmnt", "-J", "-T", str(path), "-o", "SOURCE,TARGET,FSTYPE"], text=True
    ))["filesystems"][0]
    source = row["source"].split("[", 1)[0]
    disks = []
    if source.startswith("/dev/"):
        devices = subprocess.check_output(
            ["lsblk", "-s", "-n", "-p", "-o", "NAME,TYPE,TRAN", source], text=True
        )
        disks = [line.split() for line in devices.splitlines()
                 if len(line.split()) >= 2 and line.split()[1] == "disk"]
    row["backing_devices"] = sorted({row[0] for row in disks})
    row["backing_nvme"] = sorted({row[0] for row in disks if row[2:] == ["nvme"]})
    row["device_specs"] = [
        json.loads(subprocess.check_output(
            ["lsblk", "-J", "-b", "-d", "-p", "-o", "NAME,SIZE,MODEL,ROTA,TRAN", disk], text=True
        ))["blockdevices"][0] for disk in row["backing_devices"]
    ]
    row["device_id"] = path.stat().st_dev
    return row


class Evidence:
    def __init__(self, out, raw):
        self.out = out
        self.lock = threading.Lock()
        self.samples = {}
        self.raw = raw
        self.origin = time.perf_counter()

    def event(self, **entry):
        with self.lock:
            self.raw.write(json.dumps(entry, separators=(",", ":")) + "\n")
            self.raw.flush()
            if "phase" in entry and "operation" in entry:
                self.samples.setdefault((entry["phase"], entry["operation"]), []).append(entry)

    def call(self, server, phase, operation, method, path, body=None, *, expected=SUCCESS,
             detail=False):
        offset = time.perf_counter() - self.origin
        started = time.perf_counter()
        try:
            response, http_ms = server.call(method, path, body, allowed=OBSERVED)
        except (OSError, RuntimeError) as error:
            self.event(phase=phase, operation=operation, path=path, method=method, start_s=offset,
                       http_ms=(time.perf_counter() - started) * 1000, status=None,
                       success=False, busy=False, transport_error=str(error)[:400])
            raise
        status = server.last_status
        items = response.get("results", []) if isinstance(response, dict) else []
        codes = [item.get("code") for item in items]
        error_code = None
        if isinstance(response, dict):
            nested = response.get("error")
            error_code = response.get("code") or (nested.get("code") if isinstance(nested, dict) else None)
        busy_items = codes.count("busy")
        busy = status == 429 or error_code == "busy" or busy_items > 0
        success = status in expected and not busy and (
            not items or all(code in {"applied", "committed"} for code in codes)
        )
        row = {
            "phase": phase, "operation": operation, "path": path, "method": method,
            "start_s": offset, "end_s": time.perf_counter() - self.origin,
            "http_ms": http_ms, "status": status, "success": success, "busy": busy,
            "busy_items": busy_items, "item_codes": {code: codes.count(code) for code in set(codes)},
        }
        if detail or not success:
            row["response"] = response
        self.event(**row)
        return response, row

    def summary(self):
        result = {}
        for (phase, name), rows in sorted(self.samples.items()):
            durations = [r["http_ms"] for r in rows if r["success"]]
            wall = max(r.get("end_s", r["start_s"]) for r in rows) - min(r["start_s"] for r in rows)
            result.setdefault(phase, {})[name] = {
                "requests": len(rows), "successful": len(durations),
                "busy": sum(r["busy"] for r in rows),
                "other_failures": sum(not r["success"] and not r["busy"] for r in rows),
                "busy_fraction": sum(r["busy"] for r in rows) / len(rows),
                "failure_fraction": (len(rows) - len(durations)) / len(rows),
                "wall_s": wall, "successful_requests_per_s": len(durations) / wall if wall else None,
                "latency_ms": quantiles(durations) if durations else None,
            }
        return result


def require(response, row):
    if not row["success"]:
        raise RuntimeError(f'{row["phase"]} {row["operation"]} rejected: {row["status"]}: {str(response)[:400]}')
    return response


def api(evidence, server, phase, operation, method, path, body=None, **kwargs):
    return require(*evidence.call(server, phase, operation, method, path, body, **kwargs))


def collect_resources(server, roots, evidence, phase, done):
    while not done.is_set():
        try:
            fields = dict(line.split(":", 1) for line in Path(
                f"/proc/{server.proc_pid}/status").read_text().splitlines() if ":" in line)
            point = {
                "sample": "resources", "sample_phase": phase[0],
                "start_s": time.perf_counter() - evidence.origin,
                "rss_kib": int(fields["VmRSS"].split()[0]), "hwm_kib": int(fields["VmHWM"].split()[0]),
                "allocated_bytes": [disk_bytes(path) for path in roots],
            }
            evidence.event(**point)
            phase[1].append(point)
        except (FileNotFoundError, KeyError, ProcessLookupError):
            break
        done.wait(1)


@contextlib.contextmanager
def service(binary, data, out, name, cpus, evidence, summary, *, kill=False):
    started = time.perf_counter()
    server = Server(binary, data, out / f"{name}.log", cpus)
    summary.setdefault("processes", {})[name] = {
        "launch_to_live_ms": (time.perf_counter() - started) * 1000,
        "data_root": str(data),
    }
    phase = [name, []]
    done = threading.Event()
    temporary = data.parent / "tmp"
    thread = threading.Thread(target=collect_resources, args=(
        server, (data, temporary), evidence, phase, done))
    thread.start()
    try:
        yield server, phase
    finally:
        done.set()
        thread.join()
        summary["processes"][name]["samples"] = len(phase[1])
        if phase[1]:
            summary["processes"][name].update(
                peak_vm_hwm_kib=max(p["hwm_kib"] for p in phase[1]),
                peak_data_allocated_bytes_sampled=max(p["allocated_bytes"][0] for p in phase[1]),
                peak_tmp_allocated_bytes_sampled=max(p["allocated_bytes"][1] for p in phase[1]),
            )
        if kill and server.p.poll() is None:
            server.p.kill()
            if server.p.wait(timeout=20) != -signal.SIGKILL:
                raise RuntimeError("unexpected SIGKILL exit")
            summary["processes"][name]["exit"] = "SIGKILL"
        server.close()


def vector(i):
    raw = hashlib.shake_256(f"{SEED}/{i}".encode()).digest(DIMENSIONS)
    return [round((value - 127.5) / 127.5, 5) for value in raw]


def path_for(collection):
    return f"{API}/index/{collection}"


def document(i):
    length = BODY_BYTES[i % len(BODY_BYTES)]
    body = (f"topic{i % 32:02d} " + "padding " * (length // 8 + 1))[:length]
    return {"body": body, "bucket": (i // 32) % 8, "vector": vector(i)}


def new_collection(evidence, server, collection, epoch):
    config = {
        "index_name": collection, "tokenizer": "TantivySimpleV1",
        "schema": [
            {"field": "body", "field_type": "Text", "store": True, "index_lexical": True},
            {"field": "bucket", "field_type": "Int", "store": True, "filterable": True},
            {"field": "vector", "field_type": "Json", "store": True, "index_vector": True},
        ],
        "vector": {"dimensions": DIMENSIONS},
    }
    api(evidence, server, "load", "create", "POST", f"{API}/index",
        {"operation_id": oid(epoch, f"create-{collection}"), "collection_id": collection,
         "config": config}, detail=True)
    return config


def wait_cut(evidence, server, phase, collection, seq, timeout=180):
    started = time.perf_counter()
    while time.perf_counter() - started < timeout:
        progress = api(evidence, server, phase, "index_progress", "GET", path_for(collection) + "/ingest")
        if progress["index_error"] or progress["recovery_required"]:
            raise RuntimeError(f"index failure: {progress}")
        if min(progress["lexical_seq"], progress["vector_seq"]) >= seq:
            return {"cut": seq, "wait_ms": (time.perf_counter() - started) * 1000,
                    "progress": progress}
        time.sleep(.02)
    raise TimeoutError(f"index catch-up did not reach {collection}/{seq}: {progress}")


def load(evidence, server, collection, count, epoch, run):
    path = path_for(collection)
    corpus = hashlib.sha256()
    progress = {"requested": count, "confirmed": 0,
                "config": new_collection(evidence, server, collection, epoch)}
    run[collection] = progress
    started = time.perf_counter()
    for begin in range(0, count, INGEST_BATCH):
        if begin % (INGEST_BATCH * 16) == 0 and shutil.disk_usage(evidence.out).free < MIN_FREE_BYTES:
            raise RuntimeError("fixture filesystem reserve reached before API request")
        records = [{"record_id": f"r{i:09d}", "operation_id": oid(epoch, f"load-{i}"),
                    "expected": "Absent", "document": document(i)}
                   for i in range(begin, min(begin + INGEST_BATCH, count))]
        body = {"records": records}
        response, attempt = evidence.call(server, "load", f"{collection}_ingest", "POST",
                                          path + "/ingest", body)
        items = response.get("results", [])
        confirmed = [item["receipt"] for item in items if item["code"] == "committed" and item.get("receipt")]
        progress["confirmed"] += len(confirmed)
        if len(confirmed) != len(records) or not attempt["success"]:
            progress["boundary"] = {"begin": begin, "request_sha256": digest(body),
                                    "status": attempt["status"], "response": response}
            raise RuntimeError(f"{collection} load stopped after {progress['confirmed']}/{count}")
        if [r["record_id"] for r in confirmed] != [r["record_id"] for r in records]:
            raise RuntimeError("async committed receipts changed record identities")
        corpus.update((digest([r["document"] for r in records]) + "\n").encode())
        if (begin // INGEST_BATCH + 1) % 8 == 0 or begin + len(records) == count:
            cut = confirmed[-1]["seq"]
            sample = wait_cut(evidence, server, "load", collection, cut)
            progress.setdefault("catch_up", []).append(sample)
    progress["wall_s"] = time.perf_counter() - started
    progress["confirmed_records_per_s"] = count / progress["wall_s"]
    progress["corpus_batch_hash_chain_sha256"] = corpus.hexdigest()
    info = api(evidence, server, "load", "collection_info", "GET", path, detail=True)
    if info["statistics"]["active_records"] != count:
        raise RuntimeError(f"wrong active record count for {collection}: {info}")
    first = api(evidence, server, "load", "verify_point", "GET", path + "/doc/r000000000")
    if first["document"] != document(0) or first["version"] != 1:
        raise RuntimeError(f"loaded point read differs for {collection}: {first}")


def read(evidence, server, phase, collection, count, lane, iterations, barrier=None, point_failure=None):
    if barrier is not None:
        barrier.wait()
    path = path_for(collection)
    for j in range(iterations):
        i = (j * 53 + lane * 97) % count
        lexical = {"length": TOP_K, "fields": ["body", "bucket"], "query": f"topic{i % 32:02d}"}
        query = {"length": TOP_K, "fields": ["body", "bucket"], "query_vector": vector(i)}
        hybrid = dict(lexical, query_vector=query["query_vector"], filter=[
            {"Int": {"field": "bucket", "filter": {"start": (i // 32) % 8,
                                                   "end": (i // 32) % 8 + 1}}}
        ])
        operations = [("point", "GET", f"{path}/doc/r{i:09d}", None),
                      ("lexical", "POST", path + "/query", lexical),
                      ("vector", "POST", path + "/query", query),
                      ("hybrid_filtered", "POST", path + "/query", hybrid)]
        for name, method, endpoint, body in operations[j % 4:] + operations[:j % 4]:
            response, attempt = evidence.call(server, phase, name, method, endpoint, body)
            if not attempt["success"]:
                if point_failure and name == "point" and attempt["status"] == 503:
                    point_failure(collection, i, attempt)
                continue
            if name == "point" and (response["record_id"], response["version"], response["document"]) != (
                f"r{i:09d}", 1, document(i)
            ):
                raise RuntimeError("point identity/version mismatch")
            if name == "lexical" and not any(
                hit["record"]["document"]["body"].startswith(f"topic{i % 32:02d} ")
                for hit in response["hits"]
            ):
                raise RuntimeError("lexical query did not find a known topic")
            if name == "vector" and not response["hits"]:
                raise RuntimeError("vector query did not find a known indexed vector")
            if name == "hybrid_filtered" and (not response["hits"] or any(
                hit["record"]["document"]["bucket"] != (i // 32) % 8 for hit in response["hits"]
            )):
                raise RuntimeError("hybrid result escaped its metadata filter")


def replace_group(evidence, server, phase, collection, command, *, classify_rejection=False):
    result, attempt = evidence.call(server, phase, "group_replace", "POST",
                                    path_for(collection) + "/group/replace", command, detail=True)
    if classify_rejection and not attempt["success"]:
        return None
    require(result, attempt)
    receipt = result["receipt"]
    if not result["atomic"] or receipt["group_id"] != command["group_id"] or (
        receipt["group_version"], receipt["record_count"]
    ) != (command["expected_group_version"] + 1, len(command["records"])):
        raise RuntimeError(f"incorrect group receipt: {result}")
    return receipt


def activity(evidence, server, epoch, iterations, phase, barrier=None):
    if barrier is not None:
        barrier.wait()
    path = path_for("hot")
    for i in range(iterations):
        group = f"{phase}-{i}"
        records = [{"record_id": f"{group}-{j}", "document": document(i + j)} for j in range(2)]
        for previous, members in enumerate((records, records[:1], [])):
            command = {"operation_id": oid(epoch, f"{group}-{previous}"), "group_id": group,
                       "expected_group_version": previous, "records": members}
            if replace_group(evidence, server, phase, "hot", command, classify_rejection=True) is None:
                break  # The later versions still depend on this write.
        noise = {"record_id": f"noise-{group}", "operation_id": oid(epoch, f"noise-add-{group}"),
                 "expected": "Absent", "document": document(i)}
        response, row = evidence.call(server, phase, "async_write", "POST",
                                      path + "/ingest", {"records": [noise]}, detail=True)
        if not row["success"]:
            continue
        receipt = response["results"][0]["receipt"]
        sample = wait_cut(evidence, server, phase, "hot", receipt["seq"])
        evidence.event(sample="index_catch_up", sample_phase=phase, **sample)
        delete = dict(noise, operation_id=oid(epoch, f"noise-delete-{group}"),
                      expected={"Match": receipt["version"]}, document=None)
        evidence.call(server, phase, "record_delete", "POST", path + "/batch",
                      {"writes": [delete]}, detail=True)


def checkpoint(evidence, server, barrier):
    barrier.wait()
    for name in ("hot", "peer"):
        evidence.call(server, "mixed", f"{name}_checkpoint", "PATCH", path_for(name), detail=True)


def mixed_workload(evidence, server, counts, iterations, epoch, *, writer=True, maintenance=True,
                   point_failure=None):
    workers = 8 + int(writer) + int(maintenance)
    barrier = threading.Barrier(workers)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = []
        for lane in range(8):
            name = "hot" if lane < 6 else "peer"
            futures.append(pool.submit(read, evidence, server, "mixed", name, counts[name], lane,
                                       iterations, barrier, point_failure))
        if writer:
            futures.append(pool.submit(activity, evidence, server, epoch, iterations, "mixed", barrier))
        if maintenance:
            futures.append(pool.submit(checkpoint, evidence, server, barrier))
        for future in futures:
            future.result()


def measure(args, cpu_ids, backup_root, evidence, report, epoch):
    binary, out = args.binary, evidence.out
    loaded = report["load"]
    iterations = 4 if args.mode == "smoke" else 128
    target = args.scale if args.mode == "dedicated" else 512
    counts = {"hot": target, "peer": min(4096, target), "cold": 64}
    with service(binary, out / "live", out, "load_and_measure", cpu_ids, evidence, report, kill=True) as (server, phase):
        phase[0] = "load"
        load(evidence, server, "hot", counts["hot"], epoch, loaded)
        phase[0] = "hot_only"
        read(evidence, server, "warmup", "hot", target, 0, 4)
        read(evidence, server, "hot_only", "hot", target, 0, iterations)
        phase[0] = "load"
        for name in ("peer", "cold"):
            load(evidence, server, name, counts[name], epoch, loaded)
        phase[0] = "write_only"
        activity(evidence, server, epoch, iterations, "write_only")
        phase[0] = "mixed"
        mixed_workload(evidence, server, counts, iterations, epoch)
        phase[0] = "backup"
        from recovery import make_backup
        cut = make_backup(evidence, server, backup_root, epoch, report)
    from recovery import recover
    recover(args, evidence, report, cpu_ids, out / "live", backup_root, epoch, cut)


def maintenance_evidence(out, evidence):
    log = out / "load_and_measure.log"
    matches = re.findall(
        r"record_maintenance phase=state_image seq=(\d+) reason=(\S+) "
        r"incremental=(\w+) compact=(\w+) payload_compaction=(\w+)",
        log.read_text(encoding="utf-8", errors="replace") if log.exists() else "",
    )
    check_rows = [row for (phase, name), rows in evidence.samples.items()
                  if phase == "mixed" and name.endswith("_checkpoint") for row in rows]
    return {
        "state_image_events": [
            {"seq": int(seq), "reason": reason, "incremental": incremental == "true",
             "compact": compact == "true", "payload_compaction": payload == "true"}
            for seq, reason, incremental, compact, payload in matches
        ],
        "overlapping_queries_by_checkpoint": {
            checkpoint["operation"]: sum(
                row["start_s"] < checkpoint.get("end_s", checkpoint["start_s"])
                and checkpoint["start_s"] < row.get("end_s", row["start_s"])
                for (phase, operation), rows in evidence.samples.items()
                if phase == "mixed" and operation in {"point", "lexical", "vector", "hybrid_filtered"}
                for row in rows
            ) for checkpoint in check_rows
        },
        "explicit_compact": "not exposed over the baseline HTTP route",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--revision", required=True, help="full Git SHA of the built executable")
    parser.add_argument("--out", type=Path, required=True, help="new results and database directory")
    parser.add_argument("--backup-root", type=Path, help="new directory on independent backup media")
    parser.add_argument("--mode", choices=("smoke", "dedicated"), default="smoke")
    parser.add_argument("--scale", type=int, choices=SCALES)
    parser.add_argument("--cpus", default=",".join(map(str, sorted(os.sched_getaffinity(0))[:2])))
    parser.add_argument("--exclusive-nvme", action="store_true", help="attest that test devices and CPU are reserved")
    parser.add_argument("--point-fence-probe", action="store_true", help="run the four small-store point-read controls")
    parser.add_argument("--probe-rounds", type=int, default=3, help="fresh repetitions of each point-read control")
    args = parser.parse_args()
    if len(args.revision) != 40 or any(c not in "0123456789abcdef" for c in args.revision):
        parser.error("--revision must be a full lowercase commit SHA")
    if args.mode == "dedicated" and not (args.scale and args.exclusive_nvme and args.backup_root):
        parser.error("dedicated mode requires --scale, --exclusive-nvme and --backup-root")
    if args.mode == "smoke" and (args.scale or args.exclusive_nvme):
        parser.error("smoke mode has a fixed 512-record primary collection")
    if args.point_fence_probe and (args.mode != "smoke" or args.backup_root or args.probe_rounds < 1):
        parser.error("the point-fence probe needs smoke mode, no --backup-root, and positive --probe-rounds")
    args.binary, out = args.binary.resolve(strict=True), args.out.resolve()
    backup = args.backup_root.resolve() if args.backup_root else out / "backup-media"
    cpus = [int(cpu_id) for cpu_id in args.cpus.split(",")]
    if len(set(cpus)) != 2 or not set(cpus) <= os.sched_getaffinity(0):
        parser.error("--cpus must name two distinct CPUs available to the runner")
    media = mount(out.parent)
    backup_media = mount(backup.parent) if args.backup_root else media
    independent = bool(media["backing_devices"] and backup_media["backing_devices"] and
                       set(media["backing_devices"]).isdisjoint(backup_media["backing_devices"]) and
                       media["device_id"] != backup_media["device_id"])
    if args.mode == "dedicated" and (not media["backing_nvme"] or not independent):
        parser.error("dedicated acceptance requires a local NVMe data device and independent backup media")
    out.mkdir(exist_ok=False)
    if not args.point_fence_probe:
        backup.mkdir(exist_ok=False)
    # The subset below describes the fixed default-budget experiment. An operator's
    # AnySeek overrides, credentials, proxies and allocators must not affect it.
    for key in list(os.environ):
        if key.startswith(("ANYSEEK_", "SEEKPOWER_", "AWS_")) or key in {"LD_PRELOAD", "RUST_LOG"}:
            os.environ.pop(key)
    epoch = int(time.time() // 86400)
    if args.point_fence_probe:
        from point_fence import run_probe

        return run_probe(args, out, cpus, media, epoch)
    report = {
        "status": "running", "mode": args.mode, "scale": args.scale or 512,
        "revision": args.revision, "baseline_revision": BASELINE,
        "binary_sha256": file_digest(args.binary),
        "harness_sha256": {file: file_digest(Path(__file__).with_name(file))
                           for file in ("measure.py", "recovery.py")},
        "platform": platform.platform(), "machine_profile": optional_text("/proc/cpuinfo").split("\n\n")[0],
        "machine_model": optional_text("/sys/class/dmi/id/product_name"),
        "memory_total": optional_text("/proc/meminfo").splitlines()[0],
        "cgroup": {name: optional_text(f"/sys/fs/cgroup/{name}") for name in ("cpu.max", "memory.max")},
        "cpus": cpus, "threads": {"tokio": 2, "rayon": 2},
        "media": media, "backup_media": backup_media, "independent_backup_media_verified": independent,
        "exclusive_operator_attestation": args.exclusive_nvme, "epoch": epoch,
        "workload": {"seed": SEED, "body_bytes_cycle": BODY_BYTES, "dimensions": DIMENSIONS,
                     "top_k": TOP_K, "ingest_batch": INGEST_BATCH, "load_catchup_every_batches": 8,
                     "mixed_read_lanes": {"hot": 6, "peer": 2},
                     "mixed_writers": 1, "mixed_checkpoint_clients": 1},
        "load": {},
        "same_version_reopen": {"status": "not_run"},
        "independent_backup_restore": {"status": "not_run"},
        "pitr": {"status": "not_run"},

    }
    with gzip.open(out / "requests.jsonl.gz", "wt", encoding="utf-8") as raw:
        evidence = Evidence(out, raw)
        try:
            measure(args, cpus, backup, evidence, report, epoch)
            report["status"] = "complete"
        except BaseException as error:
            report["status"] = "boundary" if any(x.get("boundary") for x in report["load"].values()) else "failed"
            report["error"] = f"{type(error).__name__}: {error}"
            raise
        finally:
            report["operations"] = evidence.summary()
            report["maintenance"] = maintenance_evidence(out, evidence)
            output_json(out / "report.json", report)


if __name__ == "__main__":
    raise SystemExit(main())
