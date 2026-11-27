"""Snapshot, SIGKILL/reopen and future PITR cut fixtures for the single-node workload."""
import re
import shutil
import time
from datetime import UTC, datetime
from pathlib import Path

from measure import (
    API,
    api,
    digest,
    document,
    file_digest,
    new_collection,
    oid,
    output_json,
    path_for,
    replace_group,
    service,
)

COLLECTION = "recovery"
PATH = path_for(COLLECTION)
SNAPSHOT_LIMIT = 1024**3
ARCHIVE_ID = re.compile(r"[0-9a-f]{64}")
PREFLIGHT = re.compile(r"service snapshot preflight payload_bytes=(\d+) limit_bytes=(\d+)")


def utc():
    return datetime.now(UTC).isoformat()


def copy_verified(source, destination):
    started = time.perf_counter()
    shutil.copytree(source, destination)
    source_files = sorted(p.relative_to(source) for p in source.rglob("*") if p.is_file())
    target_files = sorted(p.relative_to(destination) for p in destination.rglob("*") if p.is_file())
    if source_files != target_files or not source_files:
        raise RuntimeError("copied archive inventory differs")
    copies = []
    for relative_path in source_files:
        expected = file_digest(source / relative_path)
        observed = file_digest(destination / relative_path)
        if expected != observed:
            raise RuntimeError(f"copied archive digest differs: {relative_path}")
        copies.append({"file": str(relative_path), "sha256": expected,
                       "bytes": (destination / relative_path).stat().st_size})
    return {"files": copies, "copy_and_verify_ms": (time.perf_counter() - started) * 1000,
            "bytes": sum(row["bytes"] for row in copies)}


def group_request(epoch, label, group, previous, records):
    members = []
    for i, ident in enumerate(records):
        body = document(i)
        body["body"] = f"{label} {body['body']}"
        members.append({"record_id": ident, "document": body})
    return {"operation_id": oid(epoch, label), "group_id": group,
            "expected_group_version": previous, "records": members}


def member(evidence, server, phase, identity, version, expected_doc=None):
    row = api(evidence, server, phase, "verify_member", "GET", PATH + "/doc/" + identity,
              expected=(404,) if version is None else (200,), detail=True)
    if version is not None and (row["record_id"], row["version"], row["document"]) != (
        identity, version, expected_doc
    ):
        raise RuntimeError(f"incorrect recovered record {identity}: {row}")


def group_version(evidence, server, phase, identity, expected):
    value = api(evidence, server, phase, "verify_group_version", "GET",
                PATH + "/group?group_id=" + identity,
                expected=(404,) if expected is None else (200,), detail=True)
    if expected is not None and value.get("group_version") != expected:
        raise RuntimeError(f"incorrect group version: {value}")


def ordinary_receipt(evidence, server, phase, command, expected):
    op = command["operation_id"]
    reply = api(evidence, server, phase, "verify_operation", "GET",
                f"{PATH}/operation/{op['epoch']}/{op['nonce']}",
                detail=True)
    if reply.get("receipt") != expected:
        raise RuntimeError(f"receipt changed: {reply}")


def verify(evidence, server, phase, fixture, cut):
    a = fixture["commands"]["A"]
    b = fixture["commands"]["B"]
    c = fixture["commands"]["C"]
    group_version(evidence, server, phase, a["group_id"], 1)
    for item in a["records"]:
        member(evidence, server, phase, item["record_id"], 1, item["document"])
    group_version(evidence, server, phase, b["group_id"], {"A": None, "B": 1, "C": 2}[cut])
    for item in b["records"]:
        member(evidence, server, phase, item["record_id"], 1 if cut == "B" else None,
               item["document"])
    sentinel = fixture["commands"]["C_witness"]
    member(evidence, server, phase, sentinel["record_id"], 1 if cut == "C" else None,
           sentinel["document"])
    ordinary_receipt(evidence, server, phase, sentinel, fixture["receipts"]["C_witness"] if cut == "C" else None)
    if replace_group(evidence, server, phase, COLLECTION, a) != fixture["receipts"]["A"]:
        raise RuntimeError("recovered A receipt differs")
    if cut != "A" and replace_group(evidence, server, phase, COLLECTION, b) != fixture["receipts"]["B"]:
        raise RuntimeError("recovered B receipt differs")
    if cut == "C" and replace_group(evidence, server, phase, COLLECTION, c) != fixture["receipts"]["C"]:
        raise RuntimeError("recovered C receipt differs")
    # An original B replay cannot resurrect C's tombstones.
    hits = api(evidence, server, phase, "verify_lexical_cut", "POST", PATH + "/query",
               {"query": "pitronly", "length": 16}, detail=True)["hits"]
    ids = sorted(hit["record"]["record_id"] for hit in hits)
    expected = sorted(item["record_id"] for item in b["records"]) if cut == "B" else []
    if ids != expected:
        raise RuntimeError(f"wrong cut search at {cut}: {ids} != {expected}")


def make_backup(evidence, server, backup_root, epoch, report):
    phase = "backup"
    new_collection(evidence, server, COLLECTION, epoch)
    a = group_request(epoch, "base", "base-group", 0, ["a-answer", "a-projection"])
    b = group_request(epoch, "pitronly", "later-group", 0, ["b-answer", "b-projection"])
    c = group_request(epoch, "retract", "later-group", 1, [])
    sentinel = {"record_id": "c-witness", "operation_id": oid(epoch, "after-cut"),
                "expected": "Absent", "document": document(29)}
    fixture = {
        "commands": {"A": a, "B": b, "C": c, "C_witness": sentinel},
        "receipts": {"A": replace_group(evidence, server, phase, COLLECTION, a)},
        "timestamps_utc": {"A_observed": utc()},
        "expected_at_B": {"a_group_version": 1, "b_group_version": 1,
                          "b_record_ids": ["b-answer", "b-projection"],
                          "c_witness_absent": True, "c_group_version_not_applied": True,
                          "c_witness_receipt_absent": True, "b_retry_returns_original": True},
    }
    backup_request = {"operation_id": oid(epoch, "service-backup-A")}
    reply, row = evidence.call(server, phase, "service_backup_A", "POST",
                               f"{API}/service-backups", backup_request, detail=True)
    log_lines = (evidence.out / "load_and_measure.log").read_text(encoding="utf-8", errors="replace")
    estimates = PREFLIGHT.findall(log_lines)
    estimate, limit = (map(int, estimates[-1]) if estimates else (None, SNAPSHOT_LIMIT))
    report["backup"] = {
        "status": "published" if row["success"] else "refused_or_indeterminate",
        "preflight_payload_bytes": estimate, "preflight_limit_bytes": limit,
        "preflight_bytes_source": "server debug log" if estimates else "not exposed by this binary",
        "http_status": row["status"], "response": reply, "duration_ms": row["http_ms"],
        "backup_request": backup_request,
    }
    if row["success"]:
        backup_id = reply["backup_id"]
        if not ARCHIVE_ID.fullmatch(backup_id):
            raise RuntimeError("invalid service archive identity")
        archive_name = f"service-backup-{backup_id}"
        report["backup"]["copy"] = copy_verified(evidence.out / "live" / archive_name,
                                                    backup_root / archive_name)
        fixture["backup_id"] = backup_id
        fixture["timestamps_utc"]["A_archive_copied"] = utc()
    fixture["receipts"]["B"] = replace_group(evidence, server, phase, COLLECTION, b)
    fixture["timestamps_utc"]["B_observed"] = utc()
    verify(evidence, server, "live_B", fixture, "B")
    fixture["receipts"]["C"] = replace_group(evidence, server, phase, COLLECTION, c)
    result = api(evidence, server, phase, "write_after_cut_witness", "POST", PATH + "/batch",
                 {"writes": [sentinel]}, detail=True)
    fixture["receipts"]["C_witness"] = result["results"][0]["receipt"]
    fixture["timestamps_utc"]["C_observed"] = utc()
    verify(evidence, server, "live_C", fixture, "C")
    fixture["restore_status"] = "prepared_only"
    fixture["archive_window"] = None
    fixture["restore_to_seq_api"] = None
    fixture["pending"] = ["archive continuity and explicit cut selection", "Origence B paired manifest"]
    output_json(evidence.out / "pitr-fixture.json", fixture)
    return fixture


def probe_corrupt_archive(args, evidence, report, cpus, archive, backup_root, epoch, fixture):
    parent = evidence.out / "damaged-restore"
    parent.mkdir()
    control = parent / "control"
    with service(args.binary, control, evidence.out, "damaged_archive", cpus, evidence, report) as (server, _):
        copied = control / archive
        copy_verified(backup_root / archive, copied)
        payload = next(iter(sorted(copied.rglob("payload-*"))))
        relative = payload.relative_to(copied)
        expected = file_digest(payload)
        with payload.open("r+b") as stream:
            original = stream.read(1)
            if not original:
                raise RuntimeError("archive witness has no payload bytes")
            stream.seek(0)
            stream.write(bytes([original[0] ^ 1]))
        reply, attempt = evidence.call(
            server, "damaged_archive", "restore_corrupt_copy", "POST", f"{API}/service-restores",
            {"operation_id": oid(epoch, "restore-corrupt-A"), "backup_id": fixture["backup_id"]},
            detail=True,
        )
        if attempt["success"] or list(parent.glob("restored-service-*")):
            raise RuntimeError(f"damaged archive activated a restore: {reply}")
        if file_digest(backup_root / archive / relative) != expected:
            raise RuntimeError("golden archive was unexpectedly changed")
        report["damaged_archive"] = {
            "status": "rejected", "http_status": attempt["status"], "response": reply,
            "file": str(relative), "original_sha256": expected, "damaged_sha256": file_digest(payload),
        }


def recover(args, evidence, report, cpus, live, backup_root, epoch, fixture):
    started = time.perf_counter()
    with service(args.binary, live, evidence.out, "same_version_reopen", cpus, evidence, report) as (server, _):
        verify(evidence, server, "reopen_C", fixture, "C")
        # Avoid substituting a healthy catalog listener for validated data readiness.
        value = api(evidence, server, "reopen_C", "verify_hot_point", "GET",
                    path_for("hot") + "/doc/r000000000", detail=True)
        if value["document"] != document(0) or value["version"] != 1:
            raise RuntimeError("hot collection was not recovered")
        cold = api(evidence, server, "reopen_C", "untouched_collection_first_point", "GET",
                   path_for("cold") + "/doc/r000000000", detail=True)
        if cold["document"] != document(0) or cold["version"] != 1:
            raise RuntimeError("untouched collection was not recovered")
        report["same_version_reopen"] = {"status": "verified", "verified_at": "C", "launch_to_verified_ms": (
            time.perf_counter() - started) * 1000,
            "page_cache_policy": "process cold; existing OS cache left intact"}
    report["pitr"] = {
        "status": "pending_archive_and_cut_API", "fixture": "pitr-fixture.json",
        "positive_control_B_verified_before_C": True,
    }
    if "backup_id" not in fixture:
        report["independent_backup_restore"] = {"status": "skipped_no_published_backup"}
        return

    archive = f"service-backup-{fixture['backup_id']}"
    parent = evidence.out / "independent-restore"
    parent.mkdir()
    started = time.perf_counter()
    with service(args.binary, parent / "control", evidence.out, "restore_control", cpus, evidence, report) as (control, _):
        copy_verified(backup_root / archive, parent / "control" / archive)
        restore = api(evidence, control, "independent_restore", "restore_A", "POST", f"{API}/service-restores",
                      {"operation_id": oid(epoch, "restore-A"), "backup_id": fixture["backup_id"]}, detail=True)
    restored = Path(restore["target_root"]).resolve()
    if restore["started"] or restored.parent != parent or not restored.is_dir():
        raise RuntimeError("restore did not return a new non-serving root")
    with service(args.binary, restored, evidence.out, "restored_A", cpus, evidence, report) as (server, _):
        verify(evidence, server, "restored_A", fixture, "A")
        report["independent_backup_restore"] = {
            "status": "verified", "cut": "A", "archive_objects_sha256": digest(restore["snapshot"]["objects"]),
            "restore_and_verify_ms": (time.perf_counter() - started) * 1000,
            "separate_device": report["independent_backup_media_verified"],
        }
    probe_corrupt_archive(args, evidence, report, cpus, archive, backup_root, epoch, fixture)
