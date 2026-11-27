#!/usr/bin/env python3
"""Run the paired Origence archive probe against actual, independent local HTTP processes."""
import argparse
from pathlib import Path
import secrets
import shutil
import socket
import subprocess
import sys
import time
import urllib.error

from measure import file_digest, output_json


def probe(args, out):
    sys.path.insert(0, str(args.origence_repo.resolve() / "tools"))
    from private_archive_smoke import (ANSWER, Host, Model, command, isolated_env, native,
                                       new_turn, peer, request)

    binary = str(args.origence_binary.resolve(strict=True))
    anyseek = str(args.anyseek_binary.resolve(strict=True))
    authority = out / "origence"
    archive = out / "native"
    authority.mkdir()
    report = {
        "origence_revision": subprocess.check_output(
            ["git", "-C", str(args.origence_repo), "rev-parse", "HEAD"], text=True).strip(),
        "origence_binary_sha256": file_digest(Path(binary)),
        "anyseek_binary_sha256": file_digest(Path(anyseek)),
        "external_anyseek": True,
        "status": "running",
    }

    def save():
        output_json(out / "report.json", report)

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    secret = secrets.token_hex(32)
    try:
        with peer(Model) as (_, model):
            env = isolated_env(str(authority))
            env.update(OC_ENABLE_MODELS="true", OC_EMBEDDING_BASE_URL=model + "/v1",
                       OC_EMBEDDING_MODEL="synthetic-vector", OC_EMBEDDING_DIMENSION="3")
            with native(anyseek, archive, port, secret) as (base, key):
                env.update(OC_ANYSEEK_URL=base, OC_ANYSEEK_API_KEY=key)
                actor = command(binary, env, "workspace-create", "issue229 paired recovery")
                token = actor["token"]
                forbidden = [token, key, ANSWER]
                report["workspace_created"] = True
                headers = {"apikey": key, "X-AnySeek-Tenant": actor["tenant_id"]}
                try:
                    with Host(binary, env, token, forbidden) as host:
                        report["service_start"] = "ready"
                        session = host.request("/v1/session")["session_id"]
                        turns = f"/v1/sessions/{session}/turns"
                        a = new_turn("Synthetic record A at the base cut.")
                        b = new_turn("Synthetic record B at the recovery cut.")
                        for item in (a, b):
                            status, response = request(host.base, turns, "POST", token, item)
                            report.setdefault("archive_writes", []).append({"status": status, "response": response})
                            if status not in (200, 201):
                                break
                        saved = len(report["archive_writes"]) == 2 and all(
                            item["status"] in (200, 201) for item in report["archive_writes"])
                        status, reply = request(host.base, "/v1/search", "POST", token,
                                                {"query": "issue229", "mode": "keyword"})
                        report["asset_search"] = {"status": status, "response": reply}
                finally:
                    native_status, entries = request(base, "/api/v1/index", headers=headers)
                    report["anyseek_collection_listing"] = {"status": native_status, "entries": entries}
                    if native_status == 200:
                        details = {}
                        for entry in entries:
                            code, info = request(base, "/api/v1/index/" + entry["id"], headers=headers)
                            details[entry["id"]] = {"status": code, "info": info}
                        report["anyseek_collection_info"] = details
                if not saved:
                    report["status"] = "blocked_before_paired_backup"
                    return report
                before = command(binary, env, "archive-verify", "--isolated-copy")
                report["B_manifest"] = before
            # Both source servers are stopped from here. Preserve the original
            # bound URL while the restored AnySeek root starts on the same port.
            restored_auth = out / "restored-origence"
            restored_native = out / "restored-anyseek"
            shutil.copytree(authority, restored_auth)
            shutil.copytree(archive, restored_native)
            env.update(OC_DATA_DIR=str(restored_auth))
            with native(anyseek, restored_native, port, secret):
                after = command(binary, env, "archive-verify", "--isolated-copy")
                report["restored_manifest"] = after
                if (before != after or len(before["directory_sha256"]) != 64
                        or before["archived"] != 2):
                    raise RuntimeError("paired restore manifest mismatch")
                with Host(binary, env, token, forbidden) as host:
                    listing = host.request(turns)
                    contents = host.request(turns + "/" + b["turn_id"])
                    if (sorted(t["sequence"] for t in listing["turns"]) != [1, 2]
                            or contents["answer"] != ANSWER or contents["question"] != b["question"]):
                        raise RuntimeError("restored turn ordering or body mismatch")
                    target = turns + "/" + b["turn_id"]
                    withdrawn, _ = request(host.base, target, "DELETE", token)
                    missing, _ = request(host.base, target, token=token)
                    if (withdrawn, missing) != (200, 404):
                        raise RuntimeError("restored turn withdrawal did not take effect")
                    report["restored_http"] = {"listed_sequences": [1, 2], "read_matches": True,
                                               "withdrawal": withdrawn, "post_withdrawal": missing}
            report["status"] = "paired_restore_verified"
            save()

            # Provision normal assets only after verifying the archive pair.
            # A fatal ordinary-asset worker error must not erase archive evidence.
            fresh = out / "asset-anyseek"
            separate = out / "asset-origence"
            separate.mkdir()
            asset_env = dict(env, OC_DATA_DIR=str(separate))
            with native(anyseek, fresh, port, secret) as (base, key):
                asset_actor = command(binary, asset_env, "workspace-create", "issue229 ordinary asset")
                asset_token = asset_actor["token"]
                with Host(binary, asset_env, asset_token, [asset_token, key]) as host:
                    status, reply = request(host.base, "/v1/knowledge", "POST", asset_token,
                                            {"title": "issue229", "content": "isolated synthetic source",
                                             "format": "text"})
                    asset = {"http_status": status, "response": reply}
                    report["asset_publication"] = asset
                    if status in (200, 201, 202) and "job_id" in reply:
                        deadline = time.monotonic() + 35
                        try:
                            while time.monotonic() < deadline:
                                _, job = request(host.base, "/v1/jobs/" + reply["job_id"],
                                                 token=asset_token)
                                if job.get("state") in {"completed", "failed", "cancelled", "superseded"}:
                                    asset["job"] = job
                                    if job["state"] == "completed" and job.get("outcome") == "published":
                                        code, search = request(
                                            host.base, "/v1/search", "POST", asset_token,
                                            {"query": "synthetic", "mode": "keyword"},
                                        )
                                        if code != 200 or not any(
                                            hit["asset_id"] == reply["asset_id"]
                                            and hit["content"] == "isolated synthetic source"
                                            for hit in search["hits"]
                                        ):
                                            raise RuntimeError("published asset was not searchable")
                                        asset["search_after_publication"] = {"status": code, "response": search}
                                    break
                                time.sleep(.15)
                        except urllib.error.URLError:
                            asset["process_exit"] = host.process.wait(timeout=5)
                            host.stderr.seek(0)
                            stderr = host.stderr.read().decode("utf-8", errors="replace")
                            for secret_value in (key, asset_token, str(separate)):
                                stderr = stderr.replace(secret_value, "[redacted]")
                            asset["process_log_tail"] = stderr[-3000:]
                    headers = {"apikey": key, "X-AnySeek-Tenant": asset_actor["tenant_id"]}
                    code, entries = request(base, "/api/v1/index", headers=headers)
                    asset["native_index_status"] = code
                    if code == 200:
                        asset["native_collections"] = entries
            return report
    except Exception as error:
        report["status"] = "failed"
        report["failure"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--origence-repo", type=Path, required=True)
    parser.add_argument("--origence-binary", type=Path, required=True)
    parser.add_argument("--anyseek-binary", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    out = args.out.resolve()
    out.mkdir(exist_ok=False)
    probe(args, out)


if __name__ == "__main__":
    main()
