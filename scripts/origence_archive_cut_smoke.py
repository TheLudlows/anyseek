#!/usr/bin/env python3
"""Compare a paired Origence backup before and after an AnySeek archive-cut restore."""
import argparse
import json
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ("origence-repo", "origence-binary", "anyseek-binary", "archive-tool"):
        parser.add_argument("--" + option, type=lambda value: Path(value).resolve(), required=True)
    parser.add_argument("--chain", action="store_true", help="capture A–B and B–C in separate retained WAL generations")
    parser.add_argument("--automatic", action="store_true", help="let the live checkpoint publish both archive windows")
    args = parser.parse_args()
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(args.origence_repo / "tools"))
    from private_archive_smoke import (
        ANSWER, MARKER, Proxy, command, expect, native, new_turn, peer, request, wait_for,
    )
    from mcp_smoke import Host, isolated_env

    class ObserveGroups(Proxy):
        def read_json(self):
            self.observed_request = super().read_json()
            return self.observed_request

        def reply(self, status, value, upstream_headers=None):
            if status == 200 and self.path.endswith("/group/replace"):
                self.server.groups.append((
                    self.path,
                    self.headers["X-AnySeek-Tenant"],
                    self.observed_request,
                    value,
                ))
            super().reply(status, value, upstream_headers)

    def archive(*values, ok=True):
        result = subprocess.run([args.archive_tool, *map(str, values)],
                                capture_output=True, timeout=120)
        if ok and result.returncode:
            raise subprocess.CalledProcessError(result.returncode, result.args, result.stdout, result.stderr)
        assert (result.returncode == 0) == ok
        return result.stdout

    def write_turn_b(env, token, forbidden, route, proxy, base_seq):
        members = new_turn(MARKER + " at B")
        with Host(args.origence_binary, env, token, forbidden, []) as origence:
            origence.request(route, members)
        _, _, write, receipt = proxy.groups[-1]
        assert receipt["atomic"] and receipt["receipt"]["record_count"] == 2
        assert receipt["receipt"]["seq"] == base_seq + 1
        expected = command(args.origence_binary, env, "archive-verify", "--isolated-copy")
        assert expected["archived"] == 2
        return members, write, receipt, expected

    with tempfile.TemporaryDirectory(prefix="anyseek-origence-cut-") as directory:
        root = Path(directory)
        backend = root / "anyseek"
        authority = root / "origence"
        authority.mkdir()
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        secret = secrets.token_hex(32)
        with peer(ObserveGroups) as (proxy, proxy_url):
            proxy.groups = []
            proxy.action = None
            env = isolated_env(str(authority))
            env["OC_ENABLE_MODELS"] = "false"
            env["OC_ANYSEEK_URL"] = proxy_url
            with native(args.anyseek_binary, backend, port, secret) as (upstream, key):
                proxy.upstream = upstream
                env["OC_ANYSEEK_API_KEY"] = key
                owner = command(args.origence_binary, env, "workspace-create", "archive cut fixture")
                token = owner["token"]
                forbidden = [token, key, MARKER, ANSWER]
                with Host(args.origence_binary, env, token, forbidden, []) as origence:
                    session = origence.request("/v1/session")["session_id"]
                    route = f"/v1/sessions/{session}/turns"
                    origence.request(route, new_turn("base synthetic question", "base answer"))
                    group_path, tenant, _, base_reply = proxy.groups[-1]
                    headers = {"apikey": key, "X-AnySeek-Tenant": tenant}
                    collection = group_path.removesuffix("/group/replace")
                    expect(request(upstream, collection, "PATCH", headers=headers), 200)
                    backup = expect(request(upstream, collection + "/backup", "POST", headers=headers,
                                            body={"operation_id": {"epoch": int(time.time()) // 86400,
                                                                   "nonce": "synthetic-base"}}), 200)
                    assert backup["visible_seq"] == base_reply["receipt"]["seq"]
                if not args.automatic:
                    members, write_b, receipt_b, expected = write_turn_b(
                        env, token, forbidden, route, proxy, backup["visible_seq"])
            private_collection = collection.rsplit("/", 1)[1]
            candidates = list((backend / "collections").glob(f"*/{private_collection}/*"))
            assert len(candidates) == 1
            source = candidates[0]
            base = backend / ("backup-" + backup["backup_id"])
            budget = 256 * 1024 * 1024
            if args.automatic:
                medium = root / "archive-medium"
                medium.mkdir()
                base = Path(shutil.copytree(base, medium / "base-a"))
                stream = Path(json.loads(archive("enroll", source, base, medium, budget, 2 * budget, 2)))
                before = set(source.glob("generation-*"))
                assert len(before) == 1
                with native(args.anyseek_binary, backend, port, secret) as (upstream, _):
                    proxy.upstream = upstream
                    members, write_b, receipt_b, expected = write_turn_b(
                        env, token, forbidden, route, proxy, backup["visible_seq"])
                    expect(request(upstream, collection, "PATCH", headers=headers), 200)
                assert before.isdisjoint(source.glob("generation-*"))
            cut = receipt_b["receipt"]["seq"]
            # These are stopped B copies. C is applied only to the disposable live pair.
            saved_authority = root / "origence-at-b"
            saved_backend = root / "anyseek-at-b"
            shutil.copytree(authority, saved_authority)
            shutil.copytree(backend, saved_backend)
            window = root / "archive"
            if args.chain and not args.automatic:
                first_path = root / "first"
                first = json.loads(archive("capture", source, base, first_path, budget))
                assert (first["base_seq"], first["last_seq"]) == (backup["visible_seq"], cut)
            with native(args.anyseek_binary, backend, port, secret) as (upstream, _):
                proxy.upstream = upstream
                if args.chain and not args.automatic:
                    expect(request(upstream, collection, "PATCH", headers=headers), 200)
                    middle_backup = expect(request(upstream, collection + "/backup", "POST", headers=headers,
                                                   body={"operation_id": {"epoch": int(time.time()) // 86400,
                                                                          "nonce": "synthetic-middle"}}), 200)
                    assert middle_backup["visible_seq"] == cut
                with Host(args.origence_binary, env, token, forbidden, []) as origence:
                    target = route + "/" + members["turn_id"]
                    expect(request(origence.base, target, "DELETE", token), 200)
                    expect(request(origence.base, target, token=token), 404)
                    wait_for(lambda: any(row[3]["receipt"]["seq"] > cut
                                         and row[3]["receipt"]["record_count"] == 0 for row in proxy.groups))
                    _, _, _, receipt_c = proxy.groups[-1]
                    assert receipt_c["receipt"]["seq"] == cut + 1
                if args.automatic:
                    expect(request(upstream, collection, "PATCH", headers=headers), 200)
            assert any(anchor.joinpath("primary-pages").is_file()
                       and anchor.joinpath("records-0000000000000000").is_file()
                       for anchor in source.glob("anchor-*"))
            if args.chain or args.automatic:
                if args.automatic:
                    parts = json.loads(archive("manifest", stream, budget))
                    assert len(parts) == 2
                    first, second = (part["witness"] for part in parts)
                else:
                    middle = backend / ("backup-" + middle_backup["backup_id"])
                    second = json.loads(archive("capture", source, middle, window, budget))
                    parts = [{"path": str(first_path), "witness": first},
                             {"path": str(window), "witness": second}]
                assert (first["base_seq"], first["last_seq"]) == (backup["visible_seq"], cut)
                assert (second["base_seq"], second["last_seq"]) == (cut, cut + 1)
                info = {"base_seq": first["base_seq"], "last_seq": second["last_seq"]}
                manifest = root / "parts.json"
                rejected = root / "rejected-partition"
                # Even though B is in the first part, an incomplete declared
                # chain cannot publish a new partition.
                manifest.write_text(json.dumps([parts[0], dict(parts[1], path=str(root / "missing"))]))
                archive("restore-chain", base, rejected, budget, cut, manifest, ok=False)
                assert not rejected.exists()
                manifest.write_text(json.dumps(parts[1:]))
                archive("restore-chain", base, rejected, budget, cut, manifest, ok=False)
                assert not rejected.exists()
                manifest.write_text(json.dumps(parts))
            else:
                witness = root / "witness.json"
                witness.write_bytes(archive("capture", source, base, window, budget))
                info = json.loads(witness.read_bytes())
                assert (info["base_seq"], info["last_seq"]) == (backup["visible_seq"], cut + 1)
            # Restore the same private partition identity into the isolated B Catalog.
            partition = saved_backend / source.relative_to(backend)
            partition.rename(root / "original-b-partition")
            if args.chain or args.automatic:
                archive("restore-chain", base, partition, budget, cut, manifest)
            else:
                archive("restore", base, window, partition, budget, cut, witness)
            with native(args.anyseek_binary, saved_backend, port, secret) as (upstream, _):
                proxy.upstream = upstream
                replayed = expect(request(upstream, group_path, "POST", headers=headers, body=write_b), 200)
                assert replayed == receipt_b
                for item in write_b["records"]:
                    record = expect(request(upstream, collection + "/doc/" + item["record_id"],
                                            headers=headers), 200)
                    assert record["document"] == item["document"]
                restored_env = dict(env, OC_DATA_DIR=str(saved_authority))
                actual = command(args.origence_binary, restored_env, "archive-verify", "--isolated-copy")
                assert actual["directory_sha256"] == expected["directory_sha256"]
                assert actual["archived"] == expected["archived"]
                with Host(args.origence_binary, restored_env, token, forbidden, []) as origence:
                    assert origence.request(target)["answer"] == ANSWER
            print(json.dumps({"base_seq": info["base_seq"], "restored_seq": cut,
                              "withdrawn_seq": info["last_seq"], "directory_sha256": actual["directory_sha256"]}))


if __name__ == "__main__":
    main()
