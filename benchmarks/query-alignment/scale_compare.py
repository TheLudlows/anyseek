"""Compare local Release servers on identical checkpointed fixtures."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
import uuid

from support import Sampler, Server, digest, quantiles, wait


ROUTE = "/api/v1/index/bench"
SIZES = (10_000, 50_000, 100_000)
REQUESTS = 96
SERVER_CPUS = (0, 1)
CLIENT_CPUS = (2, 3)
ORDER = ("base", "final", "final", "base")
SYNTHETIC = (
    ("miss-count", {"query": "absentterm", "length": 0, "count": True}, (1, 8)),
    ("rare-top-count", {"query": "rareterm", "length": 16, "count": True}, (1, 4, 8)),
    ("dense-top-count", {"query": "common", "length": 16, "count": True}, (1, 4, 8)),
    ("dense-top", {"query": "common", "length": 16, "count": False}, (4,)),
    ("hybrid-rare", {
        "query": "rareterm", "query_vector": [1, 0], "all": True, "length": 16, "count": True,
        "rrf": {"constant": 60.0, "keyword_candidates": 32, "vector_candidates": 48},
    }, (4,)),
)
MIRACL = (
    ("miss-count", {"query": "zzqneverlinkedanyseekbenchmark", "length": 0, "count": True}, (1, 8)),
    ("rare-top-count", {"query": "francisco", "length": 10, "count": True}, (1, 4)),
    ("dense-top-count", {"query": "中国", "length": 10, "count": True}, (1, 4)),
)
SCHEMA = [
    {"field": "body", "field_type": "Text", "store": True, "index_lexical": True},
    {"field": "priority", "field_type": "Int", "store": True, "filterable": True, "index_lexical": False},
    {"field": "vector", "field_type": "Json", "store": True, "index_lexical": False, "index_vector": True},
]


class ScaleServer(Server):
    tenant = "bench"


def operation(nonce=None):
    return {"epoch": int(time.time() // 86400), "nonce": nonce or uuid.uuid4().hex}


def save(path, row):
    with path.open("a") as stream:
        stream.write(json.dumps(row, separators=(",", ":")) + "\n")


def seed(base, builder, directory, size):
    directory.mkdir()
    data = directory / "db"
    server = ScaleServer(base, data, directory / "create.log", SERVER_CPUS)
    try:
        server.call("POST", "/api/v1/index", {
            "operation_id": operation(), "collection_id": "bench",
            "embedding_space_id": "scale-fixed", "config": {
                "index_name": "Published query scale", "schema": SCHEMA,
                "tokenizer": "TantivySimpleV1", "vector": {"dimensions": 2},
            },
        })
    finally:
        server.close()
    partition, = (data / "collections/bench/bench").iterdir()
    env = {**os.environ, "ANYSEEK_LEXICAL_FIXTURE_PARTITION": str(partition),
           "ANYSEEK_LEXICAL_FIXTURE_ROWS": str(size), "TOKIO_WORKER_THREADS": "2", "RAYON_NUM_THREADS": "2"}
    with (directory / "fixture.log").open("w") as log:
        subprocess.run([
            "taskset", "-c", ",".join(map(str, SERVER_CPUS)), str(builder),
            "record::lexical::visibility::tests::prepare_lexical_scale_fixture", "--ignored", "--exact", "--nocapture",
        ], env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
    assert "1 passed" in (directory / "fixture.log").read_text()
    return data


def signature(response, normalize_ingestion=False):
    stable = {key: response[key] for key in ("hits", "count", "count_total", "visible_seq", "query_terms", "statistics")}
    stable["statistics"] = {key: value for key, value in response["statistics"].items() if key != "evaluation_time_ms"}
    if normalize_ingestion:
        stable["hits"] = [
            {**hit, "record": {key: value for key, value in hit["record"].items() if key != "ingested_at_ms"}}
            for hit in response["hits"]
        ]
    return digest(stable)


def reads(server, output, tags, request, concurrency, signatures):
    first, _ = server.call("POST", ROUTE + "/query", request)
    assert first["visible_seq"] == tags["n"], tags
    mode, size, corpus = tags["mode"], tags["n"], tags["corpus"]
    exact = {"miss-count": 0, "rare-top-count": 100 if corpus == "synthetic" else 6}
    if corpus == "synthetic":
        exact.update({"dense-top-count": size, "hybrid-rare": size})
    if mode in exact:
        assert first["count_total"] == exact[mode], (tags, first["count_total"])
    fingerprint = signature(first)
    assert fingerprint == signatures.setdefault(mode, fingerprint), tags

    def run(_):
        result, elapsed = server.call("POST", ROUTE + "/query", request, allowed=(200, 429))
        return {"http_ms": elapsed, "status": server.last_status}, result

    with ThreadPoolExecutor(concurrency) as pool:
        warmups = list(pool.map(run, range(max(8, 2 * concurrency))))
        with Sampler(server) as resources:
            start = time.perf_counter()
            responses = list(pool.map(run, range(REQUESTS)))
            wall = time.perf_counter() - start
    for sample, result in warmups + responses:
        if sample["status"] == 200:
            assert signature(result) == fingerprint, tags
        else:
            assert result["code"] == "busy", (tags, result)
    samples = [sample for sample, _ in responses]
    successes = [sample["http_ms"] for sample in samples if sample["status"] == 200]
    assert successes, tags
    row = dict(**tags, concurrency=concurrency, requests=len(samples), ok=len(successes),
               hit_count=first["count_total"], qps=len(successes) / wall, result_digest=fingerprint,
               **quantiles(successes), **resources.summary())
    save(output / "read.jsonl", row)
    save(output / "samples.jsonl", {"tags": row, "samples": samples})
    print(json.dumps({key: row[key] for key in (*tags, "concurrency", "ok", "p95_ms", "qps")}), flush=True)


def writes(server, output, tags, signatures):
    samples = []
    with Sampler(server) as resources:
        start = time.perf_counter()
        for batch in range(8):
            records = [{
                "operation_id": operation(f"scale-edit-{index}"), "record_id": f"r{index:06}",
                "expected": {"Match": 1}, "document": None if index % 4 == 0 else {
                    "body": "common changedterm", "priority": index, "vector": [1.0, 0.0],
                },
            } for index in range(batch * 8, (batch + 1) * 8)]
            before = time.perf_counter()
            outcome, elapsed = server.call("POST", ROUTE + "/ingest", {"records": records})
            cut = tags["n"] + (batch + 1) * 8
            assert len(outcome["results"]) == 8 and all(row["code"] == "committed" for row in outcome["results"]), outcome
            assert outcome["results"][-1]["receipt"]["seq"] == cut
            progress = wait(server, "bench", cut)["progress"]
            samples.append({"http_ms": elapsed, "indexed_ms": (time.perf_counter() - before) * 1000,
                            "watermarks": progress})
        wall = time.perf_counter() - start
    for term, count in (("rareterm", 36), ("changedterm", 48), ("common", tags["n"] - 16)):
        result, _ = server.call("POST", ROUTE + "/query", {"query": term, "length": 16, "count": True})
        assert result["count_total"] == count and result["visible_seq"] == tags["n"] + 64, (tags, term)
        fingerprint = signature(result, normalize_ingestion=True)
        assert fingerprint == signatures.setdefault(f"after-{term}", fingerprint), (tags, term)
    save(output / "write.jsonl", dict(**tags, **resources.summary(), wall_s=wall, batches=samples))


def measure(binaries, output, corpus, size, source, workloads):
    signatures = {}
    for ordinal, arm in enumerate(ORDER):
        directory = output / f"{corpus}-{size}-{ordinal}-{arm}"
        shutil.copytree(source, directory / "db")
        server = ScaleServer(binaries[arm], directory / "db", directory / "server.log", SERVER_CPUS)
        try:
            progress, _ = server.call("GET", ROUTE + "/ingest")
            assert all(progress[key] == size for key in ("committed_seq", "lexical_seq", "vector_seq")), progress
            assert not progress["index_error"] and not progress["recovery_required"], progress
            tags = dict(corpus=corpus, n=size, arm=arm, round=ordinal // 2 + 1)
            for mode, query, levels in workloads:
                for concurrency in levels:
                    reads(server, output, dict(**tags, mode=mode), query, concurrency, signatures)
            assert "event=worker_started" not in (directory / "server.log").read_text(), tags
            if corpus == "synthetic" and size < 100_000:
                writes(server, output, tags, signatures)
        finally:
            server.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ("base", "candidate", "output"):
        parser.add_argument(f"--{flag}", required=True, type=lambda path: Path(path).resolve())
    seeds = parser.add_mutually_exclusive_group(required=True)
    seeds.add_argument("--fixture-test", type=Path, help="candidate Release library test executable")
    seeds.add_argument("--synthetic-seeds", type=Path, help="directory containing idle seed-{size}/db fixtures")
    parser.add_argument("--miracl-seed", type=Path, help="optional 30,416-record MIRACL bench collection root")
    args = parser.parse_args()
    os.sched_setaffinity(0, CLIENT_CPUS)
    args.output.mkdir(parents=True)
    binaries = {"base": args.base, "final": args.candidate}
    with (args.output / "provenance.json").open("w") as stream:
        hashes = {}
        for arm, binary in binaries.items():
            with binary.open("rb") as executable:
                hashes[arm] = hashlib.file_digest(executable, "sha256").hexdigest()
        json.dump({
            "binaries": hashes, "server_cpus": SERVER_CPUS, "client_cpus": CLIENT_CPUS,
            "samples_per_phase": REQUESTS, "order": ORDER,
            "cpu_max": Path("/sys/fs/cgroup/cpu.max").read_text().strip(),
            "memory_max": Path("/sys/fs/cgroup/memory.max").read_text().strip(),
            "patterns": SYNTHETIC, "real_patterns": MIRACL if args.miracl_seed else (),
        }, stream, indent=2)
    if args.fixture_test:
        fixtures = {size: seed(args.base, args.fixture_test.resolve(), args.output / f"seed-{size}", size) for size in SIZES}
    else:
        fixtures = {size: args.synthetic_seeds / f"seed-{size}" / "db" for size in SIZES}
    for size, source in fixtures.items():
        measure(binaries, args.output, "synthetic", size, source, SYNTHETIC)
    if args.miracl_seed:
        measure(binaries, args.output, "miracl", 30_416, args.miracl_seed, MIRACL)


if __name__ == "__main__":
    main()
