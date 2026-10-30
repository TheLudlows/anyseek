#!/usr/bin/env python3
"""Bounded final-build asynchronous comparison; outputs raw evidence outside git.

Both services run sequentially on CPU 0–1, client on 2–3. This measures small
batch completion latency, not saturated ingestion throughput. AnySeek replies
with committed receipts; Meili 202 is acceptance only. Indexes are checked separately.
"""
import argparse
import hashlib
import json
import os
import re
from pathlib import Path
import time
from http_engine import Engine, doc, dump, op, stats, disk


def request_with_busy_wait(e, method, path, body=None, timeout=180):
    """Retry explicit Busy, preserving all original identities and request bodies.

    Every attempt remains in the raw log. Unknown/transport outcomes are never
    retried. Returned wall time includes rejected attempts and their backoff.
    """
    started = time.perf_counter()
    deadline = time.monotonic() + timeout
    attempts = []
    while True:
        code, result, ms, size = e.request(method, path, body, required=False)
        attempt = {'status': code, 'http_ms': ms, 'response': result}
        attempts.append(attempt)
        e.event('http_attempt', method=method, path=path, attempt=len(attempts), **attempt)
        if 200 <= code < 300:
            return code, result, (time.perf_counter() - started) * 1000, attempts
        if (e.name != 'anyseek' or code != 429 or result.get('code', (result.get('error') or {}).get('code')) != 'busy'
                or time.monotonic() >= deadline):
            raise RuntimeError(('request failed; no unknown-outcome retry', method, path, attempts))
        time.sleep(min(.05, max(0, deadline - time.monotonic())))


def summarize_maintenance(log_text):
    """Report observed intervals; missing evidence is null, never zero duration."""
    phases = {}
    for name, elapsed in re.findall(
            r'record_maintenance phase=(\w+) seq=\d+ event=exit elapsed_us=(\d+)', log_text):
        phases.setdefault(name, []).append(int(elapsed) / 1000)
    measured = {name: {**stats(values), 'max_ms': max(values)}
                for name, values in sorted(phases.items())}
    grouped = {'locked': {}, 'unlocked': {}, 'unclassified': {}}
    for name, values in measured.items():
        group = ('unlocked' if name.endswith('_unlocked') else
                 'locked' if name.endswith('_locked') else 'unclassified')
        grouped[group][name] = values
    required = [
        'intake_snapshot_locked', 'intake_selection_locked',
        'intake_local_prepare_unlocked', 'intake_local_release_unlocked',
        'cache_merge_capture_locked', 'cache_merge_build_unlocked',
        'cache_merge_install_locked', 'cache_merge_cleanup_locked',
    ]
    # HTTP PATCH uses JobMode::Index. Native handoff/membership are conditional
    # JobMode::Compact work and cannot be required by this HTTP-only workload.
    explicit_compact = ['compact_tail_handoff_locked', 'compact_native_membership_capture_locked']
    coverage = {name: len(phases[name]) if name in phases else None for name in required}
    return {
        'maintenance_phases': measured,
        'maintenance_phases_by_lock_scope': grouped,
        'maintenance_coverage': coverage,
        'missing_required_maintenance_phases': [name for name, count in coverage.items() if count is None],
        'required_maintenance_observed': all(count is not None for count in coverage.values()),
        'optional_explicit_compact_coverage': {
            name: len(phases[name]) if name in phases else None for name in explicit_compact},
        'maintenance_phase_caveat': (
            'Named intervals may overlap; locked phases cover only the instrumented critical-section '
            'interval, not mutex wait time. Unclassified phases have no asserted lock scope. '
            'An exit event includes errors/cancellation and does not prove successful installation. '
            'Missing coverage is null and has no timing statistics. Explicit Compact phases are '
            'optional: the HTTP PATCH in this workload runs Index, not Compact.'),
    }


def run(engine, rows, queries, out, batch_size):
    out.mkdir(parents=True, exist_ok=False)
    e = Engine(engine, 'restricted', out)
    summary = {'engine': engine, 'documents': len(rows), 'batch_size': batch_size,
               'status': 'running', 'server_cpus': [0, 1], 'client_cpus': [2, 3],
               'cpu_max': Path('/sys/fs/cgroup/cpu.max').read_text().strip(),
               'memory_max': Path('/sys/fs/cgroup/memory.max').read_text().strip()}
    submits, completes, http_submits, business_completes = [], [], [], []
    summary['submit_attempts'] = 0
    summary['submit_busy_rejections'] = 0
    summary['write_ack_contract'] = 'committed_receipts' if engine == 'anyseek' else 'accepted_task'
    summary['acknowledged_records'] = 0
    summary['verified_business_receipts'] = 0
    summary['verified_meili_tasks'] = 0
    summary['completion_observation_model'] = (
        'HTTP response with a committed receipt for every item. Separate index '
        'timing observes lexical/vector watermarks; search is validated by later queries.'
        if engine == 'anyseek' else
        'Successful Meili task is the shared business/index-completion observation; no independently '
        'exposed business/lexical/vector watermarks are measured. Search is validated by later queries.')
    summary['log_filter'] = os.environ.get('RUST_LOG')
    try:
        summary['startup_ms'] = e.start()
        e.create()
        e.phase = 'load'
        started = time.perf_counter()
        for offset in range(0, len(rows), batch_size):
            free = os.statvfs(out)
            if free.f_bavail * free.f_frsize < 4 * 1024**3:
                raise RuntimeError('4GiB free-space reserve reached; all data retained')
            if disk(out.parent)['allocated_bytes'] > 1536 * 1024**2:
                raise RuntimeError('1.5GiB run budget reached; all data retained')
            docs = [doc(r) for r in rows[offset:offset + batch_size]]
            batch_started = time.perf_counter()
            if engine == 'anyseek':
                writes = []
                for d in docs:
                    e.seq += 1
                    writes.append({'operation_id': op(e.seq), 'record_id': d['id'],
                                   'routing_key': 'benchmark', 'expected': 'Any', 'document': d})
                code, result, ms, attempts = request_with_busy_wait(e, 'POST', '/api/v1/index/bench/ingest', {'records': writes})
                e.event('accept', offset=offset, code=code, result=result, ms=ms)
                assert code == 200 and len(result['results']) == len(writes), result
                assert all(r['code'] == 'committed' and r.get('receipt') for r in result['results']), result
                summary['acknowledged_records'] += len(writes)
                summary['submit_attempts'] += len(attempts)
                summary['submit_busy_rejections'] += len(attempts) - 1
                http_submits.append(attempts[-1]['http_ms'])
                submits.append(ms)
                batch_cut = max(r['receipt']['seq'] for r in result['results'])
                business_ms = (time.perf_counter() - batch_started) * 1000
                business_completes.append(business_ms)
                e.event('business_completion_observed', offset=offset, committed_cut=batch_cut, elapsed_ms=business_ms)
                until = time.monotonic() + 180
                while True:
                    progress = e.request('GET', '/api/v1/index/bench/ingest')[1]
                    summary['last_ingest_progress'] = progress
                    if progress['recovery_required'] or progress['index_error']:
                        raise RuntimeError(progress)
                    if (progress['lexical_seq'] >= batch_cut
                            and progress['vector_seq'] >= batch_cut):
                        break
                    if time.monotonic() >= until:
                        raise RuntimeError(('completion timeout', progress))
                    time.sleep(.005)
                # Every accepted operation must have a successful business outcome.
                # This validation is excluded from each batch's completion timer.
                complete_ms = (time.perf_counter() - batch_started) * 1000
                e.event('index_watermarks_observed', offset=offset, durable_cut=batch_cut,
                        elapsed_ms=complete_ms, progress=progress)
                for w in writes:
                    oid = w['operation_id']
                    status = e.request('GET', f"/api/v1/index/bench/ingest/operation/{oid['epoch']}/{oid['nonce']}")[1]
                    assert status and status['receipt'], status
                    summary['verified_business_receipts'] += 1
                e.event('published', progress=progress, business_elapsed_ms=business_ms,
                        index_watermarks_elapsed_ms=complete_ms)
            else:
                code, result, ms, attempts = request_with_busy_wait(e, 'POST', '/indexes/bench/documents', docs)
                assert code == 202, result
                summary['acknowledged_records'] += len(docs)
                summary['submit_attempts'] += len(attempts)
                summary['submit_busy_rejections'] += len(attempts) - 1
                http_submits.append(attempts[-1]['http_ms'])
                submits.append(ms)
                task = e.task(result)
                complete_ms = (time.perf_counter() - batch_started) * 1000
                business_completes.append(complete_ms)
                summary['verified_meili_tasks'] += 1
                e.event('accept_and_complete', offset=offset, result=result, task=task, ms=ms,
                        business_elapsed_ms=complete_ms, index_completion_elapsed_ms=complete_ms)
            completes.append(complete_ms)
            print(engine, 'completed', offset + len(docs), flush=True)
        summary['load_wall_seconds_including_validation'] = time.perf_counter() - started
        summary['batch_acknowledgement_including_busy_wait'] = stats(submits)
        summary['successful_acknowledgement_http_only'] = stats(http_submits)
        summary['batch_business_completion_observed'] = stats(business_completes)
        summary['batch_index_watermarks_observed'] = stats(completes)
        summary['batch_published_completion'] = stats(completes)
        summary['summed_batch_completion_seconds'] = sum(completes) / 1000
        summary['completion_documents_per_second'] = len(rows) / (sum(completes) / 1000)
        summary['visible_count'] = e.count()
        assert summary['visible_count'] == len(rows), summary
        # Same representative corpus title queried on each engine after publishing.
        checks = []
        for row in rows[:8]:
            token = row['title'].strip()
            if not token:
                continue
            code, response, ms, _ = e.query(token)
            checks.append({'query': token, 'code': code, 'expected_docid': row['docid'],
                           'expected_found': row['docid'] in e.hit_ids(response), 'hits': e.hit_ids(response)})
            assert code == 200, response
        dump(out / 'representative-searches.json', checks)
        assert any(c['expected_found'] for c in checks), 'no representative search found its source document'
        e.phase = 'query'
        measurements = []
        for query in queries:
            code, result, ms, _ = e.query(query['text'])
            measurements.append({'qid': query['qid'], 'status': code, 'ms': ms, 'hits': e.hit_ids(result)})
        dump(out / 'query-results.json', measurements)
        summary['query_latency'] = stats([q['ms'] for q in measurements])
        summary['query_errors'] = sum(q['status'] != 200 for q in measurements)
        summary['query_empty'] = sum(not q['hits'] for q in measurements)
        assert summary['query_errors'] == 0
        e.phase = 'checkpoint'
        checkpoint_started = time.perf_counter()
        if engine == 'anyseek':
            code, result, elapsed, attempts = request_with_busy_wait(e, 'PATCH', '/api/v1/index/bench')
            summary['checkpoint'] = result
            summary['checkpoint_attempts'] = len(attempts)
            summary['checkpoint_busy_rejections'] = len(attempts) - 1
        else:
            summary['checkpoint'] = e.checkpoint()
        summary['checkpoint_ms'] = (time.perf_counter() - checkpoint_started) * 1000
        e.stop()
        reopen_started = time.perf_counter()
        summary['reopen_ms'] = e.start()
        summary['reopen_health_ms'] = summary['reopen_ms']
        summary['reopened_count'] = e.count()
        assert summary['reopened_count'] == len(rows)
        summary['reopen_verified_count_ms'] = (time.perf_counter() - reopen_started) * 1000
        representative = next(check for check in checks if check['expected_found'])
        code, response, ms, _ = e.query(representative['query'])
        restored_hits = e.hit_ids(response)
        e.event('reopen_representative_search', query=representative['query'],
                expected_docid=representative['expected_docid'], status=code, hits=restored_hits, ms=ms)
        summary['reopen_representative_search_verified'] = (
            code == 200 and representative['expected_docid'] in restored_hits)
        assert summary['reopen_representative_search_verified'], 'representative document missing after restart'
        summary['reopen_verified_search_ms'] = (time.perf_counter() - reopen_started) * 1000
        summary['status'] = 'complete'
    except Exception as exc:
        summary['status'] = 'failed'
        summary['error'] = repr(exc)
        raise
    finally:
        e.close()
        summary['batch_acknowledgement_including_busy_wait'] = stats(submits)
        summary['successful_acknowledgement_http_only'] = stats(http_submits)
        summary['batch_business_completion_observed'] = stats(business_completes)
        summary['batch_index_watermarks_observed'] = stats(completes)
        summary['batch_published_completion'] = stats(completes)
        summary['observed_business_completion_batches'] = len(business_completes)
        summary['verified_published_batches'] = len(completes)
        summary['peak_rss_kib'] = max((r['rss_kib'] for r in e.resources), default=0)
        summary['disk'] = disk(e.data)
        if engine == 'anyseek':
            summary.update(summarize_maintenance((out / 'server.log').read_text()))
        dump(out / 'summary.json', summary)
    return summary


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--anyseek', required=True, type=Path)
    p.add_argument('--meili', required=True, type=Path)
    p.add_argument('--data', required=True, type=Path)
    p.add_argument('--out', required=True, type=Path)
    p.add_argument('--revision', required=True)
    p.add_argument('--expected-anyseek-sha256', required=True,
                   help='SHA256 recorded by the final-build owner; rejects a stale binary')
    p.add_argument('--take', type=int, default=2305)
    p.add_argument('--batch-size', type=int, default=16)
    a = p.parse_args()
    assert not any(k.startswith('ANYSEEK_') for k in os.environ), 'inherited engine overrides forbidden'
    assert 1 <= a.batch_size <= 64
    assert a.take > 0
    assert not a.out.resolve().is_relative_to(Path(__file__).resolve().parents[2]), 'raw output must stay outside repository'
    anyseek_hash = hashlib.sha256(a.anyseek.read_bytes()).hexdigest()
    assert anyseek_hash == a.expected_anyseek_sha256, 'binary does not match final-build SHA256'
    a.out.mkdir(parents=True, exist_ok=False)
    os.sched_setaffinity(0, {2, 3})
    os.environ['TOKENIZER_ANYSEEK_BINARY'] = str(a.anyseek.resolve())
    os.environ['TOKENIZER_MEILI_BINARY'] = str(a.meili.resolve())
    os.environ['TOKENIZER_PROFILE'] = 'JiebaMixedV1'
    # The execution host may inherit an error-only filter, hiding phase evidence.
    os.environ['RUST_LOG'] = 'error,anyseek::record::maintenance=info'
    rows = json.loads(a.data.read_text())[:a.take]
    assert len(rows) == a.take, 'source sample is smaller than requested paired corpus'
    queries = json.loads(Path(__file__).with_name('queries-393.json').read_text())
    manifest = {'revision': a.revision, 'documents': len(rows),
                'selected_data_sha256': hashlib.sha256(json.dumps(rows, ensure_ascii=False, sort_keys=True).encode()).hexdigest(),
                'anyseek_binary_sha256': anyseek_hash,
                'meili_binary_sha256': hashlib.sha256(a.meili.read_bytes()).hexdigest(),
                'query_sha256': hashlib.sha256(Path(__file__).with_name('queries-393.json').read_bytes()).hexdigest(),
                'comparison_limits': 'Small sequential batches; validation overhead excluded from batch completion but included in total wall time; independent default tokenizers; AND/all do not imply identical semantics; empty queries are reported; checkpoint has no Meili equivalent.'}
    dump(a.out / 'manifest.json', manifest)
    summaries = [run(engine, rows, queries, a.out / engine, a.batch_size) for engine in ['anyseek', 'meili']]
    dump(a.out / 'summary.json', {'manifest': manifest, 'results': summaries})


if __name__ == '__main__':
    main()
