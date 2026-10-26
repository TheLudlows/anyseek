"""Reopen a COPY of a preserved boundary, verify receipts/results, checkpoint once.
Never retries writes, mutates the source root or stops an unknown outcome.
"""
import argparse, hashlib, json, os, pathlib, shutil
from urllib.parse import quote
from http_engine import Engine, dump, disk


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--source', type=pathlib.Path, required=True)
    p.add_argument('--out', type=pathlib.Path, required=True)
    p.add_argument('--binary', type=pathlib.Path, required=True)
    p.add_argument('--revision', required=True)
    a = p.parse_args()
    source = a.source.resolve()
    summary = json.loads((source / 'summary.json').read_text())
    a.out = a.out.resolve()
    assert not a.out.exists()
    assert not any(k.startswith('ANYSEEK_') for k in os.environ)
    available = shutil.disk_usage(a.out.parent).free
    needed = disk(source / 'db')['logical_bytes']
    assert available > needed + 2 * 1024**3, (available, needed)
    a.out.mkdir()
    shutil.copytree(source / 'db', a.out / 'db')
    os.sched_setaffinity(0, {2, 3})
    os.environ.update(summary['overrides'])
    os.environ['TOKENIZER_ANYSEEK_BINARY'] = str(a.binary.resolve())
    e = Engine('anyseek', 'restricted', a.out)
    result = {'source': str(source), 'revision': a.revision,
              'binary_sha256': hashlib.sha256(a.binary.read_bytes()).hexdigest(),
              'overrides': summary['overrides'], 'accepted': summary['accepted'],
              'note': 'Copied preserved authority and full tail; no writes or receipt retries; unchanged 60s startup/35s client deadlines.'}
    retain = True
    try:
        result['startup_ms'] = e.start()
        result['info'] = e.request('GET', '/api/v1/index/bench')[:3]
        assert result['info'][1]['statistics']['active_records'] == summary['accepted']
        assert result['info'][1]['statistics']['retained_operations'] == summary['accepted']
        checks = []
        for item in json.loads((source / 'boundary-validation/receipts.json').read_text()):
            expected = item['expected']
            op = expected['operation_id']
            response = e.request('GET', f"/api/v1/index/bench/operation/{op['epoch']}/{quote(op['nonce'],safe='')}")[:3]
            assert response[1]['receipt'] == expected['receipt']
            checks.append({'expected': expected, 'response': response})
        dump(a.out / 'receipts.json', checks)
        result['matched_receipts'] = len(checks)
        queries = {q['qid']: q for q in json.loads(pathlib.Path(__file__).with_name('queries-393.json').read_text())}
        probes = []
        for row in [json.loads(line) for line in (source / 'boundary-validation/queries.jsonl').open()][:5]:
            response = e.request('POST', '/api/v1/index/bench/query', {'query': queries[row['qid']]['text'], 'operator': 'or', 'length': 100, 'count': True, 'search_fields': ['title', 'text'], 'fields': ['docid']})
            ids = lambda body: [(h['record']['document']['docid'], h['score']) for h in body['hits']]
            assert ids(response[1]) == ids(row['response'])
            assert response[1]['count_total'] == row['response']['count_total']
            probes.append({'qid': row['qid'], 'response': response[:3]})
        dump(a.out / 'queries.json', probes)
        result['matched_queries'] = len(probes)
        dump(a.out / 'result.json', result)
        result['checkpoint'] = e.request('PATCH', '/api/v1/index/bench', required=False)[:3]
        dump(a.out / 'result.json', result)
        if result['checkpoint'][0] != 200:
            return
        e.stop()
        result['post_checkpoint_startup_ms'] = e.start()
        result['post_checkpoint_info'] = e.request('GET', '/api/v1/index/bench')[:3]
        assert result['post_checkpoint_info'][1]['statistics']['active_records'] == summary['accepted']
        assert result['post_checkpoint_info'][1]['statistics']['retained_operations'] == summary['accepted']
        for check in checks:
            op = check['expected']['operation_id']
            assert e.request('GET', f"/api/v1/index/bench/operation/{op['epoch']}/{quote(op['nonce'],safe='')}")[1]['receipt'] == check['expected']['receipt']
        result['completed'] = True
        retain = False
    except Exception as exc:
        result['exception'] = repr(exc)
    finally:
        if retain and e.process and e.process.poll() is None:
            result['retained_service'] = {'pid': e.process.pid, 'port': e.port, 'data': str(e.data)}
            e.stop_sample.set()
            e.sampler.join(timeout=2)
            dump(a.out / 'resources.json', e.resources)
            e.writer.close()
        else:
            e.close()
        result['peak_rss_kib'] = max((x['rss_kib'] for x in e.resources), default=0)
        dump(a.out / 'result.json', result)
        print(json.dumps(result, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
