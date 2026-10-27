"""Sequential same-corpus quality follow-up; does not alter production settings/limits."""
import argparse, hashlib, json, math, os, pathlib, shutil, time
from http_engine import Engine, doc, dump

MEILI_SHA = '7e33f5889235f14290074a19a6d3574bd9b10fb2ee8731eabb7dafd0714b0513'

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def evaluate(queries, rows, accepted):
    summaries = {}
    for group in ['all', 'prior40_nonblind', 'heldout353']:
        pairs = [(q, r) for q, r in zip(queries, rows) if group == 'all' or q['group'] == group]
        totals = dict(recall10=0., recall100=0., ndcg10=0., mrr10=0.)
        errors = empty = positive_pairs = covered = 0
        for q, r in pairs:
            positives = {d for d, grade in q['qrels'].items() if grade > 0}
            positive_pairs += len(positives)
            covered += len(positives & accepted)
            ids = r['ids'] if r['status'] == 200 else []
            assert len(ids) == len(set(ids)) and set(ids) <= accepted
            errors += r['status'] != 200
            empty += r['status'] == 200 and not ids
            for k in [10, 100]:
                totals[f'recall{k}'] += len(set(ids[:k]) & positives) / len(positives)
            ideal = sum((2**g - 1) / math.log2(i + 2) for i, g in enumerate(sorted(q['qrels'].values(), reverse=True)[:10]))
            dcg = sum((2**q['qrels'].get(d, 0) - 1) / math.log2(i + 2) for i, d in enumerate(ids[:10]))
            totals['ndcg10'] += dcg / ideal if ideal else 0
            totals['mrr10'] += next((1 / (i + 1) for i, d in enumerate(ids[:10]) if d in positives), 0)
        summaries[group] = {'queries': len(pairs), 'positive_pairs': positive_pairs, 'covered_positive_pairs': covered,
                            'errors': errors, 'error_rate': errors / len(pairs), 'empty': empty,
                            'empty_over_all_queries': empty / len(pairs),
                            **{k: v / len(pairs) for k, v in totals.items()}}
    return summaries

def no_other_services():
    for entry in pathlib.Path('/proc').iterdir():
        if not entry.name.isdigit():
            continue
        try:
            args = (entry / 'cmdline').read_bytes().split(b'\0')
            executable = pathlib.Path(os.fsdecode(args[0])).name
            if ('anyseek' in executable or 'meilisearch' in executable) and any(x.startswith(b'index_path=') or x == b'--db-path' for x in args):
                raise RuntimeError(f'service PID {entry.name} is still running; quality services must run separately')
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            pass

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--engine', choices=['anyseek', 'meili'], required=True)
    p.add_argument('--baseline', type=pathlib.Path, required=True)
    p.add_argument('--corpus', type=pathlib.Path, required=True)
    p.add_argument('--binary', type=pathlib.Path, required=True)
    p.add_argument('--out', type=pathlib.Path, required=True)
    a = p.parse_args()
    assert not any(k.startswith('ANYSEEK_') for k in os.environ)
    no_other_services()
    source = json.loads((a.baseline / 'summary.json').read_text())
    accepted_ids = json.loads((a.baseline / 'accepted-docids.json').read_text())
    corpus = json.loads(a.corpus.read_text())
    assert sha(a.corpus) == source['corpus_sha256']
    assert len(accepted_ids) == len(set(accepted_ids)) == source['accepted']
    selected = corpus[:len(accepted_ids)]
    assert [r['docid'] for r in selected] == accepted_ids
    accepted = set(accepted_ids)
    queries_path = pathlib.Path(__file__).with_name('queries-393.json')
    queries = json.loads(queries_path.read_text())
    assert len(queries) == 393
    assert {d for q in queries for d, g in q['qrels'].items() if g > 0} <= accepted
    assert shutil.disk_usage(a.out.resolve().parent).free > 2 * 1024**3
    a.out = a.out.resolve()
    a.out.mkdir(exist_ok=False)
    os.sched_setaffinity(0, {2, 3})
    if a.engine == 'anyseek':
        assert sha(a.binary) == source['binary_sha256']
        os.environ.update(source['overrides'])
        os.environ['TOKENIZER_ANYSEEK_BINARY'] = str(a.binary.resolve())
    else:
        assert sha(a.binary) == MEILI_SHA
        os.environ['TOKENIZER_MEILI_BINARY'] = str(a.binary.resolve())
        os.environ['MEILI_MAX_INDEXING_MEMORY'] = str(6 * 1024**3)
    e = Engine(a.engine, 'restricted', a.out)
    if a.engine == 'anyseek':
        e.data = (a.baseline / 'db').resolve()
    result = {'engine': a.engine, 'binary_sha256': sha(a.binary), 'source_revision': source['revision'],
              'corpus_sha256': source['corpus_sha256'], 'queries_sha256': sha(queries_path),
              'accepted_ids_sha256': sha(a.baseline / 'accepted-docids.json'), 'documents': len(selected),
              'normalization': 'None; original title/text retained', 'groups': {}, 'complete': False}
    def save():
        dump(a.out / 'result.json', result)
    try:
        result['startup_ms'] = e.start()
        if a.engine == 'meili':
            e.create()
            start = time.monotonic()
            for i in range(0, len(selected), 1000):
                available = shutil.disk_usage(a.out).free
                if available < 2 * 1024**3:
                    raise RuntimeError(f'2GiB safety margin reached before indexing batch {i}: {available}')
                docs = [doc(r) for r in selected[i:i+1000]]
                for d in docs:
                    d.pop('date')
                e.write(docs)
                print('meili indexed', min(i + 1000, len(selected)), flush=True)
            result['index_seconds'] = time.monotonic() - start
        result['visible_before'] = e.count()
        assert result['visible_before'] == len(selected)
        modes = [('and', 'and'), ('or', 'or')] if a.engine == 'anyseek' else [('strict_all', 'all'), ('strict_last', 'last'), ('default_last', 'last')]
        for label, strategy in modes:
            if label == 'default_last':
                for setting in ['typo-tolerance', 'prefix-search']:
                    e.task(e.request('DELETE', '/indexes/bench/settings/' + setting)[1])
            if a.engine == 'meili':
                settings = e.request('GET', '/indexes/bench/settings')[1]
                assert settings['prefixSearch'] == ('indexingTime' if label == 'default_last' else 'disabled')
                assert settings['typoTolerance']['enabled'] == (label == 'default_last')
            else:
                settings = e.request('GET', '/api/v1/index/bench')[1]
                assert settings['text_normalization'] == 'None'
            dump(a.out / (label + '-settings.json'), settings)
            rows = []
            with (a.out / (label + '-queries.jsonl')).open('w') as f:
                for q in queries:
                    if a.engine == 'anyseek':
                        path = '/api/v1/index/bench/query'
                        body = {'query': q['text'], 'operator': strategy, 'length': 100, 'count': True, 'search_fields': ['title', 'text'], 'fields': ['docid']}
                    else:
                        path = '/indexes/bench/search'
                        body = {'q': q['text'], 'matchingStrategy': strategy, 'limit': 100, 'attributesToSearchOn': ['title', 'text'], 'attributesToRetrieve': ['docid']}
                    code, response, ms, _ = e.request('POST', path, body, required=False)
                    ids = e.hit_ids(response) if code == 200 else []
                    row = {'qid': q['qid'], 'group': q['group'], 'request': body, 'status': code, 'response': response, 'ids': ids, 'ms': ms}
                    rows.append(row)
                    f.write(json.dumps(row, ensure_ascii=False) + '\n')
                    f.flush()
            result['groups'][label] = evaluate(queries, rows, accepted)
            save()
        result['visible_after'] = e.count()
        assert result['visible_after'] == len(selected)
        if a.engine == 'meili':
            e.stop()
            result['reopen_ms'] = e.start()
            result['visible_after_reopen'] = e.count()
            assert result['visible_after_reopen'] == len(selected)
        result['complete'] = True
    except Exception as exc:
        result['exception'] = repr(exc)
    finally:
        if not result['complete'] and e.process and e.process.poll() is None:
            result['retained_service'] = {'pid': e.process.pid, 'port': e.port, 'data': str(e.data)}
            e.stop_sample.set()
            e.sampler.join(timeout=2)
            dump(a.out / 'resources.json', e.resources)
            e.writer.close()
        else:
            e.close()
        result['peak_rss_kib'] = max((r['rss_kib'] for r in e.resources), default=0)
        save()
        print(json.dumps(result, ensure_ascii=False), flush=True)

if __name__ == '__main__':
    main()
