#!/usr/bin/env python3
"""Sequential, bounded quality diagnostic; no capacity or budget overrides."""
import argparse, hashlib, json, math, os, pathlib, time
from http_engine import Engine, doc, dump, disk


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def quality(engine, queries, keywords, mode, variant):
    rows=[]
    for q in queries:
        text=q['text'] if variant=='questions' else keywords[q['qid']]
        if engine.name=='anyseek':
            path='/api/v1/index/bench/query'
            body={'query':text,'operator':mode,'length':10,'count':True,'fields':['id','docid','title','text'],'search_fields':['title','text']}
        else:
            path='/indexes/bench/search'
            body={'q':text,'matchingStrategy':mode,'limit':10,'attributesToRetrieve':['id','docid','title','text'],'attributesToSearchOn':['title','text']}
        status,data,ms,_=engine.request('POST',path,body,required=False)
        ids=engine.hit_ids(data) if status==200 else []
        relevant={d for d,g in q['qrels'].items() if g>0}
        labels=[d in relevant for d in ids]
        ideal=sum(1/math.log2(i+2) for i in range(min(10,len(relevant))))
        rows.append({'qid':q['qid'],'query':text,'mode':mode,'status':status,'ms':ms,'ids':ids,'response':data,
                     'recall10':len(set(ids)&relevant)/len(relevant),
                     'ndcg10':sum(v/math.log2(i+2) for i,v in enumerate(labels))/ideal,
                     'mrr10':next((1/(i+1) for i,v in enumerate(labels) if v),0)})
    dump(engine.out/f'{variant}-{mode}.json',rows)
    return {'input':variant,'mode':mode,'queries':len(rows),'errors':sum(r['status']!=200 for r in rows),'empty':sum(not r['ids'] for r in rows),
            **{k:sum(r[k] for r in rows)/len(rows) for k in ['recall10','ndcg10','mrr10']}}


def run(name, documents, queries, keywords, out):
    out.mkdir(parents=True,exist_ok=False)
    engine=Engine(name,'restricted',out)
    result={'engine':name,'requested':len(documents),'status':'running'}
    dump(out/'result.json',result)
    try:
        engine.start();engine.create();engine.phase='build';start=time.monotonic()
        for i in range(0,len(documents),50):
            engine.write(documents[i:i+50]);print(name,'loaded',min(i+50,len(documents)),flush=True)
        result['build_visible_seconds']=time.monotonic()-start
        result['visible_count']=engine.count()
        assert result['visible_count']==len(documents)
        result['checkpoint']=engine.checkpoint();engine.stop();engine.start()
        result['reopened_count']=engine.count();assert result['reopened_count']==len(documents)
        engine.phase='quality'
        result['quality']=[quality(engine,queries,keywords,mode,variant) for variant in ['questions','keywords'] for mode in (['and','or'] if name=='anyseek' else ['all','last'])]
        # Verify raw source data for every persisted document, via an unambiguous
        # ID lookup (anyseek) or the Meili documents endpoint, not search recall.
        checked=0
        for d in documents:
            if name=='anyseek':
                got=engine.request('GET','/api/v1/index/bench/doc/'+d['id'])[1]['document']
                assert all(got[k]==d[k] for k in ['docid','title','text']);checked+=1
                continue
            got=engine.request('GET','/indexes/bench/documents/'+d['id'])[1]
            assert all(got[k]==d[k] for k in ['docid','title','text']);checked+=1
        for path in out.glob('questions-*.json'):
            for row in json.loads(path.read_text()):
                for hit in row['response'].get('hits',[]):
                    got=hit['record']['document'] if name=='anyseek' else hit
                    expected=next(d for d in documents if d['docid']==got['docid'])
                    assert all(got[k]==expected[k] for k in ['docid','title','text'])
                    if name=='anyseek':checked+=1
        result['raw_verification']={'checks':checked,'scope':'every document by ID after reopen, plus returned question hits' }
        result['status']='complete'
    except Exception as exc:
        result['status']='failed';result['error']=repr(exc)
        try:result['visible_count_after_failure']=engine.count()
        except Exception as inner:result['count_error']=repr(inner)
    finally:
        engine.close();result['disk']=disk(engine.data)
        result['peak_rss_kib']=max((r['rss_kib'] for r in engine.resources),default=0)
        dump(out/'result.json',result)
    return result


def main():
    p=argparse.ArgumentParser();p.add_argument('--data',type=pathlib.Path,required=True);p.add_argument('--anyseek',type=pathlib.Path,required=True);p.add_argument('--meili',type=pathlib.Path,required=True);p.add_argument('--out',type=pathlib.Path,required=True);a=p.parse_args()
    a.out.mkdir(parents=True,exist_ok=False)
    os.environ['TOKENIZER_ANYSEEK_BINARY']=str(a.anyseek.resolve());os.environ['TOKENIZER_MEILI_BINARY']=str(a.meili.resolve())
    # Keep clients on different CPUs; same two service CPUs in the shared harness.
    os.sched_setaffinity(0,{2,3})
    corpus=json.loads((a.data/'corpus-1000.json').read_text());queries=json.loads((a.data/'queries.json').read_text())
    keyword_path=pathlib.Path(__file__).with_name('keywords.json');keyword_rows=json.loads(keyword_path.read_text())
    keywords={r['qid']:r['keywords'] for r in keyword_rows}
    assert len(queries)==40 and len(keywords)==40
    assert all(r['original']==next(q['text'] for q in queries if q['qid']==r['qid']) for r in keyword_rows)
    positives={d for q in queries for d,g in q['qrels'].items() if g>0}
    assert len(positives)==91 and positives<={r['docid'] for r in corpus}
    manifest={'source_hashes':{n:sha(a.data/n) for n in ['corpus-1000.json','queries.json','selection-manifest.json','qrels.miracl-v1.0-zh-dev.tsv','topics.miracl-v1.0-zh-dev.tsv']},'keywords_sha256':sha(keyword_path),'binary_sha256':{'anyseek':sha(a.anyseek),'meili':sha(a.meili)},'positive_documents':len(positives),'query_count':len(queries)}
    dump(a.out/'manifest.json',manifest)
    documents=[doc(r) for r in corpus]
    first=run('anyseek',documents,queries,keywords,a.out/'anyseek-1000')
    if first['status']!='complete':
        seed='anyseek-tokenizer-20261025-v1'
        positives_rows=[r for r in corpus if r['docid'] in positives]
        distractors=sorted((r for r in corpus if r['docid'] not in positives),key=lambda r:hashlib.sha256((seed+r['docid']).encode()).hexdigest())
        corpus=positives_rows+distractors[:256-len(positives_rows)];documents=[doc(r) for r in corpus]
        manifest['fallback_seed']=seed
        first=run('anyseek',documents,queries,keywords,a.out/'anyseek-256')
    dump(a.out/'selected-corpus.json',corpus)
    manifest['selected_sha256']=sha(a.out/'selected-corpus.json');manifest['selected_ids']=[r['docid'] for r in corpus];manifest['selected_count']=len(corpus)
    manifest['positive_coverage']=sum(d in manifest['selected_ids'] for d in positives)/len(positives)
    dump(a.out/'manifest.json',manifest)
    second=run('meili',documents,queries,keywords,a.out/f'meili-{len(documents)}')
    dump(a.out/'summary.json',{'anyseek':first,'meili':second,'limitations':['relevance-enriched sampled corpus, not full MIRACL','keyword qrels are a proxy','AND/all tokenization differs; OR/last are not equivalent','quality diagnostic only; no QPS ranking']})
    if first['status']!='complete' or second['status']!='complete':raise SystemExit(1)

if __name__=='__main__':main()
