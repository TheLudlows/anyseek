#!/usr/bin/env python3
"""Historical baseline replay only; retired profiles require the pinned old binary.
Current binaries accept only the standard profiles; no compatibility mapping.
"""
import argparse, hashlib, json, os, pathlib
from diagnose import run, sha
from http_engine import doc, dump

p=argparse.ArgumentParser()
p.add_argument('--data',type=pathlib.Path,required=True)
p.add_argument('--binary',type=pathlib.Path,required=True)
p.add_argument('--out',type=pathlib.Path,required=True)
p.add_argument('--new-binary',type=pathlib.Path,help='compare both Chinese analyzers on an independently labeled 256-document pool')
a=p.parse_args()
expected='f113cde6bd36384434977c222db75fbd27b3f5cf363266a976ce03221b36792e'
assert sha(a.binary)==expected, 'must use the verified PR82 baseline binary'
assert not any(k.startswith('ANYSEEK_') for k in os.environ)
os.sched_setaffinity(0,{2,3})
os.environ['TOKENIZER_ANYSEEK_BINARY']=str(a.binary.resolve())
os.environ['TOKENIZER_PROFILE']='UnicodeAlphanumeric'
manifest=json.loads((a.data/'selection-manifest.json').read_text())
assert sha(a.data/'corpus-1000.json')==manifest['hashes']['1000']
assert sha(a.data/'queries.json')==manifest['hashes']['queries']
queries=json.loads((a.data/'queries.json').read_text())
keywords={r['qid']:r['keywords'] for r in json.loads(pathlib.Path(__file__).with_name('keywords.json').read_text())}
corpus=json.loads((a.data/'corpus-1000.json').read_text())
metadata={'code_revision':'6d28798a300bb947b2eb1d3e48e928d193103e17','binary_sha256':expected,'tokenizer':'UnicodeAlphanumeric','corpus_sha256':manifest['hashes']['1000'],'queries_sha256':manifest['hashes']['queries'],'keywords_sha256':sha(pathlib.Path(__file__).with_name('keywords.json')),'scope':'quality-only same corpus/questions/keywords/operator; former HTTP default selected explicitly'}
if a.new_binary:
    a.out.mkdir(parents=True,exist_ok=False)
    positives={d for q in queries for d,g in q['qrels'].items() if g>0}
    assert len(positives)==91
    seed='anyseek-tokenizer-20261025-v1'
    selected=[r for r in corpus if r['docid'] in positives]
    distractors=sorted((r for r in corpus if r['docid'] not in positives),key=lambda r:hashlib.sha256((seed+r['docid']).encode()).hexdigest())
    selected+=distractors[:256-len(selected)]
    assert len(selected)==256 and positives<={r['docid'] for r in selected}
    dump(a.out/'selected-corpus.json',selected)
    results={}
    for label,binary,profile in [('previous',a.binary,'UnicodeAlphanumericZH'),('current',a.new_binary,'JiebaMixedV1')]:
        os.environ['TOKENIZER_ANYSEEK_BINARY']=str(binary.resolve())
        os.environ['TOKENIZER_PROFILE']=profile
        results[label]=run('anyseek',[doc(r) for r in selected],queries,keywords,a.out/label)
    metadata.update({'tokenizer':'paired UnicodeAlphanumericZH / JiebaMixedV1','new_binary_sha256':sha(a.new_binary),'selected_count':256,'selected_sha256':sha(a.out/'selected-corpus.json'),'seed':seed,'all_91_positives_present':True,'scope':'paired 256-doc Chinese analysis only; never mix with 1000-doc metrics'})
    dump(a.out/'baseline-manifest.json',metadata);dump(a.out/'summary.json',results)
    if any(r['status']!='complete' for r in results.values()):raise SystemExit(1)
else:
    result=run('anyseek',[doc(r) for r in corpus],queries,keywords,a.out)
    dump(a.out/'baseline-manifest.json',metadata)
    if result['status']!='complete':raise SystemExit(1)
