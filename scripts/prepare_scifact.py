#!/usr/bin/env python3
"""Prepare the complete BEIR SciFact test set with pinned multilingual E5 embeddings."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import time
import urllib.request
import zipfile

os.environ.setdefault('OMP_NUM_THREADS', '8')
os.environ.setdefault('OPENBLAS_NUM_THREADS', '1')
os.environ.setdefault('TOKENIZERS_PARALLELISM', 'false')
import numpy as np
import onnxruntime as ort
import tokenizers

MODEL = 'intfloat/multilingual-e5-small'
REVISION = '614241f622f53c4eeff9890bdc4f31cfecc418b3'
CORPUS_URL = 'https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/scifact.zip'


def sha256(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def download(url, path):
    if not path.exists():
        temporary = path.with_suffix(path.suffix + '.part')
        with urllib.request.urlopen(url, timeout=120) as source, temporary.open('wb') as target:
            while block := source.read(1024 * 1024):
                target.write(block)
        temporary.rename(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cache', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.cache.mkdir(parents=True, exist_ok=True)
    args.output.mkdir(parents=True, exist_ok=True)
    if (args.output / 'manifest.json').exists():
        parser.error('completed dataset already exists; choose another output directory')
    download(CORPUS_URL, args.cache / 'scifact.zip')
    with (args.cache / 'scifact.zip').open('rb') as source:
        if hashlib.file_digest(source, 'md5').hexdigest() != '5f7d1de60b170fc8027bb7898e2efca1':
            raise ValueError('SciFact archive does not match the BEIR published checksum')
    for name in ('tokenizer.json', 'onnx/model.onnx'):
        download(f'https://huggingface.co/{MODEL}/resolve/{REVISION}/{name}', args.cache / Path(name).name)
    with zipfile.ZipFile(args.cache / 'scifact.zip') as archive:
        for name in ('corpus.jsonl', 'queries.jsonl', 'qrels/test.tsv'):
            path = args.output / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(archive.read('scifact/' + name))
    corpus = [json.loads(line) for line in (args.output / 'corpus.jsonl').read_text().splitlines()]
    query_ids = {line.split('\t')[0] for line in (args.output / 'qrels/test.tsv').read_text().splitlines()[1:]}
    queries = [json.loads(line) for line in (args.output / 'queries.jsonl').read_text().splitlines()
               if str(json.loads(line)['_id']) in query_ids]
    (args.output / 'queries.jsonl').write_text(''.join(json.dumps(q) + '\n' for q in queries))
    tokenizer = tokenizers.Tokenizer.from_file(str(args.cache / 'tokenizer.json'))
    tokenizer.enable_truncation(max_length=512)
    tokenizer.enable_padding(pad_id=1, pad_token='<pad>')
    options = ort.SessionOptions()
    options.intra_op_num_threads = 8
    options.inter_op_num_threads = 1
    session = ort.InferenceSession(str(args.cache / 'model.onnx'), options,
                                   providers=['CPUExecutionProvider'])
    inputs = {value.name for value in session.get_inputs()}
    started = time.perf_counter()
    for name, texts in [('vectors.npy', ['passage: ' + (d.get('title', '') + '\n' + d['text']).strip() for d in corpus]),
                        ('query_vectors.npy', ['query: ' + q['text'] for q in queries])]:
        vectors = np.empty((len(texts), 384), dtype=np.float32)
        order = sorted(range(len(texts)), key=lambda i: len(texts[i]))
        for offset in range(0, len(order), 8):
            ids = order[offset:offset + 8]
            encoded = tokenizer.encode_batch([texts[i] for i in ids])
            batch = {'input_ids': np.array([t.ids for t in encoded], dtype=np.int64),
                     'attention_mask': np.array([t.attention_mask for t in encoded], dtype=np.int64),
                     'token_type_ids': np.array([t.type_ids for t in encoded], dtype=np.int64)}
            hidden = session.run(None, {k: v for k, v in batch.items() if k in inputs})[0]
            if hidden.ndim != 3 or hidden.shape[-1] != 384:
                raise ValueError(f'unexpected model output shape: {hidden.shape}')
            mask = batch['attention_mask'][..., None]
            pooled = (hidden * mask).sum(axis=1) / mask.sum(axis=1)
            pooled /= np.linalg.norm(pooled, axis=1, keepdims=True)
            vectors[ids] = pooled
            if offset % 256 == 0:
                print(f'{name}: {min(offset + 8, len(texts))}/{len(texts)}; elapsed {time.perf_counter() - started:.1f}s', flush=True)
        if not np.isfinite(vectors).all():
            raise ValueError('nonfinite embeddings')
        np.save(args.output / name, vectors)
    manifest = {'name': 'BEIR/SciFact', 'split': 'test', 'full_corpus': True,
                'documents': len(corpus), 'queries': len(queries), 'corpus_url': CORPUS_URL,
                'model': MODEL, 'revision': REVISION, 'dimensions': 384,
                'embedding': {'runtime': 'ONNX Runtime CPU FP32', 'onnxruntime': ort.__version__,
                              'tokenizers': tokenizers.__version__, 'numpy': np.__version__,
                              'max_tokens': 512, 'pooling': 'attention-mask mean, then L2 normalization',
                              'document_input': 'passage: {title}\\n{text}', 'query_input': 'query: {text}',
                              'seconds': time.perf_counter() - started, 'threads': 8, 'batch_size': 8,
                              'model_sha256': sha256(args.cache / 'model.onnx'),
                              'tokenizer_sha256': sha256(args.cache / 'tokenizer.json')},
                'files_sha256': {name: sha256(args.output / name) for name in
                                 ('corpus.jsonl', 'queries.jsonl', 'qrels/test.tsv', 'vectors.npy', 'query_vectors.npy')}}
    (args.output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(json.dumps(manifest, indent=2), flush=True)


if __name__ == '__main__':
    main()
