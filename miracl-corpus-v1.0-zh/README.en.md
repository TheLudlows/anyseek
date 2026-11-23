# MIRACL Chinese test corpus

**English** · [简体中文](README.md)

This directory holds the MIRACL v1.0 Chinese corpus for Chinese tokenization, full-text retrieval, bulk import, and performance testing in Seekpower. The corpus contains 4,934,368 passages segmented from Chinese Wikipedia articles. It includes the retrieval corpus only. Retrieval-quality evaluation also requires MIRACL queries and relevance judgments (qrels), which are obtained separately.

## Source and version

- Upstream [miracl/miracl-corpus](https://huggingface.co/datasets/miracl/miracl-corpus), directory `miracl-corpus-v1.0-zh`.
- See [MIRACL](https://github.com/project-miracl/miracl) for the project and citation information. The upstream dataset card lists Apache-2.0 and provides provenance and usage guidance. [Dataset card](https://huggingface.co/datasets/miracl/miracl-corpus/blob/main/README.md)
- See [manifest.json](manifest.json) for the precise upstream commit, SHA-256, size in bytes, and record count for each shard. Before commit, the upstream byte sizes and SHA-256 hashes were checked, each gzip file was read in full, every JSON object was parsed, and fields and total record counts were checked.

## Files and fields

`docs-0.jsonl.gz` through `docs-9.jsonl.gz` are the original upstream gzip shards, with identical bytes and no extra archive layer. Each file decompresses independently. The first 9 shards have 500,000 records each and the last has 434,368. The compressed total is approximately 733 MB (decimal). The manifest lists the exact uncompressed size.

Decompressed files use UTF-8 JSON Lines, one object per line.

| Field | Type | Meaning |
| --- | --- | --- |
| `docid` | String | Original document identifier in `文章 ID#段落序号` (article ID and passage number) format. It can be retained as an application ID. |
| `title` | String | Original Wikipedia article title |
| `text` | String | Passage body |

This corpus contains no vectors. Generate vectors separately for vector or hybrid retrieval. Decompress the `.gz` files or read them programmatically, then batch-import documents in the format required by the Seekpower API.

## Reading and decompressing

All examples run from the repository root and use only the Python 3 standard library. Stream the data to avoid expanding the whole dataset.

```python
import gzip
import json
from pathlib import Path

for path in sorted(Path("miracl-corpus-v1.0-zh").glob("docs-*.jsonl.gz")):
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        for line in stream:
            document = json.loads(line)
            # 使用 document["docid"]、document["title"]、document["text"]。
```

To produce plain JSONL files, decompress each shard individually. This example retains the compressed files and refuses to overwrite existing output.

```python
import gzip
import shutil
from pathlib import Path

for path in sorted(Path("miracl-corpus-v1.0-zh").glob("docs-*.jsonl.gz")):
    with gzip.open(path, "rb") as source:
        with path.with_suffix("").open("xb") as target:
            shutil.copyfileobj(source, target)
```

Decompressed `.jsonl` and intermediate download `.part` files are covered by the root `.gitignore`.

## Verification

Verify the current shards against the manifest. The code streams each file and does not load a whole shard into memory.

```python
import hashlib
import json
from pathlib import Path

folder = Path("miracl-corpus-v1.0-zh")
manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
for entry in manifest["files"]:
    path = folder / entry["name"]
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    if path.stat().st_size != entry["size_bytes"] or digest.hexdigest() != entry["sha256"]:
        raise ValueError(f"校验失败：{path}")
print("全部分片校验通过")
```
