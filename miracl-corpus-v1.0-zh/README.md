# MIRACL 中文测试语料

本目录保存 MIRACL v1.0 中文语料，可用于 Seekpower 中文分词、全文检索、批量导入和性能测试。内容是中文维基百科文章切分得到的段落，共 4,934,368 条。这里只包含检索语料；评测检索质量还需要另外取得 MIRACL 的查询和相关性标注（qrels）。

## 来源与版本

- 上游：[miracl/miracl-corpus](https://huggingface.co/datasets/miracl/miracl-corpus)，目录 `miracl-corpus-v1.0-zh`。
- 项目及引用信息：[MIRACL](https://github.com/project-miracl/miracl)。上游数据卡标注许可证为 Apache-2.0，来源与使用说明见[数据卡](https://huggingface.co/datasets/miracl/miracl-corpus/blob/main/README.md)。
- 精确上游 commit、每个分片的 SHA-256、字节数和记录数见 [manifest.json](manifest.json)。提交前已核对上游文件大小和 SHA-256，并完整读取 gzip、解析 JSON、检查字段与记录总数。

## 文件与字段

`docs-0.jsonl.gz` 至 `docs-9.jsonl.gz` 是上游原始 gzip 压缩分片，保留原始字节，不再套一层压缩包。每个文件可独立解压；前 9 个分片各有 500,000 条记录，最后一个有 434,368 条。压缩后的总大小约 733 MB（十进制），未压缩的精确大小见清单。

解压后的文件采用 UTF-8 JSON Lines，每行一个对象：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `docid` | 字符串 | 原始文档标识，格式为 `文章 ID#段落序号`；可保留为业务 ID |
| `title` | 字符串 | 原始维基百科文章标题 |
| `text` | 字符串 | 段落正文 |

这份语料不含向量；用于向量或混合检索时，需要自行生成向量。`.gz` 文件应先解压或通过程序读取，再将文档按 Seekpower API 所需格式分批导入。

## 读取与解压

以下示例均从仓库根目录运行，只依赖 Python 3 标准库。可以直接流式读取，避免展开整个数据集：

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

需要普通 JSONL 文件时，可逐个解压；下例保留压缩文件，并拒绝覆盖已有输出：

```python
import gzip
import shutil
from pathlib import Path

for path in sorted(Path("miracl-corpus-v1.0-zh").glob("docs-*.jsonl.gz")):
    with gzip.open(path, "rb") as source:
        with path.with_suffix("").open("xb") as target:
            shutil.copyfileobj(source, target)
```

解压产生的 `.jsonl` 和下载中间文件 `.part` 已加入根目录 `.gitignore`。

## 校验与重新下载

用清单校验当前分片（从文件流读取，不会把整个分片载入内存）：

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

[下载脚本](../scripts/download_miracl_zh.py) 支持并发下载、断点续传和 SHA-256 校验；已存在且校验通过的文件会被跳过。使用清单记录的版本可重新取得相同文件：

```python
import json
import subprocess
import sys
from pathlib import Path

manifest = json.loads(Path("miracl-corpus-v1.0-zh/manifest.json").read_text(encoding="utf-8"))
subprocess.run([
    sys.executable, "scripts/download_miracl_zh.py",
    "--revision", manifest["revision"], "--workers", "5",
], check=True)
```

直接运行 `python scripts/download_miracl_zh.py --workers 5` 会使用执行时的上游 `main` 版本。脚本遇到已存在但校验失败的文件会停止该文件的下载，请先将其移走再重试。
