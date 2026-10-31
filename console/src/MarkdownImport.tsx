import { useState } from "react";
import type { Collection } from "./types.ts";
import { chunkMarkdown, markdownDocuments, type MarkdownChunk, type MarkdownMapping } from "./markdown.ts";
import { message } from "./api.ts";
import { Notice } from "./components.tsx";

export function MarkdownImport({ collection, onSource, onReading }: {
  collection: Collection; onSource: (source: string, bodyField?: string) => void; onReading: (reading: boolean) => void;
}) {
  const textFields = collection.schema.filter(f => !f.index_vector && ["Text", "String"].includes(f.field_type));
  const [mapping, setMapping] = useState<MarkdownMapping>({ body: textFields.find(f => f.field === "body")?.field || textFields.find(f => f.field !== "title")?.field || "", title: textFields.find(f => f.field === "title")?.field || "", source: textFields.find(f => f.field === "source")?.field || "", index: collection.schema.find(f => f.field === "chunk_index" && f.field_type === "Int")?.field || "" });
  const [chunks, setChunks] = useState<MarkdownChunk[]>([]);
  const [files, setFiles] = useState<string[]>([]);
  const [error, setError] = useState("");
  const [confirmed, setConfirmed] = useState(false);
  async function readFiles(selected: FileList | null) {
    if (!selected?.length) return;
    onSource(""); setConfirmed(false); setChunks([]); setFiles([]); setError(""); onReading(true);
    try {
      const list = Array.from(selected);
      if (list.length > 20 || list.reduce((size, file) => size + file.size, 0) > 2 * 1024 * 1024) throw new Error("每次最多选择 20 个文件，合计不超过 2 MiB。");
      if (list.some(file => !/\.(md|markdown)$/i.test(file.name))) throw new Error("请选择 .md 或 .markdown 文件。");
      const result: MarkdownChunk[] = [];
      for (const file of list) {
        const text = new TextDecoder("utf-8", { fatal: true }).decode(await file.arrayBuffer());
        const items = chunkMarkdown(file.name, text);
        if (!items.length) throw new Error(`${file.name} 没有可导入的内容。`);
        result.push(...items);
      }
      if (result.length > 500) throw new Error("分块超过 500 条，请分批选择文档。");
      setChunks(result); setFiles(list.map(file => file.name));
    } catch (e) { setError(message(e)); }
    finally { onReading(false); }
  }
  return <div className="markdown-import">
    <label className="file-button">选择 Markdown 文档
      <input type="file" multiple accept=".md,.markdown,text/markdown" aria-label="选择 Markdown 文档" onChange={e => { void readFiles(e.target.files); e.target.value = ""; }} />
    </label>
    <p className="field-help">支持 UTF-8 文件，每次最多 20 个、合计 2 MiB。按标题、段落分块，目标约 4,000 字符；代码块和连续列表、表格不强行截断。外部链接与图片不会下载。</p>
    <div className="form-grid">
      {([['body', '正文目标字段（必选）'], ['title', '文档标题'], ['source', '来源文件名'], ['index', '片段序号']] as const).map(([key, label]) => <label key={key}>{label}<select value={mapping[key]} onChange={e => { setMapping({ ...mapping, [key]: e.target.value }); onSource(""); setConfirmed(false); }}>
        <option value="">{key === 'body' ? '请选择' : '不保存'}</option>
        {(key === 'index' ? collection.schema.filter(f => f.field_type === 'Int' && !f.index_vector) : textFields).map(f => <option key={f.field} value={f.field}>{f.field}</option>)}
      </select></label>)}
    </div>
    <p className="field-help">正文包含文档标题、章节路径和当前片段。每个片段新增一条记录；再次导入同一文件会新增记录，不覆盖旧文档。</p>
    {chunks.length > 0 && <>
      <p>{files.length} 个文件 · {chunks.length} 个片段</p>
      {chunks.some(c => c.oversized) && <Notice kind="info">部分完整段落或代码块超过目标长度，已保留全文。请检查模型长度限制，必要时先拆分原文件。</Notice>}
      <div className="markdown-preview">{chunks.map((chunk, i) => <details key={i}>
        <summary>{chunk.source} · 片段 {chunk.index} · {chunk.content.length} 字符{chunk.oversized ? ' · 超长' : ''}</summary>
        <p className="field-help">{chunk.section || chunk.title}</p><pre>{chunk.content}</pre>
      </details>)}</div>
      <button type="button" className="secondary" onClick={() => {
        try { onSource(JSON.stringify(markdownDocuments(chunks, mapping)), mapping.body); setConfirmed(true); setError(""); }
        catch (e) { onSource(""); setConfirmed(false); setError(message(e)); }
      }}>确认字段与分块预览</button>
      {confirmed && <p className="field-help">预览已确认，可以写入集合。</p>}
    </>}
    <Notice>{error}</Notice>
  </div>;
}
