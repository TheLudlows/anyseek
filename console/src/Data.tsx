import { useEffect, useRef, useState } from "react";
import {
  ArrowDownToLine,
  ArrowLeft,
  ArrowRight,
  Braces,
  FileJson2,
  Plus,
  RefreshCw,
  Upload,
} from "lucide-react";
import { api, collectionPath, message, storage } from "./api.ts";
import { apiFieldText, apiJsonText } from "./json.ts";
import {
  Busy,
  Empty,
  Modal,
  Notice,
  RecordPreview,
} from "./components.tsx";
import {
  prepareDocumentRequests,
  sendWrites,
  type PendingWrites,
} from "./writes.ts";
import { loadEmbeddingConfig } from "./embeddings.ts";
import { generatePendingEmbeddings, prepareEmbeddedWrites } from "./embedding-writes.ts";
import { SchemaTable } from "./SchemaTable.tsx";
import { Tools } from "./Tools.tsx";
import type { Collection, Json, Page, RecordData } from "./types.ts";

export function Data({
  collection,
  apiKey,
  owner,
  onWritten,
}: {
  collection: Collection;
  apiKey: string;
  owner: string;
  onWritten: () => Promise<void>;
}) {
  const [page, setPage] = useState<Page | null>(null),
    [cursors, setCursors] = useState<Json[]>([null]),
    [position, setPosition] = useState(0);
  const [busy, setBusy] = useState(false),
    [error, setError] = useState(""),
    [selected, setSelected] = useState<RecordData | null>(null);
  const [tab, setTab] = useState<"browse" | "tools">("browse"),
    [importing, setImporting] = useState(false),
    [revision, setRevision] = useState(0);
  useEffect(() => {
    const controller = new AbortController();
    setBusy(true);
    setError("");
    setPage(null);
    setSelected(null);
    api<Page>(
      apiKey,
      collectionPath(collection.id) + "/iterator",
      '{"length":20,"cursor":' + apiJsonText(cursors[position]) + "}",
      "POST",
      controller.signal,
    )
      .then(setPage)
      .catch((e) => {
        if (!controller.signal.aborted) setError(message(e));
      })
      .finally(() => {
        if (!controller.signal.aborted) setBusy(false);
      });
    return () => controller.abort();
  }, [collection.id, apiKey, position, revision]);
  const fields = collection.schema
    .filter((f) => f.store !== false && !f.index_vector)
    .slice(0, 3);
  function reset() {
    setCursors([null]);
    setPosition(0);
    setRevision((n) => n + 1);
  }
  return (
    <>
      <div className="page-heading">
        <div>
          <span className="eyebrow">COLLECTION / {collection.id}</span>
          <h1>数据</h1>
          <p>查看原始记录，保留每一份信息的完整表达。</p>
        </div>
        <button className="primary" onClick={() => setImporting(true)}>
          <Plus size={16} />
          导入数据
        </button>
      </div>
      <div className="page-tabs">
        <button
          className={tab === "browse" ? "active" : ""}
          onClick={() => setTab("browse")}
        >
          <FileJson2 size={15} />
          记录浏览
        </button>
        <button
          className={tab === "tools" ? "active" : ""}
          onClick={() => setTab("tools")}
        >
          <Braces size={15} />
          高级检查
        </button>
      </div>
      {tab === "tools" ? (
        <Tools collection={collection} apiKey={apiKey} />
      ) : (
        <div className={"split-content " + (selected ? "has-preview" : "")}>
          <section className="panel records-panel">
            <div className="panel-toolbar">
              <div>
                <h2>
                  记录{" "}
                  <span className="count-badge">
                    {collection.indexed_doc_count.toLocaleString()}
                  </span>
                </h2>
                <p>按服务游标分页，点击记录查看完整内容。</p>
              </div>
              <button
                className="secondary compact"
                disabled={busy}
                onClick={reset}
              >
                <RefreshCw size={14} />
                刷新
              </button>
            </div>
            <Notice>
              {error && (
                <>
                  {error}{" "}
                  <button className="text-button" onClick={reset}>
                    从第一页重新加载
                  </button>
                </>
              )}
            </Notice>
            {busy ? (
              <div className="loading-block">
                <Busy label="正在读取记录" />
                <div className="skeleton" />
                <div className="skeleton" />
                <div className="skeleton" />
              </div>
            ) : !page?.records.length ? (
              <Empty
                title={error ? "未能读取记录" : "集合里还没有记录"}
                text={
                  error
                    ? "检查连接，或刷新以建立新的分页视图。"
                    : "导入一个 JSON 对象或对象数组，开始组织数据。"
                }
                action={
                  !error && (
                    <button
                      className="secondary"
                      onClick={() => setImporting(true)}
                    >
                      <Upload size={15} />
                      导入 JSON
                    </button>
                  )
                }
              />
            ) : (
              <div className="table-scroll">
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>记录</th>
                      {fields.map((f) => (
                        <th key={f.field}>{f.field}</th>
                      ))}
                      <th>版本</th>
                    </tr>
                  </thead>
                  <tbody>
                    {page.records.map((record) => (
                      <tr
                        key={record.record_id}
                        className={
                          selected?.record_id === record.record_id
                            ? "selected"
                            : ""
                        }
                      >
                        <td>
                          <button
                            className="record-link"
                            onClick={() => setSelected(record)}
                          >
                            <FileJson2 size={16} />
                            <span title={record.record_id}>
                              {record.record_id}
                            </span>
                          </button>
                        </td>
                        {fields.map((f) => (
                          <td key={f.field}>
                            <span
                              className="cell-text"
                              title={apiFieldText(
                                record.document || {},
                                f.field,
                              )}
                            >
                              {apiFieldText(record.document || {}, f.field) ||
                                "—"}
                            </span>
                          </td>
                        ))}
                        <td className="mono">
                          {apiFieldText(record, "version")}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
            <div className="pagination">
              <span>{page ? `本页 ${page.records.length} 条` : "—"}</span>
              <div>
                <button
                  className="secondary compact"
                  aria-label="上一页记录"
                  disabled={busy || position === 0}
                  onClick={() => setPosition((n) => n - 1)}
                >
                  <ArrowLeft size={14} />
                </button>
                <span>第 {position + 1} 页</span>
                <button
                  className="secondary compact"
                  aria-label="下一页记录"
                  disabled={busy || !page?.next}
                  onClick={() => {
                    setCursors((old) => [
                      ...old.slice(0, position + 1),
                      page!.next,
                    ]);
                    setPosition((n) => n + 1);
                  }}
                >
                  <ArrowRight size={14} />
                </button>
              </div>
            </div>
          </section>
          {selected && (
            <RecordPreview record={selected} close={() => setSelected(null)} />
          )}
        </div>
      )}
      {importing && (
        <ImportData
          collection={collection}
          apiKey={apiKey}
          owner={owner}
          close={() => setImporting(false)}
          onWritten={async () => {
            reset();
            await onWritten();
          }}
        />
      )}
    </>
  );
}
function ImportData({
  collection,
  apiKey,
  owner,
  close,
  onWritten,
}: {
  collection: Collection;
  apiKey: string;
  owner: string;
  close: () => void;
  onWritten: () => Promise<void>;
}) {
  const [source, setSource] = useState(""),
    [envelope, setEnvelope] = useState(false);
  const [modelConfig] = useState(() => loadEmbeddingConfig(owner, collection.id));
  const [autoEmbed, setAutoEmbed] = useState(!!modelConfig);
  const [indexProgress, setIndexProgress] = useState<Json>(null);
  const generation = useRef<AbortController | null>(null);
  useEffect(() => () => generation.current?.abort(), []);
  async function refreshProgress() {
    try { setIndexProgress(await api(apiKey, collectionPath(collection.id) + "/ingest")); }
    catch (e) { setError(message(e)); }
  }
  const [pending, setPending] = useState<PendingWrites | null>(() =>
    storage.get("pendingWrites"),
  );
  const [busy, setBusy] = useState(false),
    [error, setError] = useState(""),
    [status, setStatus] = useState("");
  const foreign =
    !!pending &&
    (pending.owner !== owner || pending.collection !== collection.id);
  async function submit(event?: React.FormEvent) {
    event?.preventDefault();
    if (busy || foreign) return;
    setBusy(true);
    setError("");
    setStatus("");
    let request = pending;
    try {
      if (!request) {
        request = autoEmbed && !envelope && modelConfig
          ? prepareEmbeddedWrites(source, owner, collection, modelConfig)
          : {
          owner,
          collection: collection.id,
          requests: prepareDocumentRequests(source, envelope),
          completed: 0,
          unresolved: false,
        };
        storage.set("pendingWrites", request);
        setPending(request);
      }
      if (request.embedding) {
        const controller = new AbortController(); generation.current = controller;
        setStatus(`正在生成向量：${request.requests.filter((r) => r.embedded).length} / ${request.requests.length} 条`);
        await generatePendingEmbeddings(request, owner, collection, (done) => setStatus(`已生成向量 ${done} / ${request!.requests.length} 条`), controller.signal);
        controller.signal.throwIfAborted();
      }
      await sendWrites(request, apiKey, owner, (done) =>
        setStatus(`已确认 ${done} / ${request!.requests.length} 条`),
      );
      setPending(null);
      setSource("");
      setStatus(
        request.delivery === "ingest"
          ? `已确认提交 ${request.requests.length} 条，索引正在异步更新。`
          : request.requests.some((r) => r.staging)
          ? `已确认写入 ${request.requests.length} 条。暂存内容持久化完成，显式发布后才可检索。`
          : `已确认写入 ${request.requests.length} 条，已持久化且可见。`,
      );
      if (request.delivery === "ingest") await refreshProgress();
      await onWritten();
    } catch (e) {
      setPending(storage.get("pendingWrites"));
      setError(message(e));
    } finally {
      setBusy(false);
    }
  }
  return (
    <Modal title={"导入数据 · " + collection.name} close={close} busy={busy}>
      <form onSubmit={submit}>
        <p className="modal-intro">
          粘贴 JSON 或上传文件。普通对象会自动分配记录标识。
        </p>
        {pending && (
          <Notice kind="info">
            {foreign
              ? `集合 ${pending.collection} 存在待确认写入，请回到原连接与集合处理。`
              : "原始请求已保留。重试复用原始内容与标识，避免重复写入。"}
          </Notice>
        )}
        {pending?.embedding && !foreign && <Notice kind="info">
          保留任务使用原模型 {pending.embedding.config.model} · {pending.embedding.config.endpoint}，已生成的向量不会重新生成。
        </Notice>}
        <fieldset disabled={busy || !!pending}>
          <div className="editor-toolbar">
            <label className="checkbox">
              <input
                type="checkbox"
                checked={envelope}
                onChange={(e) => setEnvelope(e.target.checked)}
              />
              Record envelope 模式
            </label>
            <label className="file-button">
              <ArrowDownToLine size={14} />
              选择 JSON 文件
              <input
                type="file"
                accept=".json,application/json"
                aria-label="选择 JSON 文件"
                onChange={async (e) => {
                  const file = e.target.files?.[0];
                  if (!file) return;
                  setBusy(true);
                  setError("");
                  try {
                    setSource(await file.text());
                  } catch (e) {
                    setError(message(e));
                  } finally {
                    setBusy(false);
                  }
                }}
              />
            </label>
          </div>
          {collection.schema.some((f) => f.index_vector) && <div className="embedding-choice">
            <label className="checkbox"><input type="checkbox" checked={autoEmbed && !envelope} disabled={!modelConfig || envelope} onChange={(e) => setAutoEmbed(e.target.checked)} />自动生成缺失的向量</label>
            <p className="field-help">{envelope ? "Record envelope 使用原始请求，不自动生成向量。" : modelConfig
              ? `${modelConfig.textFields.join(" + ")} → ${modelConfig.vectorField} · ${modelConfig.model}。所选文本将发送至 ${modelConfig.endpoint}，已有向量会保留。`
              : "在连接与管理中配置本集合的浏览器嵌入服务后，可直接导入文本。"}</p>
          </div>}
          <label className="sr-only" htmlFor="document-json">
            导入 JSON
          </label>
          <textarea
            id="document-json"
            className="code-input import-editor"
            placeholder={
              '[\n  {\n    "title": "第一条记录",\n    "body": "让信息可以被发现。"\n  }\n]'
            }
            value={source}
            onChange={(e) => setSource(e.target.value)}
            spellCheck={false}
          />
          <p className="field-help">
            原始数字文本会完整保留。自动嵌入每批最多 16 条，单条文本最多 30 KiB；写入不保证批次原子性。
          </p>
        </fieldset>
        <details>
          <summary>查看集合字段</summary>
          <SchemaTable fields={collection.schema} dimensions={collection.vector_dimensions} />
        </details>
        {envelope && (
          <details>
            <summary>Record envelope 说明</summary>
            <p className="field-help">
              提供 record_id、routing_key、expected 和 document；document 为
              null 表示删除。也支持 vector_patch 和 generation。Operation ID
              由控制台生成并保留。
            </p>
          </details>
        )}
        <Notice>{error}</Notice>
        <Notice kind="success">{status}</Notice>
        {indexProgress && <Notice kind="info">
          提交进度 {indexProgress.committed_seq ?? "待恢复"} · 全文 {indexProgress.lexical_seq} · 向量 {indexProgress.vector_seq}
          {indexProgress.index_error && " · 索引异常，请检查服务状态"}
          <button type="button" className="text-button" disabled={busy} onClick={refreshProgress}>刷新索引进度</button>
        </Notice>}
        <div className="modal-actions">
          {pending && !foreign && (pending.rejected || (pending.delivery === "ingest" && !pending.unresolved)) && (
            <button
              type="button"
              className="secondary"
              disabled={busy}
              onClick={() => {
                storage.remove("pendingWrites");
                setPending(null);
                setStatus("已保留确认写入的数据，已移除拒绝及未发送的请求。");
                setError("");
              }}
            >
              {pending.delivery === "ingest" ? "移除未提交的请求" : "移除被拒绝的请求"}
            </button>
          )}
          <button
            type="button"
            className="secondary"
            disabled={busy}
            onClick={close}
          >
            关闭
          </button>
          <button
            className="primary"
            disabled={busy || foreign || (!pending && !source.trim())}
          >
            {busy ? (
              <Busy label="正在写入" />
            ) : pending ? (
              "重试原始写入"
            ) : (
              <>
                <Upload size={15} />
                写入集合
              </>
            )}
          </button>
        </div>
      </form>
    </Modal>
  );
}
