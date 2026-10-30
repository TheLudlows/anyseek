import { useState } from "react";
import {
  ArrowUpRight,
  Braces,
  Database,
  Layers3,
  Plus,
  Search,
} from "lucide-react";
import { api, ApiError, message, newOperationId, storage } from "./api.ts";
import { Busy, Empty, JsonView, Modal, Notice } from "./components.tsx";
import type { Collection, Mode, Json } from "./types.ts";

type Props = {
  collections: Collection[];
  apiKey: string;
  owner: string;
  select: (id: string, page?: "data" | "search") => void;
  refresh: () => Promise<void>;
};
export function collectionMode(c: Collection): Mode {
  const vector = c.schema.some((f) => f.index_vector);
  return vector
    ? c.schema.some((f) => f.field_type === "Text" && f.index_lexical !== false)
      ? "Hybrid"
      : "Vector"
    : "Lexical";
}
function schemaFor(mode: Mode) {
  return JSON.stringify(
    [
      {
        field: "title",
        field_type: "Text",
        ...(mode === "Vector" ? { index_lexical: false } : {}),
      },
      {
        field: "body",
        field_type: "Text",
        ...(mode === "Vector" ? { index_lexical: false } : {}),
      },
      { field: "url", field_type: "Text", index_lexical: false },
      ...(mode === "Lexical"
        ? []
        : [{ field: "vector", field_type: "Json", index_vector: true }]),
    ],
    null,
    2,
  );
}
export function Collections(props: Props) {
  const [filter, setFilter] = useState(""),
    [creating, setCreating] = useState(false),
    [detail, setDetail] = useState<Collection | null>(null);
  const list = props.collections.filter((c) =>
    (c.name + c.id).toLowerCase().includes(filter.toLowerCase()),
  );
  const total = props.collections.reduce(
    (sum, c) => sum + c.indexed_doc_count,
    0,
  );
  return (
    <>
      <div className="page-heading">
        <div>
          <span className="eyebrow">YOUR WORKSPACE</span>
          <h1>集合</h1>
          <p>组织数据，让每一次检索都有据可依。</p>
        </div>
        <button className="primary" onClick={() => setCreating(true)}>
          <Plus size={16} />
          创建集合
        </button>
      </div>
      <div className="metrics">
        <div>
          <span className="metric-icon">
            <Layers3 size={18} />
          </span>
          <div>
            <small>集合总数</small>
            <strong>{props.collections.length.toLocaleString()}</strong>
          </div>
        </div>
        <div>
          <span className="metric-icon">
            <Database size={18} />
          </span>
          <div>
            <small>活跃记录</small>
            <strong>{total.toLocaleString()}</strong>
          </div>
        </div>
        <div>
          <span className="metric-icon">
            <Braces size={18} />
          </span>
          <div>
            <small>向量集合</small>
            <strong>
              {props.collections
                .filter((c) => c.schema.some((f) => f.index_vector))
                .length.toLocaleString()}
            </strong>
          </div>
        </div>
      </div>
      <section className="panel">
        <div className="panel-toolbar">
          <div>
            <h2>
              所有集合{" "}
              <span className="count-badge">{props.collections.length}</span>
            </h2>
            <p>选择集合，浏览数据或调试检索。</p>
          </div>
          <label className="filter-input">
            <Search size={15} />
            <input
              aria-label="筛选集合"
              placeholder="搜索集合名称…"
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
            />
          </label>
        </div>
        {!list.length ? (
          <Empty
            title={filter ? "没有匹配的集合" : "创建你的第一个集合"}
            text={
              filter
                ? "试试其他名称，或清除筛选条件。"
                : "先定义数据字段，再导入 JSON，开始检索。"
            }
            action={
              !filter && (
                <button className="secondary" onClick={() => setCreating(true)}>
                  <Plus size={15} />
                  创建集合
                </button>
              )
            }
          />
        ) : (
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>集合名称</th>
                  <th>检索模式</th>
                  <th>活跃记录</th>
                  <th>字段</th>
                  <th>操作</th>
                </tr>
              </thead>
              <tbody>
                {list.map((c) => (
                  <tr key={c.id}>
                    <td>
                      <button
                        className="collection-name"
                        onClick={() => props.select(c.id)}
                      >
                        <span className="collection-symbol">
                          <Layers3 size={17} />
                        </span>
                        <span>
                          <strong>{c.name}</strong>
                          <small>{c.id}</small>
                        </span>
                      </button>
                    </td>
                    <td>
                      <span
                        className={"tag " + collectionMode(c).toLowerCase()}
                      >
                        {collectionMode(c)}
                      </span>
                    </td>
                    <td className="numeric">
                      {c.indexed_doc_count.toLocaleString()}
                    </td>
                    <td className="muted">{c.schema.length} 个字段</td>
                    <td>
                      <div className="row-actions">
                        <button
                          className="text-button"
                          onClick={() => setDetail(c)}
                        >
                          结构
                        </button>
                        <button
                          className="text-button"
                          onClick={() => props.select(c.id, "search")}
                        >
                          检索
                          <ArrowUpRight size={14} />
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        <div className="panel-foot">
          <span className="status-dot" />
          已连接服务<span className="muted">集合统计来自当前服务响应</span>
        </div>
      </section>
      <div className="workflow-note">
        <span className="note-line" />
        <p>
          <strong>从数据到答案</strong>
          <span>创建集合 → 导入记录 → 调试关键词、向量与混合检索</span>
        </p>
      </div>
      {creating && (
        <CreateCollection {...props} close={() => setCreating(false)} />
      )}
      {detail && (
        <Modal
          title={detail.name + " · 集合结构"}
          close={() => setDetail(null)}
        >
          <JsonView value={detail.schema} label="Schema" />
          <details>
            <summary>服务统计与配置</summary>
            <JsonView value={detail} label="Collection JSON" />
          </details>
        </Modal>
      )}
    </>
  );
}
function CreateCollection({
  apiKey,
  owner,
  refresh,
  select,
  close,
}: Props & { close: () => void }) {
  const [pending, setPending] = useState<{
    owner: string;
    body: string;
    rejected?: boolean;
  } | null>(() => storage.get("pendingCreate"));
  const saved = pending ? JSON.parse(pending.body) : null;
  const [name, setName] = useState(saved?.collection_id || ""),
    [mode, setMode] = useState<Mode>(
      saved?.config.vector
        ? saved.config.schema.some(
            (f: Json) => f.field_type === "Text" && f.index_lexical !== false,
          )
          ? "Hybrid"
          : "Vector"
        : "Lexical",
    );
  const [schema, setSchema] = useState(
    saved ? JSON.stringify(saved.config.schema, null, 2) : schemaFor("Lexical"),
  );
  const [dimensions, setDimensions] = useState(
      String(saved?.config.vector?.dimensions || 384),
    ),
    [model, setModel] = useState(saved?.embedding_space_id || "");
  const [busy, setBusy] = useState(false),
    [error, setError] = useState("");
  const foreign = pending && pending.owner !== owner;
  async function submit(event: React.FormEvent) {
    event.preventDefault();
    setError("");
    setBusy(true);
    try {
      if (foreign)
        throw new Error("保留的创建请求属于另一连接，请用原 API Key 处理。");
      let request = pending;
      if (!request) {
        if (!/^[a-z][a-z0-9_-]{0,62}$/.test(name))
          throw new Error(
            "集合名称须以小写字母开头，包含小写字母、数字、下划线或短横线，最长 63 字符。",
          );
        const fields = JSON.parse(schema);
        if (!Array.isArray(fields) || !fields.length)
          throw new Error("Schema 必须是非空字段数组。");
        if (
          fields.some((f: Json) => !f?.field || !f.field_type) ||
          new Set(fields.map((f: Json) => f.field)).size !== fields.length
        )
          throw new Error("请为每个字段填写唯一名称和类型。");
        const config: Json = { index_name: name, schema: fields };
        if (mode !== "Lexical") {
          const d = Number(dimensions);
          if (!Number.isInteger(d) || d < 1 || d > 4096)
            throw new Error("向量维度必须是 1–4096 的整数。");
          if (!model.trim() || !fields.some((f: Json) => f.index_vector))
            throw new Error(
              "请提供 embedding 模型标识，并在 Schema 中指定向量字段。",
            );
          config.vector = { dimensions: d };
        } else if (fields.some((f: Json) => f.index_vector))
          throw new Error("向量字段需要选择 Vector 或 Hybrid 模式。");
        request = {
          owner,
          body: JSON.stringify({
            operation_id: newOperationId(),
            collection_id: name,
            config,
            ...(mode !== "Lexical" ? { embedding_space_id: model.trim() } : {}),
          }),
        };
        storage.set("pendingCreate", request);
        setPending(request);
      }
      const result = await api(apiKey, "index", request.body);
      storage.remove("pendingCreate");
      setPending(null);
      await refresh();
      select(result.collection_id);
      close();
    } catch (e) {
      if (e instanceof ApiError && e.code === "creation_rejected") {
        const request = storage.get<{ owner: string; body: string }>(
          "pendingCreate",
        );
        if (request) {
          const next = { ...request, rejected: true };
          storage.set("pendingCreate", next);
          setPending(next);
        }
      }
      setError(message(e));
    } finally {
      setBusy(false);
    }
  }
  return (
    <Modal title="创建集合" close={close} busy={busy}>
      <form onSubmit={submit}>
        <p className="modal-intro">为你的数据定义检索方式与字段结构。</p>
        {pending && (
          <Notice kind="info">
            {foreign
              ? "另一连接存在待确认的创建请求。"
              : "原始创建请求已保留。重试将复用相同配置和 Operation ID。"}
          </Notice>
        )}
        <fieldset disabled={busy || !!pending}>
          <label>
            集合名称
            <input
              autoFocus
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="例如 knowledge_base"
              required
            />
          </label>
          <label>
            检索模式
            <select
              value={mode}
              onChange={(e) => {
                const next = e.target.value as Mode;
                setMode(next);
                setSchema(schemaFor(next));
              }}
            >
              {["Lexical", "Vector", "Hybrid"].map((m) => (
                <option key={m}>{m}</option>
              ))}
            </select>
          </label>
          <p className="field-help">
            {mode === "Lexical"
              ? "关键词检索，适合精确匹配与全文搜索。"
              : mode === "Vector"
                ? "向量相似度检索，需要外部生成 embedding。"
                : "结合关键词与向量，覆盖字面和语义匹配。"}
          </p>
          {mode !== "Lexical" && (
            <div className="form-row">
              <label>
                向量维度
                <input
                  type="number"
                  min="1"
                  max="4096"
                  value={dimensions}
                  onChange={(e) => setDimensions(e.target.value)}
                  required
                />
              </label>
              <label>
                Embedding 模型标识
                <input
                  value={model}
                  onChange={(e) => setModel(e.target.value)}
                  placeholder="例如 bge-small-zh-v1.5"
                  required
                />
              </label>
            </div>
          )}
          <details>
            <summary>字段结构 · Schema</summary>
            <label className="sr-only" htmlFor="create-schema">
              字段结构 JSON
            </label>
            <textarea
              id="create-schema"
              className="code-input"
              rows={12}
              value={schema}
              onChange={(e) => setSchema(e.target.value)}
              spellCheck={false}
            />
          </details>
        </fieldset>
        <Notice>{error}</Notice>
        <div className="modal-actions">
          {pending?.rejected && !foreign && (
            <button
              type="button"
              className="secondary"
              disabled={busy}
              onClick={() => {
                storage.remove("pendingCreate");
                setPending(null);
                setError("");
              }}
            >
              修改被拒绝的请求
            </button>
          )}
          <button
            type="button"
            className="secondary"
            disabled={busy}
            onClick={close}
          >
            取消
          </button>
          <button className="primary" disabled={busy || !!foreign}>
            {busy ? (
              <Busy label="正在创建" />
            ) : pending ? (
              "重试原始创建"
            ) : (
              "创建集合"
            )}
          </button>
        </div>
      </form>
    </Modal>
  );
}
