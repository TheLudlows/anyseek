import { useState } from "react";
import { ArrowRight, Braces, Network, ScanText } from "lucide-react";
import {
  api,
  collectionPath,
  message,
  newOperationId,
  ownerFor,
  storage,
} from "./api.ts";
import { apiJsonText } from "./json.ts";
import { Busy, JsonView, Notice } from "./components.tsx";
import type { Collection, Json } from "./types.ts";

const tools = [
  {
    id: "entities",
    name: "实体目录",
    icon: Network,
    text: "浏览可见实体，使用原始游标继续读取。",
    sample: '{"length":20}',
  },
  {
    id: "neighbors",
    name: "一跳关系",
    icon: Network,
    text: "从明确的种子记录出发，查看有界的一跳关系。",
    sample: JSON.stringify(
      {
        seeds: [{ record_id: "entity_id", routing_key: "route", version: 1 }],
        direction: "out",
        relation_types: [],
        per_seed: 10,
        max_candidates: 100,
        allow_partial: false,
      },
      null,
      2,
    ),
  },
  {
    id: "query",
    name: "关系融合",
    icon: Braces,
    text: "直接提交查询与 relation_fusion，检查通道与候选窗口。",
    sample: JSON.stringify(
      {
        query: "关键词",
        length: 10,
        relation_fusion: {
          rrf: {
            constant: 3,
            keyword_weight: 1,
            vector_weight: 0,
            keyword_candidates: 100,
          },
          relation_weight: 1,
          neighbors: {
            seeds: [
              { record_id: "entity_id", routing_key: "route", version: 1 },
            ],
            direction: "out",
            relation_types: [],
            per_seed: 10,
            max_candidates: 100,
            allow_partial: false,
          },
        },
      },
      null,
      2,
    ),
  },
  {
    id: "evidence",
    name: "来源证据",
    icon: ScanText,
    text: "读取精确的证据摘录与内容哈希。UTF-8 范围以字节计。",
    sample: JSON.stringify(
      {
        record_id: "source_id",
        routing_key: "route",
        version: 1,
        locator: { kind: "utf8", field: "body", start: 0, end: 10 },
      },
      null,
      2,
    ),
  },
];
export function Tools({
  collection,
  apiKey,
}: {
  collection: Collection;
  apiKey: string;
}) {
  const [tool, setTool] = useState(tools[0]),
    [source, setSource] = useState(tools[0].sample);
  const [result, setResult] = useState<Json>(null),
    [error, setError] = useState(""),
    [busy, setBusy] = useState(false);
  async function run(next = false) {
    setBusy(true);
    setError("");
    try {
      const path = collectionPath(collection.id) + "/" + tool.id;
      let body = source;
      JSON.parse(source);
      if (next)
        body = '{"length":20,"cursor":' + apiJsonText(result.next) + "}";
      setResult(await api(apiKey, path, body));
    } catch (e) {
      setResult(null);
      setError(message(e));
    } finally {
      setBusy(false);
    }
  }
  return (
    <div className="tools-layout">
      <nav className="tool-nav" aria-label="高级检查工具">
        {tools.map((t) => (
          <button
            key={t.id}
            disabled={busy}
            className={t === tool ? "active" : ""}
            onClick={() => {
              setTool(t);
              setSource(t.sample);
              setResult(null);
              setError("");
            }}
          >
            <t.icon size={16} />
            {t.name}
            <ArrowRight size={13} />
          </button>
        ))}
      </nav>
      <section className="panel tool-panel">
        <div className="panel-toolbar">
          <div>
            <h2>{tool.name}</h2>
            <p>{tool.text}</p>
          </div>
        </div>
        <div className="tool-body">
          <form
            onSubmit={(e) => {
              e.preventDefault();
              run();
            }}
          >
            <label>
              请求 JSON
              <textarea
                aria-label="请求 JSON"
                className="code-input"
                rows={13}
                value={source}
                spellCheck={false}
                onChange={(e) => setSource(e.target.value)}
              />
            </label>
            <div className="inline-actions">
              <button className="primary" disabled={busy}>
                {busy ? <Busy label="正在读取" /> : "运行检查"}
              </button>
              {tool.id === "entities" && result?.next && (
                <button
                  className="secondary"
                  type="button"
                  disabled={busy}
                  onClick={() => run(true)}
                >
                  下一页实体
                  <ArrowRight size={14} />
                </button>
              )}
            </div>
          </form>
          <Notice>{error}</Notice>
          {result && <JsonView value={result} label="API 响应" />}
        </div>
      </section>
    </div>
  );
}
export function Recovery() {
  const [master, setMaster] = useState(""),
    [backup, setBackup] = useState("");
  const [busy, setBusy] = useState(false),
    [result, setResult] = useState<Json>(null),
    [error, setError] = useState("");
  const [confirmRestore, setConfirmRestore] = useState(false);
  async function run(action: "backup" | "restore") {
    setBusy(true);
    setError("");
    setResult(null);
    try {
      if (!master.trim()) throw new Error("请输入 Master API Key。");
      const owner = await ownerFor(master.trim());
      let pending = storage.get<{ owner: string; body: string }>(
        "service." + action,
      );
      if (pending && pending.owner !== owner)
        throw new Error(
          "已有另一管理连接的待确认操作，请用原 Master Key 重试。",
        );
      if (!pending) {
        if (action === "restore" && !/^[0-9a-f]{64}$/.test(backup))
          throw new Error("备份 ID 应为 64 位小写十六进制字符串。");
        pending = {
          owner,
          body: JSON.stringify({
            operation_id: newOperationId(),
            ...(action === "restore" ? { backup_id: backup } : {}),
          }),
        };
        storage.set("service." + action, pending);
      }
      const value = await api(
        master.trim(),
        action === "backup" ? "service-backups" : "service-restores",
        pending.body,
      );
      storage.remove("service." + action);
      setResult(value);
      if (action === "backup") setBackup(value.backup_id);
      setConfirmRestore(false);
    } catch (e) {
      setError(message(e));
    } finally {
      setBusy(false);
    }
  }
  return (
    <section className="panel recovery-panel">
      <div className="panel-toolbar">
        <div>
          <h2>服务备份与恢复</h2>
          <p>管理员操作，需要单独提供 Master API Key。</p>
        </div>
        <span className="tag">ADMIN</span>
      </div>
      <div className="tool-body">
        <Notice kind="info">
          恢复会创建新的停止状态数据根目录，不替换运行中的服务。目标服务需重新签发
          Tenant Key，证据内容遵循服务返回的隔离状态。
        </Notice>
        <label>
          Master API Key
          <input
            type="password"
            autoComplete="off"
            value={master}
            onChange={(e) => setMaster(e.target.value)}
            placeholder="仅用于本页操作，不保存此密钥"
          />
        </label>
        <label>
          服务备份 ID
          <input
            className="mono"
            value={backup}
            onChange={(e) => setBackup(e.target.value.trim())}
            placeholder="创建备份后获取，或粘贴已有备份 ID"
          />
        </label>
        <div className="inline-actions">
          <button
            className="secondary"
            disabled={busy}
            onClick={() => run("backup")}
          >
            {busy ? <Busy label="处理中" /> : "创建 / 重试备份"}
          </button>
          <button
            className="secondary"
            disabled={busy}
            onClick={() => setConfirmRestore(true)}
          >
            恢复 / 重试到新目录
          </button>
        </div>
        {confirmRestore && (
          <div className="restore-confirm">
            <Notice kind="info">
              确认使用上方备份创建新的恢复目录？如果有待确认恢复，将复用其原始请求。
            </Notice>
            <button
              className="primary"
              disabled={busy}
              onClick={() => run("restore")}
            >
              确认恢复
            </button>
            <button
              className="text-button"
              disabled={busy}
              onClick={() => setConfirmRestore(false)}
            >
              取消
            </button>
          </div>
        )}
        <Notice>{error}</Notice>
        {result && <JsonView value={result} label="服务响应" />}
        <p className="field-help">
          网络中断时保留原始请求；使用相同密钥重试，避免重复操作。
        </p>
      </div>
    </section>
  );
}
