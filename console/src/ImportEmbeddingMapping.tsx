import { useState } from "react";
import { t } from "./i18n.ts";
import { message } from "./api.ts";
import { Notice } from "./components.tsx";
import {
  clearEmbeddingConfig, loadEmbeddingConfig, loadEmbeddingService, saveEmbeddingMapping,
  type EmbeddingConfig,
} from "./embeddings.ts";
import type { Collection } from "./types.ts";

export function importEmbeddingConfig(owner: string, collection: Collection): EmbeddingConfig | null {
  if (!collection.schema.some(f => f.index_vector && f.field_type === "Json")) return null;
  const saved = loadEmbeddingConfig(owner, collection.id);
  if (saved) return saved;
  const service = loadEmbeddingService(owner);
  return service ? {
    ...service,
    textFields: collection.schema.filter(f => f.field_type === "Text" && !f.index_vector).map(f => f.field),
    vectorField: collection.schema.find(f => f.index_vector && f.field_type === "Json")?.field || "",
  } : null;
}

export function ImportEmbeddingMapping({ owner, collection, config, onChange }: {
  owner: string; collection: Collection; config: EmbeddingConfig | null;
  onChange: (config: EmbeddingConfig | null) => void;
}) {
  const [service] = useState(() => loadEmbeddingService(owner));
  const saved = !!loadEmbeddingConfig(owner, collection.id);
  const [error, setError] = useState(""), [status, setStatus] = useState("");
  function change(value: EmbeddingConfig) { onChange(value); setError(""); setStatus(""); }
  if (!config) return <p className="field-help">{t("请先在连接与管理中保存公共模型配置，再回到导入界面设置字段映射。")}</p>;
  const differentModel = service && (service.endpoint !== config.endpoint || service.model !== config.model);
  return <div className="import-embedding-mapping">
    <h3>{t("集合字段映射")}</h3>
    <p className="field-help">{t("当前集合使用：{0} · {1}", config.model, config.endpoint)}</p>
    {differentModel && <>
      <Notice kind="info">{t("公共模型已变更，本集合继续使用原模型。切换模型后需自行重新生成已有向量。")}</Notice>
      <button type="button" className="secondary" onClick={() => change({ ...config, ...service })}>{t("使用当前公共模型")}</button>
    </>}
    <div className="embedding-fields">
      <span>{t("嵌入文本字段")}</span>
      <div className="field-options">{collection.schema.filter(f => !f.index_vector && ["Text", "String"].includes(f.field_type)).map(f => <label className="checkbox" key={f.field}>
        <input type="checkbox" checked={config.textFields.includes(f.field)} onChange={e => change({ ...config,
          textFields: e.target.checked ? [...config.textFields, f.field] : config.textFields.filter(name => name !== f.field),
        })} />{f.field}
      </label>)}</div>
      <p className="field-help">{t("按勾选顺序用换行连接文本；已有目标向量的记录将保留原向量。")}</p>
    </div>
    <label>{t("向量目标字段")}<select aria-label={t("向量目标字段")} value={config.vectorField} onChange={e => change({ ...config, vectorField: e.target.value })}>
      <option value="" disabled>{t("选择向量字段")}</option>
      {collection.schema.filter(f => f.index_vector && f.field_type === "Json").map(f => <option key={f.field} value={f.field}>{f.field}</option>)}
    </select></label>
    <p className="field-help">{t("集合维度：")}{collection.vector_dimensions || t("服务未提供")}{t("。修改模型不会转换已有向量，写入与查询须使用同一向量空间。")}</p>
    <p className="field-help">{t("写入时自动记住字段映射，下次导入自动回填，向量检索复用同一模型。")}</p>
    <div className="inline-actions">
      <button type="button" className="secondary" onClick={() => {
        try {
          onChange(saveEmbeddingMapping(owner, collection, config)); setError("");
          setStatus(t("集合字段映射已保存。导入数据和向量检索时可启用自动嵌入。"));
        } catch (e) { setError(message(e)); }
      }}>{t("保存集合字段映射")}</button>
      {saved && <button type="button" className="text-button" onClick={() => {
        try {
          clearEmbeddingConfig(owner, collection.id); onChange(importEmbeddingConfig(owner, collection));
          setError(""); setStatus(t("本集合的字段映射已清除；公共模型配置和其他集合不受影响。"));
        } catch (e) { setError(message(e)); }
      }}>{t("清除集合字段映射")}</button>}
    </div>
    <Notice>{error}</Notice><Notice kind="success">{status}</Notice>
  </div>;
}
