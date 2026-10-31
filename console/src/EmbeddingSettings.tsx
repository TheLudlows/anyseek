import { t } from "./i18n.ts";
import { useEffect, useRef, useState } from "react";
import { Busy, Notice } from "./components.tsx";
import { message } from "./api.ts";
import {
  clearEmbeddingConfig, embedTexts, embeddingKey, loadEmbeddingConfig,
  saveEmbeddingConfig, sessionEmbeddingKey, validateEmbeddingConfig,
  type EmbeddingConfig,
} from "./embeddings.ts";
import type { Collection } from "./types.ts";

export function EmbeddingSettings({ owner, collection }: { owner: string; collection: Collection }) {
  const [saved, setSaved] = useState(() => loadEmbeddingConfig(owner, collection.id));
  const [endpoint, setEndpoint] = useState(saved?.endpoint || "");
  const [model, setModel] = useState(saved?.model || "");
  const [fields, setFields] = useState(saved?.textFields || collection.schema.filter((f) => f.field_type === "Text" && !f.index_vector).map((f) => f.field));
  const [target, setTarget] = useState(saved?.vectorField || collection.schema.find((f) => f.index_vector && f.field_type === "Json")?.field || "");
  const [key, setKey] = useState(() => saved ? embeddingKey(owner, collection.id, saved) : "");
  const [remember, setRemember] = useState(() => !!saved && sessionEmbeddingKey(owner, collection.id, saved));
  const [busy, setBusy] = useState(false), [error, setError] = useState(""), [status, setStatus] = useState("");
  const request = useRef<AbortController | null>(null);
  useEffect(() => () => request.current?.abort(), []);
  function config(): EmbeddingConfig {
    return { endpoint: endpoint.trim(), model: model.trim(), textFields: fields, vectorField: target, requiresKey: !!key.trim() };
  }
  function changed() { setStatus(""); setError(""); }
  async function test() {
    setError(""); setStatus(""); setBusy(true);
    const controller = new AbortController(); request.current = controller;
    try {
      const value = config(); validateEmbeddingConfig(value, collection);
      await embedTexts(value, key.trim(), ["AnySeek connection test"], collection.vector_dimensions!, controller.signal);
      if (!controller.signal.aborted) setStatus(t("连接成功，返回 {0} 维向量。请保存配置后使用。", collection.vector_dimensions));
    } catch (e) { if (!controller.signal.aborted) setError(message(e)); }
    finally { if (!controller.signal.aborted) setBusy(false); }
  }
  return <section className="panel embedding-panel">

    <h2>{t("浏览器嵌入 ·")}{collection.name}</h2>
    <p className="muted">{t("浏览器直接调用模型服务。地址、模型名称和字段映射只保存在当前浏览器；不会发送给 AnySeek。")}</p>
    <form onSubmit={(e) => {
      e.preventDefault(); setError(""); setStatus("");
      try {
        setSaved(saveEmbeddingConfig(owner, collection, config(), key.trim(), remember));
        setStatus(t("本地配置已保存。导入数据和向量检索时可启用自动嵌入。"));
      } catch (e) { setError(message(e)); }
    }}>
      <fieldset disabled={busy}>
        <label>{t("Embeddings API 地址")}<input type="url" required value={endpoint} placeholder="https://model.example.com/v1/embeddings" onChange={(e) => {
          setEndpoint(e.target.value); setKey(""); setRemember(false); changed();
        }} /></label>
        <p className="field-help">{t("填写完整的 OpenAI 兼容 embeddings 接口地址。服务需允许当前控制台来源的跨域请求。")}</p>
        <div className="form-row">
          <label>{t("模型名称")}<input value={model} required placeholder={t("服务提供的模型名称")} onChange={(e) => { setModel(e.target.value); changed(); }} /></label>
          <label>{t("模型 API Key（可选）")}<input type="password" autoComplete="off" value={key} onChange={(e) => { setKey(e.target.value); changed(); }} placeholder={t("仅发送给上方模型服务")} /></label>
        </div>
        <label className="checkbox"><input type="checkbox" checked={remember} onChange={(e) => { setRemember(e.target.checked); changed(); }} />{t("在当前标签页会话中记住模型 Key")}</label>
        <p className="field-help">{t("默认仅保存在内存，刷新需重新输入；勾选后可跨刷新保留。断开 AnySeek 连接时清除模型 Key。")}</p>
        <div className="embedding-fields">
          <span>{t("嵌入文本字段")}</span>
          <div className="field-options">{collection.schema.filter((f) => !f.index_vector && ["Text", "String"].includes(f.field_type)).map((f) => <label className="checkbox" key={f.field}>
            <input type="checkbox" checked={fields.includes(f.field)} onChange={(e) => { setFields(e.target.checked ? [...fields, f.field] : fields.filter((name) => name !== f.field)); changed(); }} />{f.field}
          </label>)}</div>
          <p className="field-help">{t("按勾选顺序用换行连接文本；已有目标向量的记录将保留原向量。")}</p>
        </div>
        <label>{t("向量目标字段")}<select value={target} required onChange={(e) => { setTarget(e.target.value); changed(); }}>
          <option value="" disabled>{t("选择向量字段")}</option>
          {collection.schema.filter((f) => f.index_vector && f.field_type === "Json").map((f) => <option key={f.field} value={f.field}>{f.field}</option>)}
        </select></label>
        <p className="field-help">{t("集合维度：")}{collection.vector_dimensions || t("服务未提供")}{t("。修改模型不会转换已有向量，写入与查询须使用同一向量空间。")}</p>
        <Notice kind="info">{t("导入的所选文本和自然语言查询将发送至上方模型地址。测试连接仅发送固定测试文本。")}</Notice>
        <Notice>{error}</Notice><Notice kind="success">{status}</Notice>
        <div className="inline-actions">
          <button type="submit" className="primary">{t("保存本地配置")}</button>
          <button type="button" className="secondary" onClick={test}>{busy ? <Busy label={t("测试中")} /> : t("测试连接")}</button>
          {saved && <button type="button" className="text-button" onClick={() => {
            try { clearEmbeddingConfig(owner, collection.id); setSaved(null); setKey(""); setEndpoint(""); setModel(""); setRemember(false); setError(""); setStatus(t("本集合的本地模型配置和密钥已清除；待处理导入仍保留原请求。")); }
            catch (e) { setError(message(e)); }
          }}>{t("清除本地模型配置")}</button>}
        </div>
      </fieldset>
    </form>
  </section>;
}
