import { t } from "./i18n.ts";
import { useEffect, useRef, useState } from "react";
import { Busy, Notice } from "./components.tsx";
import { message } from "./api.ts";
import {
  clearEmbeddingService, embedTexts, embeddingKey,
  loadEmbeddingConfig, loadEmbeddingService, saveEmbeddingService,
  sessionEmbeddingKey, validateEmbeddingService, type EmbeddingService,
} from "./embeddings.ts";
import type { Collection } from "./types.ts";

export function EmbeddingSettings({ owner, collection }: { owner: string; collection?: Collection }) {
  const [saved, setSaved] = useState(() => loadEmbeddingService(owner));
  // Existing settings can be explicitly reused as the shared service while
  // preserving each collection's vector space and pending requests.
  const [initial] = useState(() => saved || (collection ? loadEmbeddingConfig(owner, collection.id) : null));
  const [endpoint, setEndpoint] = useState(initial?.endpoint || "");
  const [model, setModel] = useState(initial?.model || "");
  const [key, setKey] = useState(() => initial ? embeddingKey(owner, collection?.id || "", initial) : "");
  const [remember, setRemember] = useState(() => !!initial && sessionEmbeddingKey(owner, collection?.id || "", initial));
  const [busy, setBusy] = useState(false), [error, setError] = useState(""), [status, setStatus] = useState("");
  const request = useRef<AbortController | null>(null);
  useEffect(() => () => request.current?.abort(), []);
  function config(): EmbeddingService {
    return { endpoint: endpoint.trim(), model: model.trim(), requiresKey: !!key.trim() };
  }
  function changed() { setStatus(""); setError(""); }
  async function test() {
    setError(""); setStatus(""); setBusy(true);
    const controller = new AbortController(); request.current = controller;
    try {
      const value = config(); validateEmbeddingService(value);
      const [vector] = await embedTexts(value, key.trim(), ["AnySeek connection test"], undefined, controller.signal);
      if (!controller.signal.aborted) setStatus(t("连接成功，返回 {0} 维向量。请保存配置后使用。", vector.length));
    } catch (e) { if (!controller.signal.aborted) setError(message(e)); }
    finally { if (!controller.signal.aborted) setBusy(false); }
  }
  return <>
    <section className="panel embedding-panel">
      <h2>{t("公共嵌入服务")}</h2>
      <p className="muted">{t("模型地址、名称和 Key 在当前连接下配置一次，各集合复用。配置仅保存在当前浏览器，浏览器直接调用模型服务。")}</p>
      <form onSubmit={(e) => {
        e.preventDefault(); setError(""); setStatus("");
        try {
          setSaved(saveEmbeddingService(owner, config(), key.trim(), remember));
          setStatus(t("公共模型配置已保存。请在导入界面设置各集合的文本字段和向量字段。"));
        } catch (e) { setError(message(e)); }
      }}>
        <fieldset disabled={busy}>
          <label>{t("Embeddings API 地址")}<input type="url" required value={endpoint} placeholder="http://127.0.0.1:11434/v1/embeddings" onChange={(e) => {
            setEndpoint(e.target.value); setKey(""); setRemember(false); changed();
          }} /></label>
          <p className="field-help">{t("填写完整的 OpenAI 兼容 embeddings 接口地址。服务需允许当前控制台来源的跨域请求。")}</p>
          <div className="form-row">
            <label>{t("模型名称")}<input value={model} required placeholder={t("服务提供的模型名称")} onChange={(e) => { setModel(e.target.value); setKey(""); setRemember(false); changed(); }} /></label>
            <label>{t("模型 API Key（可选）")}<input type="password" autoComplete="off" value={key} onChange={(e) => { setKey(e.target.value); changed(); }} placeholder={t("仅发送给上方模型服务")} /></label>
          </div>
          <label className="checkbox"><input type="checkbox" checked={remember} onChange={(e) => { setRemember(e.target.checked); changed(); }} />{t("在当前标签页会话中记住模型 Key")}</label>
          <p className="field-help">{t("默认仅保存在内存，刷新需重新输入；勾选后可跨刷新保留。断开 AnySeek 连接时清除模型 Key。")}</p>
          <Notice kind="info">{t("修改公共模型不会切换已有集合的模型。需要切换时，请在导入界面选择使用当前公共模型，并自行重新生成已有向量。")}</Notice>
          <Notice>{error}</Notice><Notice kind="success">{status}</Notice>
          <div className="inline-actions">
            <button type="submit" className="primary">{t("保存公共模型配置")}</button>
            <button type="button" className="secondary" onClick={test}>{busy ? <Busy label={t("测试中")} /> : t("测试连接")}</button>
            {saved && <button type="button" className="text-button" onClick={() => {
              try {
                clearEmbeddingService(owner); setSaved(null); setKey(""); setEndpoint(""); setModel(""); setRemember(false); setError("");
                setStatus(t("公共模型配置和模型密钥已清除；已有集合映射和待处理请求仍保留。"));
              } catch (e) { setError(message(e)); }
            }}>{t("清除公共模型配置")}</button>}
          </div>
        </fieldset>
      </form>
    </section>
  </>;
}
