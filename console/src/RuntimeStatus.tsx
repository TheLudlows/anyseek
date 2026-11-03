import { useEffect, useState } from "react";
import { api, collectionPath, message } from "./api.ts";
import { CollectionStatistics } from "./CollectionStatistics.tsx";
import { Busy, Notice } from "./components.tsx";
import { getLocale, t } from "./i18n.ts";
import { counter, indexState } from "./runtime-status.ts";
import type { Collection, Json } from "./types.ts";

export function RuntimeStatus({ apiKey, collection, refreshVersion = 0, showStatistics = false }: {
  apiKey: string; collection: Collection; refreshVersion?: number; showStatistics?: boolean;
}) {
  const [revision, setRevision] = useState(0);
  const [progress, setProgress] = useState<Json>(null);
  const [info, setInfo] = useState<Collection | null>(null);
  const [error, setError] = useState("");
  const [statisticsError, setStatisticsError] = useState("");
  const [busy, setBusy] = useState(true);
  useEffect(() => {
    const controller = new AbortController();
    const path = collectionPath(collection.id);
    setBusy(true); setProgress(null); setInfo(null); setError(""); setStatisticsError("");
    const status = api(apiKey, path + "/ingest", undefined, "GET", controller.signal)
      .then(value => { if (!controller.signal.aborted) setProgress(value); })
      .catch(e => { if (!controller.signal.aborted) setError(message(e)); });
    const statistics = showStatistics
      ? api<Collection>(apiKey, path, undefined, "GET", controller.signal)
        .then(value => { if (!controller.signal.aborted) setInfo(value); })
        .catch(e => { if (!controller.signal.aborted) setStatisticsError(message(e)); })
      : Promise.resolve();
    void Promise.all([status, statistics]).finally(() => { if (!controller.signal.aborted) setBusy(false); });
    return () => controller.abort();
  }, [apiKey, collection.id, refreshVersion, revision, showStatistics]);
  const state = indexState(progress);
  const labels = {
    unknown: t("索引状态未知"), recovery: t("需要恢复"), error: t("索引异常"),
    pending: t("索引尚未追平"), caught_up: t("索引已追平此提交快照"),
  };
  return <section className="runtime-status" aria-label={t("运行状态")}>
    <div className="section-heading"><h3>{t("运行状态")}</h3>
      <button type="button" className="secondary" disabled={busy} onClick={() => setRevision(value => value + 1)}>{t("刷新运行状态")}</button>
    </div>
    <p className="field-help">{t("持久提交成功不等于立即可搜。全文与向量 / 元数据均追平后，才能确认此快照的索引进度。")}</p>
    {busy ? <Busy /> : <Notice kind={state === "error" || state === "recovery" ? "error" : "info"}>{labels[state]}</Notice>}
    <Notice>{error}</Notice>
    {typeof progress?.index_error === "string" && progress.index_error && <Notice>{progress.index_error}</Notice>}
    <table className="data-table statistics-table" aria-label={t("索引进度")}>
      <thead><tr><th>{t("统计项")}</th><th>{t("序号")}</th></tr></thead>
      <tbody>{[
        ["committed_seq", t("持久提交位置")], ["lexical_seq", t("全文索引位置")], ["vector_seq", t("向量 / 元数据位置")],
      ].map(([key, label]) => <tr key={key}><th scope="row">{label}</th><td>{counter(progress, key)?.toLocaleString(getLocale()) ?? t("未提供")}</td></tr>)}</tbody>
    </table>
    <p className="field-help">{t("位置是序号，不是记录数；状态为手动刷新的独立快照，不保证后续写入已可查询。")}</p>
    {showStatistics && <details>
      <summary>{t("高级服务统计")}</summary>
      <Notice>{statisticsError}</Notice>
      <CollectionStatistics statistics={info?.statistics} limits={info?.limits} />
    </details>}
  </section>;
}
