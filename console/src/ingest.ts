import { t } from "./i18n.ts";
import { collectionPath } from "./api.ts";
import { parseApiJson } from "./json.ts";
import { beginWriteAttempt, finishWriteAttempt, clearPendingWrites, type PendingWrites } from "./pending-writes.ts";

export async function sendIngest(pending: PendingWrites, key: string, progress: (done: number) => void) {
  if (pending.embedding && pending.requests.some((request) => !request.embedded))
    throw new Error(t("请先完成向量生成，再提交写入。"));
  const remaining = pending.requests.filter((request) => request.outcome !== "committed");
  for (let offset = 0; offset < remaining.length;) {
    const batch: typeof remaining = [];
    let bytes = 14;
    while (offset < remaining.length && batch.length < 64) {
      const request = remaining[offset];
      const size = new TextEncoder().encode(request.body).length + 1;
      if (bytes + size > 2 * 1024 * 1024) {
        if (!batch.length) throw new Error(t("单条写入超过 2 MiB，请处理保留请求后拆分导入。"));
        break;
      }
      batch.push(request); bytes += size; offset++;
    }
    const unknownBefore = beginWriteAttempt(pending, batch);
    const response = await fetch("/api/v1/" + collectionPath(pending.collection) + "/ingest", {
      method: "POST", headers: { apikey: key, "Content-Type": "application/json" }, cache: "no-store",
      body: '{"records":[' + batch.map((item) => item.body).join(",") + "]}",
    });
    const value = parseApiJson(await response.text());
    if (!value || !Array.isArray(value.results) || value.results.length !== batch.length)
      throw new Error(t("未收到可确认的逐项写入结果（HTTP {0}），请重试原始请求。", response.status));
    // Validate the complete envelope before applying any receipts.
    value.results.forEach((row: any, i: number) => {
      const operation = JSON.parse(batch[i].body).operation_id;
      if (!row || row.operation_id?.epoch !== operation.epoch || row.operation_id?.nonce !== operation.nonce
        || !["committed", "rejected", "outcome_unknown", "not_attempted"].includes(row.code)
        || (row.code === "committed" && (!row.receipt || row.receipt.record_id !== JSON.parse(batch[i].body).record_id)))
        throw new Error(t("写入回执身份或格式不匹配，请保留原请求重试。"));
    });
    finishWriteAttempt(pending, batch, unknownBefore,
      value.results.map((row: any) => row.code === "outcome_unknown" ? "unknown" : row.code));
    progress(pending.completed);
    if (batch.some((item) => item.outcome !== "committed")) {
      const rejected = pending.requests.filter((item) => item.outcome === "rejected").length;
      throw new Error(t("已确认提交 {0} 条；{1}{2}请重试保留请求或检查服务状态。", pending.completed, rejected ? t("{0} 条被拒绝；", rejected) : "", pending.unresolved ? t("部分结果未知；") : ""));
    }
  }
  clearPendingWrites();
}
