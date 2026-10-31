import { t } from "./i18n.ts";
import {
  api,
  ApiError,
  collectionPath,
  newIdentity,
  newOperationId,
} from "./api.ts";
import { sendIngest } from "./ingest.ts";
import { beginWriteAttempt, finishWriteAttempt, clearPendingWrites, restoreWriteOutcomes, type PendingWrites } from "./pending-writes.ts";
export type { PendingWrites } from "./pending-writes.ts";
import type { Json } from "./types.ts";

export function documentJsonSlices(text: string, parsed: Json): string[] {
  if (!Array.isArray(parsed)) return [text];
  const slices: string[] = [];
  let start = 1,
    depth = 0,
    quoted = false,
    escaped = false;
  for (let i = 1; i < text.length; i++) {
    const ch = text[i];
    if (quoted) {
      if (escaped) escaped = false;
      else if (ch === "\\") escaped = true;
      else if (ch === '"') quoted = false;
    } else if (ch === '"') quoted = true;
    else if (ch === "[" || ch === "{") depth++;
    else if (ch === "]" && depth === 0) {
      slices.push(text.slice(start, i).trim());
      break;
    } else if (ch === "]" || ch === "}") depth--;
    else if (ch === "," && depth === 0) {
      slices.push(text.slice(start, i).trim());
      start = i + 1;
    }
  }
  return slices;
}
export function prepareDocumentRequests(source: string, envelopeMode = false) {
  const text = source.replace(/^\uFEFF/, "").trim();
  const parsed = JSON.parse(text);
  const docs = Array.isArray(parsed) ? parsed : [parsed];
  if (
    !docs.length ||
    !docs.every((d) => d && typeof d === "object" && !Array.isArray(d))
  )
    throw new Error(t("请输入一个 JSON 对象或非空的对象数组。"));
  return documentJsonSlices(text, parsed).map((raw, i) => {
    if (envelopeMode) {
      const doc = docs[i];
      if (
        (!Object.hasOwn(doc, "document") &&
          !Object.hasOwn(doc, "vector_patch")) ||
        !doc.record_id ||
        !doc.expected
      )
        throw new Error(
          t("Record envelope 需要 record_id、expected，以及 document 或 vector_patch。"),
        );
      if (Object.hasOwn(doc, "operation_id"))
        throw new Error(t("Operation ID 由控制台管理，请移除此字段。"));
      return {
        body:
          raw.slice(0, -1) +
          ',"operation_id":' +
          JSON.stringify(newOperationId()) +
          "}",
      };
    }
    const id = newIdentity();
    const header = JSON.stringify({
      operation_id: newOperationId(),
      record_id: id,
      expected: "Absent",
    });
    return {
      body: header.slice(0, -1) + ',"document":' + raw + "}",
    };
  });
}
export async function sendWrites(
  pending: PendingWrites,
  key: string,
  owner: string,
  progress: (done: number) => void = () => {},
) {
  if (pending.owner !== owner)
    throw new Error(t("保留的写入属于另一连接，请使用原 API Key 处理。"));
  restoreWriteOutcomes(pending);
  if (pending.delivery === "ingest") return sendIngest(pending, key, progress);
  for (let i = pending.completed; i < pending.requests.length; i++) {
    const batch = [pending.requests[i]];
    const unknownBefore = beginWriteAttempt(pending, batch);
    try {
      await api(
        key,
        collectionPath(pending.collection) + "/doc",
        pending.requests[i].body,
      );
    } catch (error) {
      const outcome = error instanceof ApiError && ["rejected", "conflict"].includes(error.code)
        ? "rejected" : "unknown";
      finishWriteAttempt(pending, batch, unknownBefore, [outcome]);
      throw error;
    }
    finishWriteAttempt(pending, batch, unknownBefore, ["committed"]);
    progress(pending.completed);
  }
  clearPendingWrites();
}
