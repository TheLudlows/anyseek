import {
  api,
  ApiError,
  collectionPath,
  newIdentity,
  newOperationId,
  storage,
} from "./api.ts";
import { sendIngest } from "./ingest.ts";
import type { EmbeddingConfig } from "./embeddings.ts";
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
    throw new Error("请输入一个 JSON 对象或非空的对象数组。");
  return documentJsonSlices(text, parsed).map((raw, i) => {
    if (envelopeMode) {
      const doc = docs[i];
      if (
        (!Object.hasOwn(doc, "document") &&
          !Object.hasOwn(doc, "vector_patch")) ||
        !doc.record_id ||
        !doc.routing_key ||
        !doc.expected
      )
        throw new Error(
          "Record envelope 需要 record_id、routing_key、expected，以及 document 或 vector_patch。",
        );
      if (Object.hasOwn(doc, "operation_id"))
        throw new Error("Operation ID 由控制台管理，请移除此字段。");
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
      routing_key: id,
      expected: "Absent",
    });
    return {
      body: header.slice(0, -1) + ',"document":' + raw + "}",
    };
  });
}
export type PendingWrites = {
  owner: string;
  collection: string;
  requests: { body: string; embedded?: boolean;
    outcome?: "committed" | "rejected" | "unknown" | "not_attempted" }[];
  delivery?: "ingest";
  embedding?: { config: EmbeddingConfig; dimensions: number };
  completed: number;
  unresolved: boolean;
  rejected?: boolean;
};
export async function sendWrites(
  pending: PendingWrites,
  key: string,
  owner: string,
  progress: (done: number) => void = () => {},
) {
  if (pending.owner !== owner)
    throw new Error("保留的写入属于另一连接，请使用原 API Key 处理。");
  if (pending.delivery === "ingest") return sendIngest(pending, key, progress);
  for (let i = pending.completed; i < pending.requests.length; i++) {
    const previouslyUnresolved = pending.unresolved;
    pending.unresolved = true;
    pending.rejected = false;
    storage.set("pendingWrites", pending); // Storage must succeed before dispatch.
    try {
      await api(
        key,
        collectionPath(pending.collection) + "/doc",
        pending.requests[i].body,
      );
    } catch (error) {
      pending.rejected =
        !previouslyUnresolved &&
        error instanceof ApiError &&
        ["rejected", "conflict"].includes(error.code);
      pending.unresolved = !pending.rejected;
      storage.set("pendingWrites", pending);
      throw error;
    }
    pending.completed = i + 1;
    pending.unresolved = false;
    storage.set("pendingWrites", pending);
    progress(pending.completed);
  }
  storage.remove("pendingWrites");
}
