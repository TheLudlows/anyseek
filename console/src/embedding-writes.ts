import { storage } from "./api.ts";
import { apiFieldText, apiJsonText, parseApiJson } from "./json.ts";
import { prepareDocumentRequests, type PendingWrites } from "./writes.ts";
import { EMBEDDING_BATCH_SIZE, embedTexts, embeddingKey, validateEmbeddingConfig, validateVector, type EmbeddingConfig } from "./embeddings.ts";
import type { Collection } from "./types.ts";

function inputText(document: Record<string, unknown>, fields: string[]) {
  const values = fields.map((field) => document[field]).filter((value) => value != null);
  if (values.some((value) => typeof value !== "string")) throw new Error("嵌入文本字段必须是字符串。");
  const text = values.join("\n").trim();
  if (!text) throw new Error("记录的所选嵌入字段为空，请填写文本或提供已有向量。");
  if (new TextEncoder().encode(text).length > 30 * 1024)
    throw new Error("单条嵌入文本超过 30 KiB，请先拆分文本。");
  return text;
}
function withVector(body: string, field: string, vector: number[]) {
  const request = parseApiJson(body);
  const raw = apiJsonText(request.document).trim();
  const document = raw.slice(0, -1) + (Object.keys(request.document).length ? "," : "")
    + JSON.stringify(field) + ":" + JSON.stringify(vector) + "}";
  // Reuse original JSON spans for all canonical fields, including i64 numbers.
  return "{" + Object.keys(request).map((name) => JSON.stringify(name) + ":" + (name === "document" ? document
    : typeof request[name] === "string" ? JSON.stringify(request[name]) : apiFieldText(request, name) || "null")).join(",") + "}";
}
export function prepareEmbeddedWrites(source: string, owner: string, collection: Collection, config: EmbeddingConfig): PendingWrites {
  validateEmbeddingConfig(config, collection);
  const requests = prepareDocumentRequests(source).map((request) => {
    const document = JSON.parse(request.body).document;
    const embedded = Object.hasOwn(document, config.vectorField);
    if (embedded) validateVector(document[config.vectorField], collection.vector_dimensions!);
    else inputText(document, config.textFields);
    return { ...request, embedded };
  });
  return { owner, collection: collection.id, requests, completed: 0, unresolved: false, delivery: "ingest",
    embedding: { config: { ...config, textFields: [...config.textFields] }, dimensions: collection.vector_dimensions! } };
}
export async function generatePendingEmbeddings(pending: PendingWrites, owner: string, collection: Collection,
  progress: (done: number) => void = () => {}, signal?: AbortSignal) {
  if (pending.owner !== owner || pending.collection !== collection.id) throw new Error("请回到原连接与集合处理保留的嵌入任务。");
  if (!pending.embedding) return;
  const { config, dimensions } = pending.embedding;
  validateEmbeddingConfig(config, collection);
  if (dimensions !== collection.vector_dimensions) throw new Error("集合维度已变化，请检查原集合和保留任务。");
  const remaining = pending.requests.filter((request) => !request.embedded);
  for (let offset = 0; offset < remaining.length; offset += EMBEDDING_BATCH_SIZE) {
    signal?.throwIfAborted();
    const batch = remaining.slice(offset, offset + EMBEDDING_BATCH_SIZE);
    const vectors = await embedTexts(config, embeddingKey(owner, collection.id, config), batch.map((request) => inputText(JSON.parse(request.body).document, config.textFields)), dimensions, signal);
    signal?.throwIfAborted();
    batch.forEach((request, i) => {
      request.body = withVector(request.body, config.vectorField, vectors[i]);
      request.embedded = true;
    });
    storage.set("pendingWrites", pending);
    progress(pending.requests.filter((request) => request.embedded).length);
  }
}
