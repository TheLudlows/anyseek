import { t } from "./i18n.ts";
import type { Collection } from "./types.ts";

export type EmbeddingService = {
  endpoint: string;
  model: string;
  requiresKey: boolean;
};
export type EmbeddingConfig = EmbeddingService & {
  textFields: string[];
  vectorField: string;
};
const memoryKeys = new Map<string, string>();
const prefix = "anyseek.embedding.";
function scope(owner: string, collection: string) {
  // local/sessionStorage are already isolated by the console's origin.
  return prefix + JSON.stringify([owner, collection]);
}
function legacyKeyScope(owner: string, collection: string, config: EmbeddingService) {
  return scope(owner, collection) + ".key." + JSON.stringify([config.endpoint, config.model]);
}
function serviceScope(owner: string) {
  return prefix + JSON.stringify([owner]) + ".service";
}
function keyScope(owner: string, config: EmbeddingService) {
  return serviceScope(owner) + ".key." + JSON.stringify([config.endpoint, config.model]);
}
function readLocal(name: string): unknown {
  try { return JSON.parse(localStorage.getItem(name) || "null"); }
  catch { return null; }
}
function isService(value: unknown): value is EmbeddingService {
  return !!value && typeof value === "object" && "endpoint" in value && typeof value.endpoint === "string"
    && "model" in value && typeof value.model === "string" && "requiresKey" in value && typeof value.requiresKey === "boolean";
}
export function loadEmbeddingService(owner: string): EmbeddingService | null {
  const value = readLocal(serviceScope(owner));
  return isService(value) ? value : null;
}
export function loadEmbeddingConfig(owner: string, collection: string): EmbeddingConfig | null {
  const value = readLocal(scope(owner, collection));
  return isService(value) && "textFields" in value && Array.isArray(value.textFields)
    && value.textFields.every((f: unknown) => typeof f === "string")
    && "vectorField" in value && typeof value.vectorField === "string" ? value as EmbeddingConfig : null;
}
export function embeddingKey(owner: string, collection: string, config: EmbeddingService) {
  for (const name of [keyScope(owner, config), legacyKeyScope(owner, collection, config)]) {
    const key = memoryKeys.get(name) || sessionStorage.getItem(name);
    if (key) return key;
  }
  return "";
}
export function sessionEmbeddingKey(owner: string, collection: string, config: EmbeddingService) {
  const name = keyScope(owner, config);
  if (memoryKeys.has(name) || sessionStorage.getItem(name) !== null)
    return sessionStorage.getItem(name) !== null;
  return sessionStorage.getItem(legacyKeyScope(owner, collection, config)) !== null;
}
export function validateEmbeddingService(config: EmbeddingService) {
  let url: URL;
  try { url = new URL(config.endpoint); }
  catch { throw new Error(t("请填写完整的模型 HTTP(S) API 地址。")); }
  if (!["https:", "http:"].includes(url.protocol) || url.username || url.password || url.search || url.hash)
    throw new Error(t("模型地址须为完整 HTTP(S) 地址，不包含用户名、密码、查询参数或片段。"));
  if (typeof location !== "undefined" && location.protocol === "https:" && url.protocol === "http:"
    && !["localhost", "127.0.0.1", "[::1]"].includes(url.hostname))
    throw new Error(t("当前控制台使用 HTTPS，请为模型服务提供 HTTPS 地址。"));
  if (!config.model.trim()) throw new Error(t("请填写模型名称。"));
}
export function validateEmbeddingConfig(config: EmbeddingConfig, collection: Collection) {
  validateEmbeddingService(config);
  if (!config.textFields.length || new Set(config.textFields).size !== config.textFields.length
    || config.textFields.some((name) => !collection.schema.some((f) => f.field === name && !f.index_vector && ["Text", "String"].includes(f.field_type))))
    throw new Error(t("请选择集合中用于嵌入的文本字段。"));
  if (!collection.schema.some((f) => f.field === config.vectorField && f.index_vector && f.field_type === "Json"))
    throw new Error(t("请选择 Json 类型的向量目标字段。"));
  if (!Number.isInteger(collection.vector_dimensions) || collection.vector_dimensions! < 1)
    throw new Error(t("服务未返回集合向量维度，请更新 API 服务后再使用自动嵌入。"));
}
export function saveEmbeddingService(owner: string, config: EmbeddingService, key: string, remember: boolean) {
  validateEmbeddingService(config);
  // Only these explicit nonsecret properties may enter durable browser storage.
  const clean: EmbeddingService = {
    endpoint: config.endpoint.trim(), model: config.model.trim(),
    requiresKey: !!key,
  };
  const name = keyScope(owner, clean);
  // Clear stale credentials before a replacement; a storage error must not leave
  // the old credential associated with newly displayed configuration.
  memoryKeys.delete(name);
  sessionStorage.removeItem(name);
  if (remember && key) sessionStorage.setItem(name, key);
  localStorage.setItem(serviceScope(owner), JSON.stringify(clean));
  if (key) memoryKeys.set(name, key);
  return clean;
}
export function saveEmbeddingMapping(owner: string, collection: Collection, config: EmbeddingConfig) {
  validateEmbeddingConfig(config, collection);
  // Pin the service used for this collection. Editing the shared service must not
  // silently change the vector space of existing records or pending requests.
  const clean: EmbeddingConfig = {
    endpoint: config.endpoint.trim(), model: config.model.trim(), requiresKey: config.requiresKey,
    textFields: [...config.textFields], vectorField: config.vectorField,
  };
  localStorage.setItem(scope(owner, collection.id), JSON.stringify(clean));
  return clean;
}
export function saveEmbeddingConfig(owner: string, collection: Collection, config: EmbeddingConfig, key: string, remember: boolean) {
  validateEmbeddingConfig(config, collection);
  const service = saveEmbeddingService(owner, config, key, remember);
  return saveEmbeddingMapping(owner, collection, { ...config, ...service });
}
export function clearEmbeddingKeys(owner: string, collection?: string) {
  const matches = (key: string) => key.startsWith(prefix) && (collection === undefined
    ? key.startsWith(prefix + JSON.stringify([owner]).slice(0, -1) + ",") || key.startsWith(serviceScope(owner) + ".key.")
    : key.startsWith(scope(owner, collection) + ".key."));
  for (const key of memoryKeys.keys()) if (matches(key)) memoryKeys.delete(key);
  for (let i = sessionStorage.length - 1; i >= 0; i--) {
    const key = sessionStorage.key(i);
    if (key && matches(key)) sessionStorage.removeItem(key);
  }
}
export function clearEmbeddingService(owner: string) {
  clearEmbeddingKeys(owner);
  localStorage.removeItem(serviceScope(owner));
}
export function clearEmbeddingConfig(owner: string, collection: string) {
  clearEmbeddingKeys(owner, collection);
  localStorage.removeItem(scope(owner, collection));
}
export function validateVector(value: unknown, dimensions: number): asserts value is number[] {
  if (!Array.isArray(value) || value.length !== dimensions
    || !value.every((n) => typeof n === "number" && Number.isFinite(n)))
    throw new Error(t("模型返回向量无效：需要 {0} 维有限数值数组。", dimensions));
}
export const EMBEDDING_BATCH_SIZE = 16;
export async function embedTexts(config: EmbeddingService, key: string, texts: string[], dimensions: number | undefined, signal?: AbortSignal): Promise<number[][]> {
  if (config.requiresKey && !key) throw new Error(t("请在连接与管理中重新填写当前模型的 API Key；浏览器未保留该密钥。"));
  if (!texts.length || texts.length > EMBEDDING_BATCH_SIZE || texts.some((s) => !s.trim()))
    throw new Error(t("每次模型请求需包含 1–16 条非空文本。"));
  const body = JSON.stringify({ model: config.model, input: texts, encoding_format: "float" });
  if (new TextEncoder().encode(body).length > 512 * 1024)
    throw new Error(t("单批嵌入文本超过 512 KiB，请减少文本或拆分导入。"));
  const timeout = AbortSignal.timeout(60_000);
  let response: Response;
  try {
    response = await fetch(config.endpoint, {
      method: "POST", mode: "cors", credentials: "omit", redirect: "error",
      referrerPolicy: "no-referrer", cache: "no-store",
      headers: { "Content-Type": "application/json", ...(key ? { Authorization: "Bearer " + key } : {}) },
      body, signal: signal ? AbortSignal.any([signal, timeout]) : timeout,
    });
  } catch (error) {
    if (signal?.aborted) throw error;
    if (timeout.aborted) throw new Error(t("模型请求超过 60 秒，请稍后重试。"));
    throw new Error(t("无法连接模型服务，请检查地址、网络、HTTPS 和服务端 CORS 设置（需允许 POST、Content-Type 和 Authorization）。"));
  }
  if (!response.ok) throw new Error(t("模型请求失败（HTTP {0}）{1}", response.status, response.status === 401 || response.status === 403 ? t("，请检查模型 API Key。") : response.status === 429 ? t("，请稍后重试。") : t("，请检查模型服务配置。")));
  let data;
  try { data = (await response.json()).data; }
  catch { throw new Error(t("模型未返回有效的 JSON 响应。")); }
  if (!Array.isArray(data) || data.length !== texts.length) throw new Error(t("模型返回的向量数量与输入不一致。"));
  const vectors: number[][] = new Array(texts.length);
  for (const row of data) {
    if (!row || !Number.isInteger(row.index) || row.index < 0 || row.index >= texts.length || vectors[row.index])
      throw new Error(t("模型返回了重复或无效的向量序号。"));
    const expected = dimensions ?? (Array.isArray(row.embedding) ? row.embedding.length : 0);
    if (!Number.isInteger(expected) || expected < 1) throw new Error(t("模型未返回有效的非空向量。"));
    validateVector(row.embedding, expected);
    vectors[row.index] = row.embedding;
  }
  return vectors;
}
