import { parseApiJson } from "./json.ts";
import type { Json } from "./types.ts";

export class ApiError extends Error {
  status: number;
  code: string;
  constructor(message: string, status = 0, code = "") {
    super(message);
    this.status = status;
    this.code = code;
  }
}
export async function api<T = Json>(
  key: string,
  path: string,
  body?: string,
  method = body === undefined ? "GET" : "POST",
  signal?: AbortSignal,
): Promise<T> {
  const response = await fetch("/api/v1/" + path, {
    method,
    headers: { apikey: key, "Content-Type": "application/json" },
    ...(body === undefined ? {} : { body }),
    cache: "no-store",
    signal,
  });
  const text = await response.text();
  let value;
  try {
    value = text ? parseApiJson(text) : null;
  } catch {
    throw new ApiError(
      `服务返回了非 JSON 响应（HTTP ${response.status}），请检查 API 代理配置。`,
      response.status,
    );
  }
  if (!response.ok)
    throw new ApiError(
      response.status === 401
        ? "API Key 无效或已失效，请重新连接。"
        : value?.message || `请求失败（HTTP ${response.status}）`,
      response.status,
      value?.code,
    );
  return value;
}
export const collectionPath = (id: string) => "index/" + encodeURIComponent(id);
export const message = (error: unknown) =>
  error instanceof Error ? error.message : String(error);

export const storage = {
  get<T>(name: string): T | null {
    const source = sessionStorage.getItem("anyseek." + name);
    return source === null ? null : JSON.parse(source);
  },
  set(name: string, value: unknown) {
    sessionStorage.setItem("anyseek." + name, JSON.stringify(value));
  },
  remove(name: string) {
    sessionStorage.removeItem("anyseek." + name);
  },
};
export async function ownerFor(key: string) {
  const bytes = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(key),
  );
  return Array.from(new Uint8Array(bytes), (n) =>
    n.toString(16).padStart(2, "0"),
  ).join("");
}
export const newIdentity = () =>
  Array.from(crypto.getRandomValues(new Uint8Array(16)), (x) =>
    x.toString(16).padStart(2, "0"),
  ).join("");
export const newOperationId = () => ({
  epoch: Math.floor(Date.now() / 86400000),
  nonce: newIdentity(),
});
