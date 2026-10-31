import { beforeEach, test } from "node:test";
import assert from "node:assert/strict";
import {
  clearEmbeddingConfig, clearEmbeddingKeys, embedTexts, embeddingKey,
  loadEmbeddingConfig, saveEmbeddingConfig, validateEmbeddingConfig,
  type EmbeddingConfig,
} from "../src/embeddings.ts";
import { generatePendingEmbeddings, prepareEmbeddedWrites } from "../src/embedding-writes.ts";
import { sendWrites, type PendingWrites } from "../src/writes.ts";
import { storage } from "../src/api.ts";
import type { Collection } from "../src/types.ts";

function browserStorage() {
  const data = new Map<string, string>();
  return { getItem: (key: string) => data.get(key) ?? null,
    setItem: (key: string, value: string) => { data.set(key, value); },
    removeItem: (key: string) => { data.delete(key); },
    key: (index: number) => [...data.keys()][index] ?? null,
    get length() { return data.size; }, data };
}
let local: ReturnType<typeof browserStorage>, session: ReturnType<typeof browserStorage>;
const collection: Collection = {
  id: "docs", name: "docs", version: "1", indexed_doc_count: 0, vector_dimensions: 2, statistics: {}, limits: {},
  schema: [{ field: "body", field_type: "Text" }, { field: "vector", field_type: "Json", index_vector: true }],
};
const config: EmbeddingConfig = { endpoint: "https://models.example/v1/embeddings", model: "test-model", textFields: ["body"], vectorField: "vector", requiresKey: true };
beforeEach(() => {
  local = browserStorage(); session = browserStorage();
  Object.defineProperty(globalThis, "localStorage", { configurable: true, value: local });
  Object.defineProperty(globalThis, "sessionStorage", { configurable: true, value: session });
  clearEmbeddingKeys("owner");
});
function modelResponse(count: number) {
  return new Response(JSON.stringify({ data: Array.from({ length: count }, (_, index) => ({ index, embedding: [index + 0.5, 1] })).reverse() }));
}
function receipts(requests: any[], codes: string[]) {
  return new Response(JSON.stringify({ results: requests.map((r, i) => ({ operation_id: r.operation_id, code: codes[i],
    ...(codes[i] === "committed" ? { receipt: { record_id: r.record_id, seq: i + 1 } } : {}) })) }));
}

test("config is local, secrets stay in memory or opted-in session and never cross connection/endpoint scopes", () => {
  saveEmbeddingConfig("owner", collection, { ...config, apiKey: "must-not-save" } as EmbeddingConfig, "model-secret", false);
  assert.equal(session.length, 0);
  assert.equal(embeddingKey("owner", collection.id, config), "model-secret");
  assert.equal(embeddingKey("other", collection.id, config), "");
  assert.equal(embeddingKey("owner", "other", config), "");
  assert.equal(embeddingKey("owner", collection.id, { ...config, endpoint: "https://other.example/embeddings" }), "");
  assert.equal(loadEmbeddingConfig("other", collection.id), null);
  assert.ok(!JSON.stringify([...local.data]).includes("secret"));
  assert.ok(!JSON.stringify([...local.data]).includes("must-not-save"));
  saveEmbeddingConfig("owner", collection, config, "session-secret", true);
  assert.ok(JSON.stringify([...session.data]).includes("session-secret"));
  clearEmbeddingKeys("owner");
  assert.equal(embeddingKey("owner", collection.id, config), "");
  assert.ok(loadEmbeddingConfig("owner", collection.id));
  clearEmbeddingConfig("owner", collection.id);
  assert.equal(loadEmbeddingConfig("owner", collection.id), null);
});

test("model calls omit service credentials, cookies and redirects; reversed indexed rows are reordered", async () => {
  globalThis.fetch = async (url, options) => {
    assert.equal(url, config.endpoint);
    assert.deepEqual(options?.headers, { "Content-Type": "application/json", Authorization: "Bearer model-secret" });
    assert.equal(options?.credentials, "omit"); assert.equal(options?.redirect, "error");
    assert.equal(options?.referrerPolicy, "no-referrer");
    assert.deepEqual(JSON.parse(String(options?.body)), { model: "test-model", input: ["a", "b"], encoding_format: "float" });
    return modelResponse(2);
  };
  assert.deepEqual(await embedTexts(config, "model-secret", ["a", "b"], 2), [[0.5, 1], [1.5, 1]]);
});

test("bad model outputs, dimensions, credentials and endpoint forms fail before any canonical writes", async () => {
  for (const endpoint of ["javascript:alert(1)", "https://user:secret@models.example/embed", "https://models.example/embed?key=secret"]) {
    assert.throws(() => validateEmbeddingConfig({ ...config, endpoint }, collection));
  }
  for (const data of [[], [{ index: 1, embedding: [1, 0] }], [{ index: 0, embedding: [1] }], [{ index: 0, embedding: [null, 1] }]]) {
    globalThis.fetch = async () => new Response(JSON.stringify({ data }));
    await assert.rejects(embedTexts(config, "secret", ["a"], 2));
  }
  globalThis.fetch = async () => { throw new Error("should not dispatch"); };
  await assert.rejects(embedTexts(config, "", ["a"], 2), /重新填写/);
  globalThis.fetch = async () => new Response("model-secret echoed by server", { status: 401 });
  await assert.rejects(embedTexts(config, "secret", ["a"], 2), (error: Error) => !error.message.includes("model-secret") && error.message.includes("401"));
});

test("embedding interruption resumes saved batches, preserves i64 and operation identities, and skips existing vectors", async () => {
  saveEmbeddingConfig("owner", collection, config, "secret", false);
  const source = "[" + Array.from({ length: 17 }, (_, i) => `{"body":"text ${i}","rank":9223372036854775807}`).join(",") + ',{"body":"manual","vector":[8,9]}]';
  let pending = prepareEmbeddedWrites(source, "owner", collection, config);
  const ids = pending.requests.map((r) => JSON.parse(r.body).operation_id);
  storage.set("pendingWrites", pending);
  let calls = 0;
  globalThis.fetch = async (_url, options) => {
    calls++;
    if (calls === 2) throw new Error("offline");
    return modelResponse(JSON.parse(String(options?.body)).input.length);
  };
  await assert.rejects(generatePendingEmbeddings(pending, "owner", collection));
  pending = storage.get<PendingWrites>("pendingWrites")!;
  assert.equal(pending.requests.filter((r) => r.embedded).length, 17);
  globalThis.fetch = async (_url, options) => {
    assert.deepEqual(JSON.parse(String(options?.body)).input, ["text 16"]);
    return modelResponse(1);
  };
  await generatePendingEmbeddings(pending, "owner", collection);
  assert.deepEqual(pending.requests.map((r) => JSON.parse(r.body).operation_id), ids);
  assert.ok(pending.requests.slice(0, 17).every((r) => r.body.includes("9223372036854775807")));
  assert.deepEqual(JSON.parse(pending.requests[17].body).document.vector, [8, 9]);
});

test("ingest accounts for partial receipts on error status, resends only unresolved originals and never sends model configuration", async () => {
  let pending = prepareEmbeddedWrites('[{"body":"a","vector":[1,0]},{"body":"b","vector":[1,0]},{"body":"c","vector":[1,0]}]', "owner", collection, config);
  const original = pending.requests.map((r) => r.body);
  globalThis.fetch = async (url, options) => {
    assert.equal(url, "/api/v1/index/docs/ingest");
    assert.equal((options?.headers as any).apikey, "tenant-secret");
    const body = String(options?.body);
    assert.ok(!body.includes(config.endpoint)); assert.ok(!body.includes(config.model));
    const value = await receipts(JSON.parse(body).records, ["committed", "outcome_unknown", "not_attempted"]).text();
    return new Response(value, { status: 503 });
  };
  await assert.rejects(sendWrites(pending, "tenant-secret", "owner"));
  pending = storage.get<PendingWrites>("pendingWrites")!;
  assert.equal(pending.completed, 1); assert.equal(pending.unresolved, true);
  globalThis.fetch = async (_url, options) => receipts(JSON.parse(String(options?.body)).records, ["rejected", "not_attempted"]);
  await assert.rejects(sendWrites(pending, "tenant-secret", "owner"));
  assert.equal(pending.requests[1].outcome, "unknown"); assert.equal(pending.rejected, false);
  globalThis.fetch = async (_url, options) => {
    assert.equal(options?.body, '{"records":[' + original.slice(1).join(",") + "]}");
    return receipts(JSON.parse(String(options?.body)).records, ["committed", "committed"]);
  };
  await sendWrites(pending, "tenant-secret", "owner");
  assert.equal(pending.completed, 3); assert.equal(storage.get("pendingWrites"), null);
});

test("lost ingest response survives reload; malformed identities stay unknown and storage failure prevents sending", async () => {
  let pending = prepareEmbeddedWrites('{"body":"a","vector":[1,0]}', "owner", collection, config);
  const original = pending.requests[0].body;
  globalThis.fetch = async () => { throw new Error("lost response"); };
  await assert.rejects(sendWrites(pending, "key", "owner"));
  pending = storage.get<PendingWrites>("pendingWrites")!;
  globalThis.fetch = async () => receipts([{ operation_id: { nonce: "wrong", epoch: 0 } }], ["committed"]);
  await assert.rejects(sendWrites(pending, "key", "owner"), /身份/);
  assert.equal(pending.requests[0].body, original); assert.equal(pending.unresolved, true);
  let calls = 0; globalThis.fetch = async () => { calls++; return new Response("{}"); };
  session.setItem = () => { throw new Error("quota"); };
  await assert.rejects(sendWrites(pending, "key", "owner"), /quota/);
  assert.equal(calls, 0);
});

test("large embedded imports split into bounded ingest batches and stop safely after a rejected item", async () => {
  let pending = prepareEmbeddedWrites("[" + Array.from({ length: 65 }, () => '{"body":"a","vector":[1,0]}').join(",") + "]", "owner", collection, config);
  const sizes: number[] = [];
  globalThis.fetch = async (_url, options) => {
    const records = JSON.parse(String(options?.body)).records;
    sizes.push(records.length);
    return receipts(records, records.map(() => "committed"));
  };
  await sendWrites(pending, "key", "owner");
  assert.deepEqual(sizes, [64, 1]);
  pending = prepareEmbeddedWrites('[{"body":"a","vector":[1,0]},{"body":"b","vector":[1,0]}]', "owner", collection, config);
  globalThis.fetch = async (_url, options) => receipts(JSON.parse(String(options?.body)).records, ["rejected", "committed"]);
  await assert.rejects(sendWrites(pending, "key", "owner"));
  assert.equal(pending.completed, 1); assert.equal(pending.rejected, true); assert.equal(pending.unresolved, false);
});

test("aborted embedding does not publish generated values or call ingest", async () => {
  saveEmbeddingConfig("owner", collection, config, "secret", false);
  const pending = prepareEmbeddedWrites('{"body":"a"}', "owner", collection, config);
  storage.set("pendingWrites", pending);
  const controller = new AbortController();
  globalThis.fetch = async (url) => {
    assert.equal(url, config.endpoint);
    controller.abort();
    return modelResponse(1);
  };
  await assert.rejects(generatePendingEmbeddings(pending, "owner", collection, () => {}, controller.signal));
  assert.equal(storage.get<PendingWrites>("pendingWrites")!.requests[0].embedded, false);
  await assert.rejects(sendWrites(pending, "key", "owner"), /先完成向量生成/);
});
