import { beforeEach, test } from "node:test";
import assert from "node:assert/strict";
import { deleteCollection } from "../src/collection-delete.ts";
import { storage } from "../src/api.ts";
let session: Map<string, string>;
beforeEach(() => {
  session = new Map();
  for (const [name, data] of [["sessionStorage", session], ["localStorage", new Map<string, string>()]] as const) {
    Object.defineProperty(globalThis, name, { configurable: true, value: {
      getItem: (key: string) => data.get(key) ?? null,
      setItem: (key: string, value: string) => data.set(key, value),
      removeItem: (key: string) => data.delete(key),
      key: (index: number) => [...data.keys()][index] ?? null,
      get length() { return data.size; },
    } });
  }
});
test("lost deletion response retries identical operation after reload", async () => {
  let original = "";
  storage.set("collection", "docs");
  globalThis.fetch = async (url, options) => {
    assert.equal(url, "/api/v1/index/docs"); assert.equal(options?.method, "DELETE");
    original = String(options?.body);
    assert.ok(storage.get("pendingDelete"));
    throw new Error("offline");
  };
  await assert.rejects(deleteCollection("owner", "key", "docs"));
  globalThis.fetch = async (_url, options) => {
    assert.equal(options?.body, original);
    return new Response('{"deleted":true}');
  };
  await deleteCollection("owner", "key", "docs");
  assert.equal(storage.get("pendingDelete"), null);
  assert.equal(storage.get("collection"), null);
});
test("foreign tasks and outstanding writes prevent new deletion", async () => {
  globalThis.fetch = async () => { throw new Error("unexpected request"); };
  storage.set("pendingDelete", { owner: "other", collection: "docs", body: "{}" });
  await assert.rejects(deleteCollection("owner", "key", "docs"), /原连接/);
  storage.remove("pendingDelete");
  storage.set("pendingWrites", { owner: "owner", collection: "docs" });
  await assert.rejects(deleteCollection("owner", "key", "docs"), /待确认写入/);
  assert.equal(storage.get("pendingDelete"), null);
});
test("missing confirmation keeps the original request; persistence failure prevents dispatch", async () => {
  globalThis.fetch = async () => new Response('{}');
  await assert.rejects(deleteCollection("owner", "key", "docs"), /未收到/);
  assert.ok(storage.get("pendingDelete"));
  storage.remove("pendingDelete");
  sessionStorage.setItem = () => { throw new Error("quota"); };
  let calls = 0;
  globalThis.fetch = async () => { calls++; return new Response('{"deleted":true}'); };
  await assert.rejects(deleteCollection("owner", "key", "docs"), /quota/);
  assert.equal(calls, 0);
});
