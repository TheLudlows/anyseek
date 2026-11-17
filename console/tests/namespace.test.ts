import { test } from "node:test";
import assert from "node:assert/strict";
import { api, ownerFor } from "../src/api.ts";

test("browser requests pass namespace and gateway session without service credentials", async () => {
  let sent: RequestInit | undefined;
  let url: unknown;
  const previous = globalThis.fetch;
  globalThis.fetch = async (input, init) => {
    url = input;
    sent = init;
    return new Response("[]", { headers: { "Content-Type": "application/json" } });
  };
  try {
    await api("workspace_a", "index");
    assert.equal(url, "/api/v1/index");
    assert.equal(new Headers(sent?.headers).get("X-AnySeek-Tenant"), "workspace_a");
    assert.equal(new Headers(sent?.headers).has("apikey"), false);
    assert.equal(sent?.credentials, "same-origin");
  } finally { globalThis.fetch = previous; }
});

test("pending operation ownership differs between tenant namespaces", async () => {
  assert.equal(await ownerFor("workspace_a"), await ownerFor("workspace_a"));
  assert.notEqual(await ownerFor("workspace_a"), await ownerFor("workspace_b"));
});
