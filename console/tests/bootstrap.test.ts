import { test } from "node:test";
import assert from "node:assert/strict";
import {
  prepareBootstrap,
  runBootstrap,
  loadBootstrap,
} from "../src/bootstrap.ts";
const values = new Map<string, string>();
Object.defineProperty(globalThis, "sessionStorage", {
  value: {
    getItem: (k: string) => values.get(k) ?? null,
    setItem: (k: string, v: string) => values.set(k, v),
  },
});
test("lost key response survives reload and replays byte-identical key without master persistence", async () => {
  values.clear();
  const request = prepareBootstrap("first_tenant");
  assert.equal(Buffer.from(request.key, "base64").length, 32);
  const bodies: string[] = [];
  globalThis.fetch = async (_url, init) => {
    bodies.push(init!.body as string);
    if (bodies.length === 2) throw new Error("lost response");
    return new Response('{"seq":1}');
  };
  await assert.rejects(runBootstrap(request, "test-master"));
  const restored = loadBootstrap()!;
  assert.equal(restored.stage, "key");
  assert.equal(await runBootstrap(restored, "test-master"), request.key);
  assert.equal(bodies[1], bodies[2]);
  assert.equal(JSON.stringify([...values]).includes("test-master"), false);
});
test("lost tenant response replays same tenant and key identities", async () => {
  values.clear();
  const request = prepareBootstrap("first_tenant");
  const bodies: string[] = [];
  globalThis.fetch = async (_url, init) => {
    bodies.push(init!.body as string);
    if (bodies.length === 1) throw new Error("lost response");
    return new Response('{"seq":1}');
  };
  await assert.rejects(runBootstrap(request, "test-master"));
  await runBootstrap(loadBootstrap()!, "test-master");
  assert.equal(bodies[0], bodies[1]);
  assert.equal(bodies[2], request.keyRequest);
});
test("storage failure prevents dispatch and invalid tenant is rejected", async () => {
  assert.throws(() => prepareBootstrap("Wrong Name"));
  let sent = false;
  globalThis.fetch = async () => {
    sent = true;
    return new Response("{}");
  };
  const original = sessionStorage.setItem;
  sessionStorage.setItem = () => {
    throw new Error("storage disabled");
  };
  await assert.rejects(runBootstrap(prepareBootstrap("valid"), "test-master"));
  assert.equal(sent, false);
  sessionStorage.setItem = original;
});
