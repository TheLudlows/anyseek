import { beforeEach, test } from "node:test";
import assert from "node:assert/strict";
import {
  prepareDocumentRequests,
  sendWrites,
  type PendingWrites,
} from "../src/writes.ts";
import {
  apiFieldText,
  apiJsonText,
  parseApiJson,
  prettyJson,
} from "../src/json.ts";
import { ApiError, storage } from "../src/api.ts";

let data: Map<string, string>;
beforeEach(() => {
  data = new Map();
  Object.defineProperty(globalThis, "sessionStorage", {
    configurable: true,
    value: {
      getItem: (k: string) => data.get(k) ?? null,
      setItem: (k: string, v: string) => {
        data.set(k, v);
      },
      removeItem: (k: string) => {
        data.delete(k);
      },
    },
  });
});
const pending = (
  source = '[{"rank":9223372036854775807},{"rank":-9223372036854775808}]',
): PendingWrites => ({
  owner: "owner",
  collection: "precision",
  requests: prepareDocumentRequests(source),
  completed: 0,
  unresolved: false,
});

test("document requests preserve i64 boundaries, nested numbers, exponents and escaping", () => {
  const numbers = [
    "-9223372036854775808",
    "9223372036854775807",
    "9007199254740991",
    "9007199254740992",
    "9007199254740993",
    "-9007199254740993",
    "-0",
    "1.2500e0",
    "1e-300",
    "1e999",
  ];
  const raws = numbers.map(
    (n) =>
      `{"rank":${n},"data":{"nested":[${n},true,false,null]},"string":"${n}","s":"[1,2], } \\\""}`,
  );
  const requests = prepareDocumentRequests(
    "\uFEFF [\n" + raws.join(" ,\n") + "\n ]",
  );
  requests.forEach((r, i) =>
    assert.ok(r.body.endsWith('"document":' + raws[i] + "}")),
  );
  assert.equal(
    new Set(requests.map((r) => JSON.parse(r.body).operation_id.nonce)).size,
    raws.length,
  );
});

test("envelopes retain raw expected version and document; IDs remain editor-owned", () => {
  const source =
    '{"record_id":"r","routing_key":"r","expected":{"Match":9007199254740993},"document":{"rank":-9223372036854775808}}';
  const [request] = prepareDocumentRequests(source, true);
  assert.ok(request.body.startsWith(source.slice(0, -1) + ',"operation_id":'));
  for (const raw of [
    "",
    "null",
    "[]",
    "[{},1]",
    "[[]]",
    "true",
    '{"n":01}',
    '{"n":NaN}',
    "{},{}",
  ])
    assert.throws(() => prepareDocumentRequests(raw));
  assert.throws(() =>
    prepareDocumentRequests(
      '{"record_id":"r","routing_key":"r","expected":"Any","document":{},"operation_id":null}',
      true,
    ),
  );
});

test("response JSON and individual fields retain exact numeric source tokens", () => {
  const raw =
    '{"visible_seq":9007199254740993,"next":{"visible_seq":9007199254740993,"after":"r"},"record":{"document":{"rank":9223372036854775807,"measure":1.2500e0,"data":{"n":-9223372036854775808}}}}';
  const value = parseApiJson(raw);
  assert.equal(apiJsonText(value), raw);
  const displayed = prettyJson(raw);
  assert.ok(displayed.includes("9007199254740993"));
  assert.ok(displayed.includes("1.2500e0"));
  assert.ok(displayed.includes("-9223372036854775808"));
  assert.deepEqual(JSON.parse(displayed), value);
  assert.equal(
    apiJsonText(value.next),
    '{"visible_seq":9007199254740993,"after":"r"}',
  );
  assert.equal(apiFieldText(value, "visible_seq"), "9007199254740993");
  assert.equal(
    apiFieldText(value.record.document, "rank"),
    "9223372036854775807",
  );
  assert.equal(apiFieldText(value.record.document, "measure"), "1.2500e0");
  assert.equal(
    apiJsonText(value.record.document),
    '{"rank":9223372036854775807,"measure":1.2500e0,"data":{"n":-9223372036854775808}}',
  );
});

test("partial transport failure, reload and retry reuse byte-identical original requests", async () => {
  const request = pending(),
    sent: string[] = [];
  globalThis.fetch = async (_url, options) => {
    sent.push(String(options?.body));
    if (sent.length === 2) throw new Error("connection lost");
    return new Response("{}");
  };
  await assert.rejects(sendWrites(request, "key", "owner"));
  let saved = storage.get<PendingWrites>("pendingWrites")!;
  assert.equal(saved.completed, 1);
  assert.equal(saved.unresolved, true);
  const original = saved.requests[1].body;
  // Admission rejection of a previously unresolved request never proves failure.
  for (const [status, code] of [
    [400, "rejected"],
    [409, "conflict"],
    [429, "busy"],
  ] as const) {
    globalThis.fetch = async () =>
      new Response(JSON.stringify({ code, message: "retry " + code }), {
        status,
      });
    await assert.rejects(sendWrites(saved, "key", "owner"), ApiError);
    saved = storage.get<PendingWrites>("pendingWrites")!;
    assert.equal(saved.unresolved, true);
    assert.equal(saved.rejected, false);
    assert.equal(saved.requests[1].body, original);
  }
  globalThis.fetch = async (_url, options) => {
    sent.push(String(options?.body));
    return new Response("{}");
  };
  await sendWrites(saved, "key", "owner");
  assert.equal(sent[1], sent[2]);
  assert.equal(storage.get("pendingWrites"), null);
});

test("first known rejection is discardable; unsent records do not dispatch", async () => {
  const request = pending();
  let calls = 0;
  globalThis.fetch = async () => {
    calls++;
    return new Response('{"code":"rejected","message":"invalid"}', {
      status: 400,
    });
  };
  await assert.rejects(sendWrites(request, "key", "owner"));
  assert.equal(calls, 1);
  assert.equal(request.rejected, true);
  assert.equal(request.unresolved, false);
});

test("failure to persist the identity prevents dispatch; other connections cannot replay it", async () => {
  let calls = 0;
  globalThis.fetch = async () => {
    calls++;
    return new Response("{}");
  };
  await assert.rejects(sendWrites(pending(), "other-key", "other-owner"));
  const old = sessionStorage.setItem;
  sessionStorage.setItem = () => {
    throw new Error("storage full");
  };
  await assert.rejects(sendWrites(pending(), "key", "owner"), /storage full/);
  sessionStorage.setItem = old;
  assert.equal(calls, 0);
});
