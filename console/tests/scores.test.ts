import { test } from "node:test";
import assert from "node:assert/strict";
import { scoreRows, scoreText } from "../src/scores.ts";
import type { Hit } from "../src/types.ts";
const hit: Hit = { record: { record_id: "a", routing_key: "a", version: 1, document: {} }, score: 0.001,
  channels: [{ channel: "lexical", rank: 0, rank_basis: "score", raw_score: 0, rrf_contribution: 0.001 }] };
test("hybrid missing channels remain absent rather than zero scores", () => {
  const rows = scoreRows(hit, "Hybrid");
  assert.equal(rows[0].value?.raw_score, 0);
  assert.equal(rows[1].channel, "vector");
  assert.equal(rows[1].value, undefined);
  assert.equal(scoreRows(hit, "Lexical").length, 1);
  assert.deepEqual(scoreRows({ ...hit, channels: undefined }, "Hybrid"), []);
});
test("small contributions, negatives and missing scores remain distinguishable", () => {
  assert.equal(scoreText(0), "0");
  assert.equal(scoreText(null), "—");
  assert.notEqual(scoreText(0.000000123), "0");
  assert.equal(scoreText(-0.75), "-0.75");
});
