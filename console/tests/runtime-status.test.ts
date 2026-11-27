import { test } from 'node:test';
import assert from 'node:assert/strict';
import { parseApiJson } from '../src/json.ts';
import { counter, indexState } from '../src/runtime-status.ts';

test('index progress uses exact sequences and requires explicit health fields', () => {
  const progress = parseApiJson('{"committed_seq":9007199254740993,"lexical_seq":9007199254740992,"vector_seq":9007199254740993,"index_error":null,"recovery_required":false}');
  assert.equal(indexState(progress), 'pending');
  assert.equal(counter(progress, 'committed_seq'), 9007199254740993n);
  assert.equal(indexState({...progress, lexical_seq:9007199254740994}), 'caught_up');
  assert.equal(indexState({...progress, index_error:'failed'}), 'error');
  assert.equal(indexState({...progress, recovery_required:true, committed_seq:null}), 'recovery');
  for (const value of [null, {}, {committed_seq:0, lexical_seq:0, vector_seq:0}, {...progress, committed_seq:null}, {...progress, vector_seq:'0'}])
    assert.equal(indexState(value), 'unknown');
});
