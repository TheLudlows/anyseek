// Exercise the real editor and transport scripts without browser/network dependencies.
const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

function editor(storage = new Map(), send = async () => ({ok:true, json:async () => ({})})) {
    let identity = 0;
    const chain = new Proxy({}, {get: () => () => chain});
    const context = vm.createContext({
        INDEX_ID:'collection', API_KEY:'test-key',
        crypto:{getRandomValues:bytes => bytes.fill(++identity)},
        sessionStorage:{getItem:key => storage.get(key) ?? null,
            setItem:(key,value) => storage.set(key,value), removeItem:key => storage.delete(key)},
        fetch:send, $:() => chain,
    });
    for (const file of ['setup.js','documents.js'])
        vm.runInContext(fs.readFileSync(path.join(__dirname,'../anyseek/web/js',file),'utf8'),context);
    return {run:code => vm.runInContext(code,context), context, storage};
}
const prepare = (e, source, envelope=false) => e.run(`prepareDocumentRequests(${JSON.stringify(source)},${envelope})`);
const pendingKey = 'seekpower.pendingWrites';

test('ordinary documents preserve i64, 2^53 boundaries, decimals and nested JSON text', () => {
    const e = editor();
    const numbers = ['-9223372036854775808','9223372036854775807','9007199254740991',
        '9007199254740992','9007199254740993','-9007199254740993','-0','1.0000000000000002','1e-300'];
    const raws = numbers.map(n => `{"rank":${n},"data":{"nested":[${n},true,false,null]},"string":"${n}"}`);
    const requests = prepare(e, '\uFEFF [ \n'+raws.join(' ,\n')+'\n ] ');
    assert.equal(requests.length,raws.length);
    requests.forEach((request,i) => assert.ok(request.body.endsWith('"document":'+raws[i]+'}')));
    assert.equal(new Set(requests.map(r => JSON.parse(r.body).operation_id.nonce)).size,raws.length);
});

test('array slicing respects escaped strings, braces, commas and number-like strings', () => {
    const e = editor();
    const raw = String.raw`{"value":9223372036854775807,"s":"[1,2], } \\\" 9007199254740993","nested":[{"a":[]},{}]}`;
    const requests = prepare(e, `[${raw}, {"s":"\\u0031e99","float":1.20e+3}]`);
    assert.equal(requests.length,2);
    assert.ok(requests[0].body.endsWith('"document":'+raw+'}'));
    assert.ok(requests[1].body.endsWith('"document":{"s":"\\u0031e99","float":1.20e+3}}'));
});

test('update and generation envelopes retain expected version and every original number token', () => {
    const e = editor();
    const raw = '{"record_id":"r","routing_key":"r","expected":{"Match":9007199254740993},"document":{"rank":-9223372036854775808,"json":{"n":9223372036854775807}},"generation":{"action":"stage","chunks":[{"n":9007199254740993}]}}';
    const [request] = prepare(e,raw,true);
    assert.ok(request.body.startsWith(raw.slice(0,-1)+',"operation_id":'));
    assert.equal(request.staging,true);
    assert.ok(JSON.parse(request.body).operation_id.nonce);
    // Out-of-range numbers are not coerced into valid values; server validation owns rejection.
    const [overflow] = prepare(e,'{"rank":9223372036854775808,"float":1e999}');
    assert.ok(overflow.body.endsWith('"document":{"rank":9223372036854775808,"float":1e999}}'));
});

test('invalid JSON, invalid document shapes and caller operation IDs are rejected', () => {
    const e = editor();
    for (const raw of ['','null','[]','[{},1]','[[]]','true','{"n":01}','{"n":NaN}','{},{}'])
        assert.throws(() => prepare(e,raw));
    assert.throws(() => prepare(e,'{"record_id":"r","routing_key":"r","expected":"Any","document":{},"operation_id":null}',true));
    assert.throws(() => prepare(e,'{"document":{}}',true));
});

test('partial send, storage reload and retry preserve exact bytes and operation identities', async () => {
    const storage = new Map(), sent = [];
    const e = editor(storage,async (_,options) => {
        sent.push(options.body);
        if(sent.length===2)throw new Error('connection lost');
        return {ok:true,json:async () => ({})};
    });
    e.context.requests = prepare(e,'[{"rank":9223372036854775807},{"rank":-9223372036854775808,"s":"9007199254740993"}]');
    e.run('pendingDocumentCommit={format:2,collection:INDEX_ID,unresolved:false,requests};');
    await e.run('submitPendingWrites()');
    const saved = JSON.parse(storage.get(pendingKey));
    assert.equal(saved.completed,1);
    assert.equal(saved.unresolved,true);
    assert.equal(saved.requests[1].body,sent[1]);
    const retried = [];
    const reloaded = editor(storage,async (_,options) => {
        retried.push(options.body);
        return {ok:true,json:async () => ({})};
    });
    reloaded.run('pendingDocumentCommit=loadPendingWrites();');
    await reloaded.run('submitPendingWrites()');
    assert.deepEqual(retried,[sent[1]]);
    assert.equal(storage.has(pendingKey),false);
});

test('older pending requests retain their already-sent content and identities', async () => {
    const old = {operation_id:{epoch:1,nonce:'retained'},record_id:'r',routing_key:'r',expected:'Any',document:{rank:42}};
    const storage = new Map([[pendingKey,JSON.stringify({collection:'collection',unresolved:true,requests:[old]})]]);
    const sent = [];
    const e = editor(storage,async (_,options) => {sent.push(options.body);return {ok:true,json:async () => ({})};});
    e.run('pendingDocumentCommit=loadPendingWrites();');
    await e.run('submitPendingWrites()');
    assert.deepEqual(sent,[JSON.stringify(old)]);
});

test('failure to persist retry state prevents sending a request', async () => {
    let sent = 0;
    const e = editor(new Map(),async () => {sent++;});
    e.context.requests = prepare(e,'{"rank":9223372036854775807}');
    e.run('pendingDocumentCommit={format:2,collection:INDEX_ID,requests};sessionStorage.setItem=()=>{throw new Error("storage unavailable");};');
    await e.run('submitPendingWrites()');
    assert.equal(sent,0);
});
