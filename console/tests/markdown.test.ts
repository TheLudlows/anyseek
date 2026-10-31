import { test } from "node:test";
import assert from "node:assert/strict";
import { chunkMarkdown, markdownDocuments } from "../src/markdown.ts";

test("Markdown splits sections and retains heading context and per-file numbering", () => {
  const chunks = chunkMarkdown("guide.md", "\uFEFF# Guide\r\n\r\nIntro\r\n\r\n## Install\r\n\r\nRun it\r\n\r\n### Linux\r\n\r\nDetails");
  assert.deepEqual(chunks.map(c => c.section), ["Guide", "Guide / Install", "Guide / Install / Linux"]);
  assert.deepEqual(chunks.map(c => c.index), [1, 2, 3]);
  assert.equal(chunks[2].title, "Guide");
  assert.match(chunks[2].content, /Details/);
  assert.equal(chunkMarkdown("other.md", "Hello")[0].index, 1);
});

test("fences, lists and tables are not truncated even above the target", () => {
  const code = '```md\n# Not a heading\n\n' + 'x'.repeat(80) + '\n```';
  const table = '| A | B |\n|---|---|\n| 1 | 2 |';
  const list = '- first\n- second\n- third';
  const chunks = chunkMarkdown("a.md", `# Title\n\n${code}\n\n${table}\n\n${list}`, 20);
  for (const block of [code, table, list]) assert.ok(chunks.some(c => c.content.includes(block)));
  assert.ok(chunks.some(c => c.oversized));
  assert.ok(chunks.every(c => c.section === "Title"));
  assert.ok(chunkMarkdown("a.md", '~~~\n# code\n\nunfinished', 5)[0].content.includes('# code\n\nunfinished'));
});

test("paragraph splitting preserves all text and empty documents yield no records", () => {
  const paragraphs = ['a'.repeat(15), 'b'.repeat(15), 'c'.repeat(15)];
  assert.deepEqual(chunkMarkdown("a.md", paragraphs.join('\n\n'), 20).map(c => c.content), paragraphs);
  assert.deepEqual(chunkMarkdown("empty.md", ' \n\n'), []);
  assert.equal(chunkMarkdown("a.md", '# Only heading')[0].content, '# Only heading');
});

test("mapping uses selected fields, carries context and rejects collisions", () => {
  const chunks = chunkMarkdown("guide.md", '# Guide\n\n## Install\n\nDo this');
  const mapping = { body: 'content', title: 'title', source: 'filename', index: 'part' };
  const docs = markdownDocuments(chunks, mapping);
  assert.equal(docs[1].filename, 'guide.md');
  assert.equal(docs[1].part, 2);
  assert.match(String(docs[1].content), /Guide \/ Install/);
  assert.match(String(docs[1].content), /Do this/);
  assert.throws(() => markdownDocuments(chunks, { ...mapping, title: 'content' }), /不同/);
  assert.throws(() => markdownDocuments(chunks, { ...mapping, body: '' }), /正文/);
  assert.throws(() => markdownDocuments([], mapping), /没有/);
  const minimal = markdownDocuments(chunks, { body: '__proto__', title: '', source: '', index: '' });
  assert.deepEqual(Object.keys(minimal[0]), ['__proto__']);
  assert.ok(JSON.stringify(minimal).includes('__proto__'));
});
