export type MarkdownChunk = { source: string; title: string; section: string; content: string; index: number; oversized: boolean };

// Conservative Markdown splitting: fenced code and contiguous lists/tables stay intact.
// The target is a soft character limit, not a model token limit.
export function chunkMarkdown(source: string, input: string, target = 4000): MarkdownChunk[] {
  const lines = input.replace(/^\uFEFF/, "").replace(/\r\n?/g, "\n").split("\n");
  const chunks: MarkdownChunk[] = [];
  const headings: { level: number; text: string }[] = [];
  let title = source.replace(/\.(md|markdown)$/i, "");
  let foundTitle = false;
  let fence = "", fenceLength = 0, block: string[] = [], parts: string[] = [];
  function flushChunk() {
    const content = parts.join("\n\n").trim();
    if (content) chunks.push({ source, title, section: headings.map(h => h.text).join(" / "), content, index: chunks.length + 1, oversized: content.length > target });
    parts = [];
  }
  function flushBlock() {
    const text = block.join("\n").trim();
    block = [];
    if (!text) return;
    if (parts.length && parts.join("\n\n").length + text.length + 2 > target) flushChunk();
    parts.push(text);
  }
  for (const line of lines) {
    const marker = line.match(/^ {0,3}(`{3,}|~{3,})(.*)$/);
    if (fence) {
      block.push(line);
      if (marker && marker[1][0] === fence && marker[1].length >= fenceLength && !marker[2].trim()) {
        fence = "";
        flushBlock();
      }
      continue;
    }
    if (marker) {
      flushBlock(); fence = marker[1][0]; fenceLength = marker[1].length; block.push(line); continue;
    }
    const heading = line.match(/^ {0,3}(#{1,6})\s+(.+?)\s*#*\s*$/);
    if (heading) {
      flushBlock(); flushChunk();
      const level = heading[1].length;
      while (headings.length && headings[headings.length - 1].level >= level) headings.pop();
      headings.push({ level, text: heading[2] });
      if (level === 1 && !foundTitle) { title = heading[2]; foundTitle = true; }
      // Preserve heading-only sections as well as the original heading syntax.
      block.push(line);
    } else if (!line.trim()) flushBlock();
    else block.push(line);
  }
  flushBlock(); flushChunk();
  return chunks;
}

export type MarkdownMapping = { body: string; title: string; source: string; index: string };
export function markdownDocuments(chunks: MarkdownChunk[], mapping: MarkdownMapping) {
  const fields = Object.values(mapping).filter(Boolean);
  if (!mapping.body) throw new Error("请选择正文目标字段。");
  if (new Set(fields).size !== fields.length) throw new Error("每种内容需要映射到不同的字段。");
  if (!chunks.length) throw new Error("所选文档没有可导入的内容。");
  return chunks.map(chunk => {
    const context = [chunk.title, chunk.section].filter((v, i, all) => v && all.indexOf(v) === i).join("\n");
    const document: Record<string, string | number> = Object.create(null);
    document[mapping.body] = context + "\n\n" + chunk.content;
    if (mapping.title) document[mapping.title] = chunk.title;
    if (mapping.source) document[mapping.source] = chunk.source;
    if (mapping.index) document[mapping.index] = chunk.index;
    return document;
  });
}
