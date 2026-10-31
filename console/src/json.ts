// Retain original response spans for display, copy and cursor replay.
// Parsed numbers are for UI logic only; never reserialize authoritative data.
const apiJsonSources = new WeakMap<
  object,
  {
    source: string;
    start: number;
    end: number;
    fields: Map<string, [number, number]>;
  }
>();
export function parseApiJson(source: string): any {
  const value = JSON.parse(source);
  let offset = 0;
  const space = () => {
    while (/\s/.test(source[offset] || "") && offset < source.length) offset++;
  };
  const stringEnd = () => {
    offset++;
    while (offset < source.length) {
      const ch = source[offset++];
      if (ch === "\\") offset++;
      else if (ch === '"') break;
    }
  };
  function scan(node: any): [number, number] {
    space();
    const start = offset,
      ch = source[offset];
    const fields = new Map<string, [number, number]>();
    if (ch === "{" || ch === "[") {
      const object = ch === "{",
        close = object ? "}" : "]";
      offset++;
      space();
      let index = 0;
      while (source[offset] !== close) {
        let key = String(index++);
        if (object) {
          const keyStart = offset;
          stringEnd();
          key = JSON.parse(source.slice(keyStart, offset));
          space();
          offset++;
          space();
        }
        const child = node && Object.hasOwn(node, key) ? node[key] : undefined;
        const span = scan(child);
        fields.set(key, span);
        space();
        if (source[offset] !== ",") break;
        offset++;
        space();
      }
      offset++;
    } else if (ch === '"') stringEnd();
    else
      while (offset < source.length && !/[\s,}\]]/.test(source[offset]))
        offset++;
    if (node !== null && typeof node === "object")
      apiJsonSources.set(node, { source, start, end: offset, fields });
    return [start, offset];
  }
  scan(value);
  return value;
}
export function apiJsonText(value: any): string {
  const saved =
    value !== null && typeof value === "object"
      ? apiJsonSources.get(value)
      : undefined;
  return saved
    ? saved.source.slice(saved.start, saved.end)
    : (JSON.stringify(value, null, 2) ?? "null");
}
export function apiFieldText(parent: any, key: string): string {
  const value = parent[key];
  if (value == null) return "";
  if (typeof value === "string") return value;
  const saved = apiJsonSources.get(parent),
    span = saved?.fields.get(String(key));
  return span && saved
    ? saved.source.slice(...span)
    : typeof value === "object"
      ? apiJsonText(value)
      : String(value);
}

// Format presentation only; quoted strings and number tokens remain untouched.
export function prettyJson(source: string): string {
  try {
    JSON.parse(source);
  } catch {
    return source;
  }
  const tokens =
    source.match(/"(?:\\.|[^"\\])*"|[{}\[\],:]|[^\s{}\[\],:]+/g) || [];
  let level = 0,
    result = "";
  const newline = () => {
    result += "\n" + "  ".repeat(level);
  };
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i];
    if (token === "{" || token === "[") {
      result += token;
      if (tokens[i + 1] === (token === "{" ? "}" : "]")) result += tokens[++i];
      else {
        level++;
        newline();
      }
    } else if (token === "}" || token === "]") {
      level--;
      newline();
      result += token;
    } else if (token === ",") {
      result += ",";
      newline();
    } else if (token === ":") result += ": ";
    else result += token;
  }
  return result;
}
