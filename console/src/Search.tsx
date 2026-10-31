import { t, getLocale } from "./i18n.ts";
import { useEffect, useRef, useState } from "react";
import {
  ArrowLeft,
  ArrowRight,
  ArrowUpRight,
  ChevronDown,
  Search as SearchIcon,
  SlidersHorizontal,
  Sparkles,
} from "lucide-react";
import { ScoreDetails } from "./ScoreDetails.tsx";
import { channelName, scoreRows, scoreText } from "./scores.ts";
import { api, collectionPath, message } from "./api.ts";
import { embedTexts, embeddingKey, loadEmbeddingConfig, validateEmbeddingConfig } from "./embeddings.ts";
import { apiFieldText } from "./json.ts";
import {
  Busy,
  JsonView,
  Empty,
  Notice,
  RecordPreview,
  recordBody,
  recordTitle,
} from "./components.tsx";
import type { Collection, Hit, Json, Mode, QueryResult } from "./types.ts";

export function Search({
  collection,
  apiKey,
  owner,
}: {
  collection: Collection;
  apiKey: string;
  owner: string;
}) {
  const [modelConfig] = useState(() => loadEmbeddingConfig(owner, collection.id));
  const [autoEmbed, setAutoEmbed] = useState(!!modelConfig);
  const [preview, setPreview] = useState<{ text: string; vector: number[] } | null>(null);
  const [resultMode, setResultMode] = useState<Mode>("Lexical");
  const submittedMode = useRef<Mode>("Lexical");
  const [phase, setPhase] = useState("");
  const [mode, setMode] = useState<Mode>(
      collection.schema.some((f) => f.index_vector) &&
        !collection.schema.some(
          (f) => f.field_type === "Text" && f.index_lexical !== false,
        )
        ? "Vector"
        : "Lexical",
    ),
    [query, setQuery] = useState(""),
    [vector, setVector] = useState("");
  const [filters, setFilters] = useState(false),
    [from, setFrom] = useState(""),
    [until, setUntil] = useState(""),
    [valid, setValid] = useState("");
  const [dateFrom, setDateFrom] = useState(""),
    [dateUntil, setDateUntil] = useState(""),
    [sort, setSort] = useState("score");
  const [advanced, setAdvanced] = useState("{}"),
    [page, setPage] = useState(0),
    [result, setResult] = useState<QueryResult | null>(null);
  const [busy, setBusy] = useState(false),
    [error, setError] = useState(""),
    [selected, setSelected] = useState<Hit | null>(null);
  const request = useRef<AbortController | null>(null),
    submitted = useRef<string | null>(null);
  const dateField = collection.schema.find(
    (f) => f.field_type === "Timestamp" && f.filterable,
  )?.field;
  const hasText = collection.schema.some(
    (f) => f.field_type === "Text" && f.index_lexical !== false,
  );
  const hasVector = collection.schema.some((f) => f.index_vector);
  useEffect(() => () => request.current?.abort(), []);
  const currentPreview = preview?.text === query && preview.vector.length === collection.vector_dimensions ? preview : null;
  async function queryEmbedding(signal: AbortSignal) {
    if (!query.trim()) throw new Error(t("请输入检索内容。"));
    if (!modelConfig) throw new Error(t("请先在连接与管理中配置浏览器嵌入服务。"));
    validateEmbeddingConfig(modelConfig, collection);
    if (currentPreview) return currentPreview.vector;
    setPhase(t("正在生成查询向量"));
    const [embedding] = await embedTexts(modelConfig, embeddingKey(owner, collection.id, modelConfig), [query], collection.vector_dimensions!, signal);
    signal.throwIfAborted();
    setPreview({ text: query, vector: embedding });
    return embedding;
  }
  async function previewVector() {
    if (busy) return;
    request.current?.abort();
    const controller = new AbortController();
    request.current = controller;
    setError(""); setBusy(true); setPhase(t("正在生成查询向量"));
    try { await queryEmbedding(controller.signal); }
    catch (e) { if (!controller.signal.aborted) setError(message(e)); }
    finally { if (!controller.signal.aborted) setBusy(false); }
  }
  async function build(signal: AbortSignal) {
    const body: Json = JSON.parse(advanced);
    if (!body || typeof body !== "object" || Array.isArray(body))
      throw new Error(t("高级参数必须是 JSON 对象。"));
    if ((mode === "Hybrid" || (mode === "Vector" && autoEmbed)) && !query.trim()) throw new Error(t("请输入检索内容。"));
    delete body.query;
    delete body.query_vector;
    if (mode !== "Vector") body.query = query;
    if (mode !== "Lexical" && !autoEmbed) {
      const parsed = JSON.parse(vector);
      if (
        !Array.isArray(parsed) ||
        !parsed.length ||
        !parsed.every((n) => typeof n === "number" && Number.isFinite(n))
      )
        throw new Error(t("请输入非空的有限数值向量数组。"));
      body.query_vector = parsed;
    }
    const ms = (value: string) => {
      if (!value.trim()) return undefined;
      const n = Number(value);
      if (!Number.isSafeInteger(n))
        throw new Error(t("时间筛选须为整数 UTC 毫秒。"));
      return n;
    };
    const start = ms(from),
      end = ms(until),
      at = ms(valid);
    if (start !== undefined && end !== undefined && start >= end)
      throw new Error(t("事件结束时间必须晚于开始时间。"));
    if (start !== undefined || end !== undefined || at !== undefined)
      body.temporal = {
        event:
          start !== undefined || end !== undefined
            ? { from_ms: start, to_ms: end }
            : undefined,
        valid_at_ms: at,
      };
    if (dateField && (dateFrom || dateUntil)) {
      const a = dateFrom
        ? Date.parse(dateFrom + "T00:00:00Z") / 1000
        : Number.MIN_SAFE_INTEGER;
      const b = dateUntil
        ? Date.parse(dateUntil + "T00:00:00Z") / 1000 + 86400
        : Number.MAX_SAFE_INTEGER;
      if (!Number.isFinite(a) || !Number.isFinite(b) || a >= b)
        throw new Error(t("请选择有效日期范围。"));
      body.filter = [
        ...(body.filter || []),
        { Timestamp: { field: dateField, filter: { start: a, end: b } } },
      ];
    }
    if (dateField && sort !== "score")
      body.sort = [
        {
          field: dateField,
          order: sort === "new" ? "Descending" : "Ascending",
        },
      ];
    body.length = 10;
    delete body.offset;
    body.count = true;
    // Advanced filters may include i64 tokens. Do not round them via stringify.
    // Keep the original JSON members and append the explicitly controlled fields.
    const original = advanced.trim();
    const controlled = new Set([
      "query",
      "query_vector",
      "offset",
      "length",
      "count",
      "temporal",
      "filter",
      "sort",
    ]);
    if (mode !== "Lexical" && autoEmbed) {
      body.query_vector = await queryEmbedding(signal);
    }
    // Source-preserving JSON rewrite for advanced parameters is provided below.
    return mergeQuerySource(original, body, controlled);
  }
  async function run(next = 0, fresh = true) {
    request.current?.abort();
    const controller = new AbortController();
    request.current = controller;
    setError("");
    setSelected(null);
    setBusy(true);
    setResult(null);
    setPhase(t("正在检索集合"));
    try {
      const source = fresh ? await build(controller.signal) : submitted.current;
      controller.signal.throwIfAborted();
      if (!source) return;
      if (fresh) { submitted.current = source; submittedMode.current = mode; }
      // offset is appended last and controlled by pagination.
      const body = source.slice(0, -1) + ',"offset":' + next * 10 + "}";
      setPhase(t("正在检索集合"));
      const response = await api<QueryResult>(
        apiKey,
        collectionPath(collection.id) + "/query",
        body,
        "POST",
        controller.signal,
      );
      if (!controller.signal.aborted) {
        setResult(response);
        setResultMode(submittedMode.current);
        setPage(next);
      }
    } catch (e) {
      if (!controller.signal.aborted) setError(message(e));
    } finally {
      if (!controller.signal.aborted) setBusy(false);
    }
  }
  const total = result?.count_total;
  return (
    <>
      <div className="page-heading">
        <div>

          <h1>{t("检索调试")}</h1>
          <p>{t("探索数据中的关联，找到更准确的结果。")}</p>
        </div>
        <span className="context-pill">
          <span className="status-dot" />
          {collection.name}
        </span>
      </div>
      <section className="panel query-panel">
        <div className="query-top">
          <div className="segmented" aria-label={t("检索模式")}>
            {(["Lexical", "Vector", "Hybrid"] as Mode[]).map((m) => (
              <button
                key={m}
                className={mode === m ? "active" : ""}
                disabled={
                  busy ||
                  (!hasVector && m !== "Lexical") ||
                  (!hasText && m !== "Vector")
                }
                onClick={() => setMode(m)}
              >
                {m === "Hybrid" && <Sparkles size={13} />}
                {{ Lexical: t("关键词"), Vector: t("向量"), Hybrid: t("混合") }[m]}
              </button>
            ))}
          </div>
          <button
            className={"text-button filter-toggle " + (filters ? "active" : "")}
            onClick={() => setFilters(!filters)}
            aria-expanded={filters}
          >
            <SlidersHorizontal size={15} />
            {t("筛选与参数")}<ChevronDown size={13} />
          </button>
        </div>
        <form
          onSubmit={(e) => {
            e.preventDefault();
            run();
          }}
        >
          <div className="search-entry">
            <SearchIcon size={21} />
            <input
              aria-label={t("检索内容")}
              value={query}
              disabled={busy || (mode === "Vector" && !autoEmbed)}
              onChange={(e) => { setQuery(e.target.value); setPreview(null); }}
              placeholder={
                mode === "Vector" && !autoEmbed
                  ? t("向量模式使用下方 query vector")
                  : mode === "Lexical"
                    ? t("输入关键词；留空可浏览记录…")
                    : t("输入关键词，探索你的数据…")
              }
            />
            <button className="primary" disabled={busy}>
              {busy ? (
                <Busy label={t("检索中")} />
              ) : (
                <>
                  {t("运行检索")}<ArrowRight size={15} />
                </>
              )}
            </button>
          </div>
          {mode !== "Lexical" && <div className="embedding-choice">
            <label className="checkbox"><input type="checkbox" checked={autoEmbed} disabled={busy || !modelConfig} onChange={(e) => setAutoEmbed(e.target.checked)} />{t("自动生成查询向量")}</label>
            <p className="field-help">{modelConfig ? t("{0} · 查询文本将发送至 {1}", modelConfig.model, modelConfig.endpoint) : t("在连接与管理中配置本集合的嵌入服务，或在下方粘贴查询向量。")}</p>
          </div>}
          {mode !== "Lexical" && autoEmbed && <div className="vector-entry">
            <button type="button" className="secondary" disabled={busy || !modelConfig || !query.trim()} onClick={previewVector}>
              {t("生成并预览向量")}</button>
            <p className="field-help">{t("仅调用嵌入模型，不执行检索。运行检索将复用预览向量；修改文本后需重新生成。")}</p>
            {currentPreview && <details open>
              <summary>{t("当前查询向量 · {0} 维", currentPreview.vector.length)}</summary>
              <JsonView value={currentPreview.vector} label={t("查询向量预览")} />
            </details>}
          </div>}
          {mode !== "Lexical" && !autoEmbed && (
            <div className="vector-entry">
              <label>
                {t("查询向量")}
                <textarea
                  aria-label={t("查询向量")}
                  className="code-input"
                  value={vector}
                  onChange={(e) => setVector(e.target.value)}
                  placeholder="[0.12, -0.08, 0.31, …]"
                  rows={3}
                />
              </label>
              <p className="field-help">
                {t("使用与集合相同模型生成的向量")}{collection.embedding_space_id &&
                  `（${collection.embedding_space_id}）`}
                。
              </p>
            </div>
          )}
          {filters && (
            <div className="query-filters">
              {dateField && (
                <>
                  <div className="form-row">
                    <label>
                      {dateField} {t("· 起始日期（UTC）")}<input
                        type="date"
                        value={dateFrom}
                        onChange={(e) => setDateFrom(e.target.value)}
                      />
                    </label>
                    <label>
                      {t("截至日期（含当天）")}<input
                        type="date"
                        value={dateUntil}
                        onChange={(e) => setDateUntil(e.target.value)}
                      />
                    </label>
                    <label>
                      {t("排序")}<select
                        value={sort}
                        onChange={(e) => setSort(e.target.value)}
                      >
                        <option value="score">{t("相关度")}</option>
                        <option value="new">{t("最新优先")}</option>
                        <option value="old">{t("最早优先")}</option>
                      </select>
                    </label>
                  </div>
                </>
              )}
              <div className="form-row">
                <label>
                  {t("事件起始 · UTC 毫秒")}<input
                    value={from}
                    onChange={(e) => setFrom(e.target.value)}
                    placeholder={t("可选")}
                  />
                </label>
                <label>
                  {t("事件结束 · UTC 毫秒")}<input
                    value={until}
                    onChange={(e) => setUntil(e.target.value)}
                    placeholder={t("不含此时刻")}
                  />
                </label>
                <label>
                  {t("有效时刻 · UTC 毫秒")}<input
                    value={valid}
                    onChange={(e) => setValid(e.target.value)}
                    placeholder={t("可选")}
                  />
                </label>
              </div>
              <details>
                <summary>{t("高级查询参数 · JSON")}</summary>
                <p className="field-help">
                  {t("可配置 RRF、字段、过滤条件等。查询文本、向量及分页由上方控制；关系融合可在高级检查中调试。")}</p>
                <textarea
                  aria-label={t("高级查询参数")}
                  className="code-input"
                  value={advanced}
                  onChange={(e) => setAdvanced(e.target.value)}
                  rows={5}
                />
              </details>
              <button
                type="button"
                className="text-button"
                onClick={() => {
                  setFrom("");
                  setUntil("");
                  setValid("");
                  setDateFrom("");
                  setDateUntil("");
                  setSort("score");
                  setAdvanced("{}");
                }}
              >
                {t("重置筛选与参数")}</button>
            </div>
          )}
        </form>
        <div className="query-hint">
          <span>
            {mode === "Lexical"
              ? t("关键词匹配 · 精确发现")
              : mode === "Vector"
                ? t("向量相似度 · 语义发现")
                : t("关键词 + 向量 · 融合排序")}
          </span>
          <span>{t("Enter 运行检索")}</span>
        </div>
      </section>
      <Notice>{error}</Notice>
      {!result && !busy ? (
        <section className="panel search-empty">
          <Empty
            title={t("让数据给出答案")}
            text={t("输入查询并运行检索，在这里查看匹配记录与通道得分。")}
          />
          <div className="search-capabilities">
            <span>
              01 <b>{t("关键词匹配")}</b>
            </span>
            <span>
              02 <b>{t("语义检索")}</b>
            </span>
            <span>
              03 <b>{t("混合排序")}</b>
            </span>
          </div>
        </section>
      ) : busy ? (
        <section className="panel loading-block">
          <Busy label={phase || t("正在检索集合")} />
          <div className="skeleton" />
          <div className="skeleton" />
          <div className="skeleton" />
        </section>
      ) : (
        result && (
          <div className={"split-content " + (selected ? "has-preview" : "")}>
            <section className="panel results-panel">
              <div className="results-toolbar">
                <h2>
                  {t("检索结果")}{" "}
                  <span className="count-badge">
                    {total == null ? t("总数未知") : total.toLocaleString(getLocale())}
                  </span>
                </h2>
                <span className="muted">
                  {(result.time / 1e6).toLocaleString(getLocale(), {
                    maximumFractionDigits: 2,
                  })}{" "}
                  ms
                </span>
              </div>
              {!result.hits.length ? (
                <Empty
                  title={t("没有找到匹配记录")}
                  text={t("尝试其他关键词、调整向量或放宽筛选条件。")}
                />
              ) : (
                <div className="result-list">
                  {result.hits.map((hit, index) => (
                    <button
                      className={
                        "result-card " + (selected === hit ? "selected" : "")
                      }
                      key={hit.record.record_id}
                      onClick={() => setSelected(hit)}
                    >
                      <span className="result-rank">
                        {String(page * 10 + index + 1).padStart(2, "0")}
                      </span>
                      <div>
                        <div className="result-meta">
                          <span className="mono">{hit.record.record_id}</span>
                          <span title={String(hit.score)}>{t("最终得分")}{scoreText(hit.score)}</span>
                        </div>
                        <h3>
                          {recordTitle(hit.record)}
                          <ArrowUpRight size={15} />
                        </h3>
                        <p>{recordBody(hit.record) || t("点击查看记录内容")}</p>
                        <div className="channel-tags">
                          {scoreRows(hit, resultMode).map(({ channel, value }) => (
                            <span key={channel}>{channelName(channel)}：{value ? scoreText(value.raw_score) : t("未进入候选或未启用")}</span>
                          ))}
                        </div>
                      </div>
                    </button>
                  ))}
                </div>
              )}
              <div className="pagination">
                <span>
                  {result.evaluation_time_ms
                    ? t("评估于 ") +
                      new Date(result.evaluation_time_ms).toLocaleTimeString(getLocale())
                    : ""}
                </span>
                <div>
                  <button
                    className="secondary compact"
                    aria-label={t("上一页结果")}
                    disabled={page === 0}
                    onClick={() => run(page - 1, false)}
                  >
                    <ArrowLeft size={14} />
                  </button>
                  <span>{t("第 {0} 页", page + 1)}</span>
                  <button
                    className="secondary compact"
                    aria-label={t("下一页结果")}
                    disabled={
                      total == null
                        ? result.hits.length < 10
                        : (page + 1) * 10 >= total
                    }
                    onClick={() => run(page + 1, false)}
                  >
                    <ArrowRight size={14} />
                  </button>
                </div>
              </div>
            </section>
            {selected && (
              <RecordPreview
                record={selected.record}
                close={() => setSelected(null)}
                scoreDetails={<ScoreDetails hit={selected} mode={resultMode} />}
                explanation={makeExplanation(selected)}
              />
            )}
          </div>
        )
      )}
    </>
  );
}
function makeExplanation(hit: Hit) {
  return (
    '{"score":' +
    (apiFieldText(hit, "score") || "null") +
    ',"channels":' +
    (apiFieldText(hit, "channels") || "[]") +
    "}"
  );
}

// Preserve arbitrary numeric tokens in advanced JSON instead of rounding i64 filters.
import { apiJsonText, parseApiJson } from "./json.ts";
function mergeQuerySource(source: string, body: Json, controlled: Set<string>) {
  const original = parseApiJson(source),
    parts: string[] = [];
  for (const key of Object.keys(original)) {
    if (!controlled.has(key))
      parts.push(JSON.stringify(key) + ":" + rawField(original, key));
  }
  for (const key of controlled) {
    if (body[key] === undefined) continue;
    // Unmodified advanced filters/sort/temporal retain their source.
    const unchanged =
      original[key] !== undefined &&
      JSON.stringify(original[key]) === JSON.stringify(body[key]);
    if (unchanged)
      parts.push(JSON.stringify(key) + ":" + rawField(original, key));
    else if (
      key === "filter" &&
      original.filter &&
      body.filter?.length > original.filter.length
    ) {
      parts.push(
        '"filter":' +
          apiJsonText(original.filter).slice(0, -1) +
          (original.filter.length ? "," : "") +
          JSON.stringify(body.filter.at(-1)) +
          "]",
      );
    } else parts.push(JSON.stringify(key) + ":" + JSON.stringify(body[key]));
  }
  return "{" + parts.join(",") + "}";
}
function rawField(object: Json, key: string) {
  if (typeof object[key] === "string") return JSON.stringify(object[key]);
  return apiFieldText(object, key) || "null";
}
