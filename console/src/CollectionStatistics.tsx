import { getLocale, t } from "./i18n.ts";
import { apiFieldText } from "./json.ts";
import type { Collection } from "./types.ts";

type Format = "count" | "bytes" | "time" | "status";
type Metric = { key: string; label: string; help: string; format?: Format };

function metricValue(source: Record<string, unknown> | null | undefined, metric: Metric): string {
  const value = source?.[metric.key];
  if (value == null) return t("未提供");
  if (metric.format === "status")
    return typeof value === "boolean" ? value ? t("已隔离") : t("未隔离") : t("未提供");
  const raw = apiFieldText(source, metric.key);
  // Use the response's integer token, not the rounded JS number, for counters.
  if (!/^\d+$/.test(raw)) return t("未提供");
  const integer = BigInt(raw);
  const exact = integer.toLocaleString(getLocale());
  if (metric.format === "bytes") {
    if (integer < 1024n || integer > BigInt(Number.MAX_SAFE_INTEGER)) return `${exact} B`;
    const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    const exponent = Math.min(Math.floor(Math.log2(Number(integer)) / 10), units.length - 1);
    const size = (Number(integer) / 1024 ** exponent).toLocaleString(getLocale(), { maximumFractionDigits: 2 });
    return `${size} ${units[exponent]} (${exact} B)`;
  }
  if (metric.format === "time") {
    const date = new Date(Number(integer));
    return Number.isNaN(date.getTime()) ? t("未提供") : `${date.toLocaleString(getLocale(), { timeZone: "UTC", hour12: false })} UTC`;
  }
  return exact;
}

export function CollectionStatistics({ statistics, limits }: Pick<Collection, "statistics" | "limits">) {
  const groups: { title: string; source: Record<string, unknown> | null | undefined; metrics: Metric[] }[] = [
    { title: t("运行统计"), source: statistics, metrics: [
      { key: "active_records", label: t("活跃记录"), help: t("当前视图中参与检索统计的有效记录数。") },
      { key: "retained_identities", label: t("保留的记录标识"), help: t("包括已删除记录的墓碑，不等同于活跃记录数。") },
      { key: "retained_operations", label: t("保留的操作回执"), help: t("用于识别重复请求和重试的操作结果数量。") },
      { key: "retained_source_revisions", label: t("保留的来源历史版本"), help: t("独立于操作重试期限保留的历史来源正文数量。") },
      { key: "physical_versions", label: t("物理索引版本"), help: t("包含已失效、会在检索时被过滤的索引版本。") },
      { key: "checkpoint_bytes", label: t("检查点大小"), help: t("完整权威检查点的数据大小，不代表全部存储占用。"), format: "bytes" },
      { key: "visible_seq", label: t("可见序号"), help: t("当前视图向检索组件发布的数据位置，不是记录数量。") },
      { key: "evaluation_time_ms", label: t("统计时间"), help: t("本次统计视图的评估时间，统一显示为 UTC。"), format: "time" },
      { key: "evidence_quarantined", label: t("来源与证据隔离"), help: t("已隔离表示来源与证据暂不提供，等待可信的删除历史核对。"), format: "status" },
    ] },
    { title: t("容量限制"), source: limits, metrics: [
      { key: "max_records", label: t("记录标识上限"), help: t("允许保留的记录标识数量上限，包含删除墓碑。") },
      { key: "max_operations", label: t("操作回执上限"), help: t("允许保留的操作结果数量上限。") },
      { key: "max_journal_bytes", label: t("日志容量上限"), help: t("单代日志的字节上限；发布检查点期间可能同时保留两代。"), format: "bytes" },
      { key: "max_candidates", label: t("检索候选上限"), help: t("分区检索候选与最终结果的数量上限。") },
    ] },
  ];
  return <div className="collection-statistics">
    {groups.map(group => <section key={group.title} aria-label={group.title}>
      <h3>{group.title}</h3>
      <table className="data-table statistics-table" aria-label={group.title}>
        <thead><tr><th>{t("统计项")}</th><th>{t("数值")}</th></tr></thead>
        <tbody>{group.metrics.map(metric => <tr key={metric.key}>
          <th scope="row"><span>{metric.label}</span><small>{metric.help}</small></th>
          <td>{metricValue(group.source, metric)}</td>
        </tr>)}</tbody>
      </table>
    </section>)}
  </div>;
}
