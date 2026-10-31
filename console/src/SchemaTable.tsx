import { t } from "./i18n.ts";
import type { SchemaField } from "./types.ts";

const types = (): Record<string, string> => ({
  Text: t("文本"), String: t("字符串"), StringSet: t("字符串集合"),
  Int: t("整数"), Float: t("小数"), Timestamp: t("时间戳"),
  Json: t("JSON 对象"), Binary: t("二进制"),
});

export function SchemaTable({ fields, dimensions }: {
  fields: SchemaField[];
  dimensions?: number | null;
}) {
  return (
    <div className="schema-summary">
      <p className="field-help">
        {t("共 {0} 个字段", fields.length)}{dimensions ? t(" · 向量维度 {0}", dimensions) : ""}
      </p>
      <div className="table-scroll">
        <table className="data-table" aria-label={t("集合字段结构")}>
          <thead>
            <tr>
              <th>{t("字段名称")}</th><th>{t("数据类型")}</th><th>{t("检索方式")}</th>
              <th>{t("过滤与排序")}</th><th>{t("搜索结果返回")}</th><th>{t("全文权重")}</th>
            </tr>
          </thead>
          <tbody>
            {fields.map((field) => {
              const lexical = field.index_lexical ?? (!field.index_vector && field.field_type === "Text");
              const stored = field.store ?? !field.index_vector;
              const retrieval = [lexical && t("全文"), field.index_vector && t("向量")].filter(Boolean).join(" + ");
              return (
                <tr key={field.field}>
                  <td className="mono">{field.field}</td>
                  <td>{field.index_vector ? dimensions ? t("{0} 维向量", dimensions) : t("向量（维度未知）") : types()[field.field_type] || field.field_type}</td>
                  <td>{retrieval || t("不参与检索")}</td>
                  <td>{field.filterable ? t("支持") : t("不启用")}</td>
                  <td>{stored ? t("返回") : t("不返回")}</td>
                  <td>{lexical ? field.boost ?? 1 : "—"}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <p className="field-help">{t("“搜索结果返回”控制搜索命中的字段展示，不影响原始记录保存。")}</p>
    </div>
  );
}
