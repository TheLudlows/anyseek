import type { SchemaField } from "./types.ts";

const types: Record<string, string> = {
  Text: "文本", String: "字符串", StringSet: "字符串集合",
  Int: "整数", Float: "小数", Timestamp: "时间戳",
  Json: "JSON 对象", Binary: "二进制",
};

export function SchemaTable({ fields, dimensions }: {
  fields: SchemaField[];
  dimensions?: number | null;
}) {
  return (
    <div className="schema-summary">
      <p className="field-help">
        共 {fields.length} 个字段{dimensions ? ` · 向量维度 ${dimensions}` : ""}
      </p>
      <div className="table-scroll">
        <table className="data-table" aria-label="集合字段结构">
          <thead>
            <tr>
              <th>字段名称</th><th>数据类型</th><th>检索方式</th>
              <th>过滤与排序</th><th>搜索结果返回</th><th>全文权重</th>
            </tr>
          </thead>
          <tbody>
            {fields.map((field) => {
              const lexical = field.index_lexical ?? (!field.index_vector && field.field_type === "Text");
              const stored = field.store ?? !field.index_vector;
              const retrieval = [lexical && "全文", field.index_vector && "向量"].filter(Boolean).join(" + ");
              return (
                <tr key={field.field}>
                  <td className="mono">{field.field}</td>
                  <td>{field.index_vector ? `向量（${types[field.field_type] || field.field_type}）` : types[field.field_type] || field.field_type}</td>
                  <td>{retrieval || "不参与检索"}</td>
                  <td>{field.filterable ? "支持" : "不启用"}</td>
                  <td>{stored ? "返回" : "不返回"}</td>
                  <td>{lexical ? field.boost ?? 1 : "—"}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <p className="field-help">“搜索结果返回”控制搜索命中的字段展示，不影响原始记录保存。</p>
    </div>
  );
}
