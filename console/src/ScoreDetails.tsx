import type { Hit, Mode } from "./types.ts";
import { channelName, rankBasis, scoreRows, scoreText } from "./scores.ts";

export function ScoreDetails({ hit, mode }: { hit: Hit; mode: Mode }) {
  const rows = scoreRows(hit, mode);
  const fused = hit.channels?.some(c => c.rrf_contribution != null);
  return <section className="score-details" aria-label="检索打分详情">
    <h3>检索打分</h3>
    <p><strong>{fused ? "最终融合分" : "最终得分"}：{scoreText(hit.score)}</strong></p>
    {rows.length ? <div className="table-scroll"><table className="data-table">
      <thead><tr><th>通道</th><th>原始分数</th><th>通道内排名</th><th>融合贡献</th></tr></thead>
      <tbody>{rows.map(({ channel, value }) => <tr key={channel}>
        <td>{channelName(channel)}</td>
        <td title={value?.raw_score == null ? undefined : String(value.raw_score)}>{value ? scoreText(value.raw_score) : "未进入候选或未启用"}</td>
        <td>{value ? <>{Number.isInteger(value.rank) && value.rank >= 0 ? `第 ${value.rank + 1} 名` : "—"}<small className="score-basis">{rankBasis(value.rank_basis)}</small></> : "—"}</td>
        <td title={value?.rrf_contribution == null ? undefined : String(value.rrf_contribution)}>{scoreText(value?.rrf_contribution)}</td>
      </tr>)}</tbody>
    </table></div> : <p className="field-help">服务未返回通道打分详情。</p>}
    <p className="field-help">关键词与向量原始分数尺度不同，不能直接比较，也不代表置信度。排名从第 1 名展示，仅对应本次通道候选范围。</p>
    {fused && <p className="field-help">最终融合分由各通道的 RRF 贡献相加得到，不是原始分数相加；显示数值经过舍入。</p>}
  </section>;
}
