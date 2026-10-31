import type { Hit, Mode } from "./types.ts";

export type ChannelScore = {
  channel: string;
  rank: number;
  rank_basis: string;
  raw_score?: number | null;
  rrf_contribution?: number | null;
};
export const channelName = (name: string) => ({ lexical: "关键词", vector: "向量", relation: "关系", browse: "浏览" }[name] || name);
export const rankBasis = (basis: string) => ({ score: "按得分", requested_order: "按指定排序", stable_adjacency: "按邻接顺序", browse_order: "按浏览顺序" }[basis] || basis);
export function scoreText(score: number | null | undefined) {
  if (score == null || !Number.isFinite(score)) return "—";
  return Number(score.toPrecision(7)).toString();
}
export function scoreRows(hit: Hit, mode: Mode): { channel: string; value?: ChannelScore }[] {
  const channels = hit.channels || [];
  // Older responses without explanations cannot distinguish missing candidates.
  if (!channels.length) return [];
  const expected = mode === "Hybrid" ? ["lexical", "vector"] : [mode === "Vector" ? "vector" : "lexical"];
  return [...new Set([...expected, ...channels.map(c => c.channel)])].map(channel => ({ channel, value: channels.find(c => c.channel === channel) }));
}
