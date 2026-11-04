import { apiFieldText } from "./json.ts";

// Use retained JSON tokens for both arithmetic and presentation.
export function counter(source: any, key: string): bigint | null {
  if (source == null || typeof source[key] !== "number") return null;
  const raw = apiFieldText(source, key);
  return /^\d+$/.test(raw) ? BigInt(raw) : null;
}
export function remainingCapacity(statistics: any, limits: any, used: string, maximum: string): bigint | null {
  const count = counter(statistics, used), limit = counter(limits, maximum);
  return count === null || limit === null ? null : limit > count ? limit - count : 0n;
}
export function indexState(progress: any): "unknown" | "recovery" | "error" | "pending" | "caught_up" {
  if (progress?.recovery_required === true) return "recovery";
  if (typeof progress?.index_error === "string" && progress.index_error.length > 0) return "error";
  const committed = counter(progress, "committed_seq"), lexical = counter(progress, "lexical_seq"), vector = counter(progress, "vector_seq");
  if (progress?.recovery_required !== false || progress?.index_error !== null || committed === null || lexical === null || vector === null) return "unknown";
  return lexical >= committed && vector >= committed ? "caught_up" : "pending";
}
