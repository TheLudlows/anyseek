import { english } from "./messages.ts";
export type Language = "zh" | "en";
const listeners = new Set<() => void>();
function readLanguage(): Language {
  try { return globalThis.localStorage?.getItem("anyseek.language") === "en" ? "en" : "zh"; }
  catch { return "zh"; }
}
let language: Language = readLanguage();
export const getLocale = () => language === "zh" ? "zh-CN" : "en-US";
export const getLanguage = () => language;
export const subscribeLanguage = (listener: () => void) => {
  listeners.add(listener);
  return () => { listeners.delete(listener); };
};
function applyLanguage() {
  if (typeof document === "undefined") return;
  document.documentElement.lang = language === "zh" ? "zh-CN" : "en";
  document.title = language === "zh" ? "AnySeek · 语义空间" : "AnySeek · Semantic Space";
}
export function setLanguage(value: Language) {
  language = value;
  try { globalThis.localStorage?.setItem("anyseek.language", value); } catch { /* Switching still works without persistent storage. */ }
  applyLanguage();
  listeners.forEach(listener => listener());
}
export function t(key: string, ...values: unknown[]): string {
  const template = language === "en" ? english[key] ?? key : key;
  return template.replace(/\{(\d+)\}/g, (match, index: string) => Number(index) < values.length ? String(values[Number(index)]) : match);
}
// Translate saved UI notices without touching records or API payloads.
const noticePatterns = Object.entries(english).flatMap(([zh, en]) => [zh, en].map(template => {
  const slots: number[] = [];
  const pattern = template.split(/(\{\d+\})/g).map(part => {
    const slot = part.match(/^\{(\d+)\}$/);
    if (slot) { slots.push(Number(slot[1])); return "([\\s\\S]*?)"; }
    return part.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  }).join("");
  return { zh, slots, pattern: new RegExp("^" + pattern + "$") };
}));
export function localizeNotice(text: string): string {
  for (const { zh, slots, pattern } of noticePatterns) {
    const match = text.match(pattern);
    if (!match) continue;
    const values: string[] = [];
    slots.forEach((slot, i) => { values[slot] = match[i + 1]; });
    return t(zh, ...values);
  }
  return text;
}
applyLanguage();
