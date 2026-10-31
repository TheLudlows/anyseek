import { Languages } from "lucide-react";
import { useSyncExternalStore } from "react";
import { getLanguage, setLanguage, subscribeLanguage } from "./i18n.ts";
export function LanguageSwitch() {
  const language = useSyncExternalStore(subscribeLanguage, getLanguage, () => "zh");
  return <label className="language-switch icon-button" title="语言 / Language">
    <Languages size={18} aria-hidden="true" />
    <select aria-label="语言 / Language" value={language} onChange={event => setLanguage(event.target.value === "en" ? "en" : "zh")}>
      <option value="zh">简体中文</option>
      <option value="en">English</option>
    </select>
  </label>;
}
