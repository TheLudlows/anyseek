import { LanguageSwitch } from "./LanguageSwitch.tsx";
import { getLanguage, subscribeLanguage } from "./i18n.ts";
import { t } from "./i18n.ts";
import { Component, useSyncExternalStore, type ReactNode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App.tsx";
import "./styles.css";
function FatalScreen() {
  useSyncExternalStore(subscribeLanguage, getLanguage, () => "zh");
  return <div className="fatal"><LanguageSwitch /><h1>{t("控制台遇到问题")}</h1><p>{t("请刷新后重试。原始待确认请求会继续保留。")}</p><button onClick={() => location.reload()}>{t("重新加载")}</button></div>;
}
class Boundary extends Component<{ children: ReactNode }, { failed: boolean }> {
  state = { failed: false };
  static getDerivedStateFromError() {
    return { failed: true };
  }
  render() {
    return this.state.failed ? (
      <FatalScreen />
    ) : (
      this.props.children
    );
  }
}
createRoot(document.getElementById("root")!).render(
  <Boundary>
    <App />
  </Boundary>,
);
