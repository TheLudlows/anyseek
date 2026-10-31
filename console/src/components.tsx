import { LanguageSwitch } from "./LanguageSwitch.tsx";
import { localizeNotice } from "./i18n.ts";
import { t } from "./i18n.ts";
import { useState, Children, isValidElement, cloneElement, Fragment, type ReactNode } from "react";
import {
  AlertCircle,
  ArrowRight,
  Check,
  Copy,
  LoaderCircle,
  Search,
  X,
} from "lucide-react";
import { apiJsonText, apiFieldText, prettyJson } from "./json.ts";
import type { RecordData } from "./types.ts";

function noticeContent(children: ReactNode): ReactNode {
  return Children.map(children, child => {
    if (typeof child === "string") return localizeNotice(child);
    if (isValidElement<{ children?: ReactNode }>(child) && child.type === Fragment)
      return cloneElement(child, {}, noticeContent(child.props.children));
    return child;
  });
}
export function Notice({
  children,
  kind = "error",
}: {
  children?: ReactNode;
  kind?: "error" | "success" | "info";
}) {
  return children ? (
    <div
      className={"notice " + kind}
      role={kind === "error" ? "alert" : "status"}
    >
      <AlertCircle size={16} />
      <div>{noticeContent(children)}</div>
    </div>
  ) : null;
}
export function Busy({ label = t("正在加载") }: { label?: string }) {
  return (
    <span className="busy" role="status">
      <LoaderCircle size={15} className="spin" />
      {localizeNotice(label)}
    </span>
  );
}
export function Empty({
  title,
  text,
  action,
}: {
  title: string;
  text: string;
  action?: ReactNode;
}) {
  return (
    <div className="empty">
      <span className="empty-icon">
        <Search size={24} />
      </span>
      <h3>{title}</h3>
      <p>{text}</p>
      {action}
    </div>
  );
}
export function JsonView({
  value,
  label = "JSON",
}: {
  value: unknown;
  label?: string;
}) {
  const [copied, setCopied] = useState(false),
    [error, setError] = useState("");
  const source = typeof value === "string" ? value : apiJsonText(value);
  return (
    <div className="json-view">
      <div className="json-bar">
        <span>{label}</span>
        <button
          className="icon-button"
          type="button"
          aria-label={t("复制 ") + label}
          onClick={async () => {
            try {
              await navigator.clipboard.writeText(source);
              setCopied(true);
              setTimeout(() => setCopied(false), 1800);
            } catch {
              setError(t("复制失败，请选择下方内容手动复制。"));
            }
          }}
        >
          {copied ? <Check size={14} /> : <Copy size={14} />}
        </button>
      </div>
      <pre>{prettyJson(source)}</pre>
      {error && <small role="alert">{error}</small>}
    </div>
  );
}
export function Modal({
  title,
  children,
  close,
  busy = false,
}: {
  title: string;
  children: ReactNode;
  close: () => void;
  busy?: boolean;
}) {
  // Native dialog provides focus containment and Escape semantics.
  return (
    <dialog
      open
      className="modal"
      ref={(node) => {
        if (node && !node.matches(":modal")) {
          node.close();
          node.showModal();
        }
      }}
      onCancel={(event) => {
        event.preventDefault();
        if (!busy) close();
      }}
    >
      <div className="modal-title">
        <h2>{title}</h2>
        <LanguageSwitch />
        <button
          className="icon-button"
          disabled={busy}
          aria-label={t("关闭")}
          onClick={close}
        >
          <X size={18} />
        </button>
      </div>
      {children}
    </dialog>
  );
}
export function recordTitle(record: RecordData) {
  const doc = record.document || {};
  if (doc.title != null) return apiFieldText(doc, "title");
  const first = Object.keys(doc).find((k) => typeof doc[k] === "string");
  return first ? apiFieldText(doc, first) : record.record_id;
}
export function recordBody(record: RecordData) {
  const doc = record.document || {};
  const first = ["body", "text", "content", "description"].find(
    (k) => doc[k] != null,
  );
  return first ? apiFieldText(doc, first) : "";
}
export function RecordPreview({
  record,
  close,
  explanation,
  scoreDetails,
}: {
  record: RecordData;
  close: () => void;
  explanation?: unknown;
  scoreDetails?: ReactNode;
}) {
  let sourceUrl: string | null = null;
  try {
    const url = new URL(String(record.document?.url || ""));
    if (["http:", "https:"].includes(url.protocol)) sourceUrl = url.href;
  } catch {
    /* Records without a web URL still have their full JSON preview. */
  }
  return (
    <aside className="preview" aria-label={t("记录详情")}>
      <div className="preview-heading">
        <span>{t("记录详情")}</span>
        <button className="icon-button" aria-label={t("关闭预览")} onClick={close}>
          <X size={17} />
        </button>
      </div>
      <div className="preview-inner">
        <span className="eyebrow">RECORD</span>
        <h2>{recordTitle(record)}</h2>
        <dl className="metadata">
          <dt>Record ID</dt>
          <dd>{apiFieldText(record, "record_id")}</dd>
          <dt>{t("版本")}</dt>
          <dd>{apiFieldText(record, "version")}</dd>
        </dl>
        {recordBody(record) && (
          <p className="preview-body">{recordBody(record)}</p>
        )}
        {sourceUrl && (
          <a
            className="source-link"
            href={sourceUrl}
            target="_blank"
            rel="noopener noreferrer"
          >
            {t("打开来源链接")}<ArrowRight size={13} />
          </a>
        )}
        {scoreDetails}
        <JsonView value={record.document} label="Document JSON" />
        <details>
          <summary>
            {t("完整记录")}<ArrowRight size={13} />
          </summary>
          <JsonView value={record} label="Record JSON" />
        </details>
        {explanation != null && (
          <details>
            <summary>{t("原始检索解释 · JSON")}</summary>
            <JsonView value={explanation} label={t("检索得分与通道")} />
          </details>
        )}
      </div>
    </aside>
  );
}
