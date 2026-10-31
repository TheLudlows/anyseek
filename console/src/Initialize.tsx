import { t } from "./i18n.ts";
import { useRef, useState } from "react";
import { ApiError } from "./api.ts";
import { Busy, Notice } from "./components.tsx";
import {
  clearBootstrap,
  loadBootstrap,
  prepareBootstrap,
  runBootstrap,
  type Bootstrap,
} from "./bootstrap.ts";

export function Initialize({
  connect,
  back,
}: {
  connect: (key: string) => Promise<void>;
  back: () => void;
}) {
  const [pending, setPending] = useState<Bootstrap | null>(() =>
    loadBootstrap(),
  );
  const [tenant, setTenant] = useState(pending?.tenant ?? "");
  const [master, setMaster] = useState("");
  const [confirmed, setConfirmed] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const locked = useRef(false);
  function editTenant() {
    if (locked.current || pending?.stage !== "tenant") return;
    try {
      // No key request has been sent at this stage. Remove persistence first:
      // if storage fails, keep the original request locked and retryable.
      clearBootstrap();
      setPending(null);
      setMaster("");
      setConfirmed(false);
      setError("");
    } catch {
      setError(t("无法清除原初始化请求，请检查浏览器存储设置后重试。"));
    }
  }
  async function submit() {
    if (locked.current || !confirmed) return;
    locked.current = true;
    setBusy(true);
    setError("");
    let request = pending;
    try {
      request ??= loadBootstrap() ?? prepareBootstrap(tenant);
      setPending(request);
      if (request.stage !== "connect") {
        await runBootstrap(request, master.trim());
        setMaster("");
        setPending({ ...request });
        setConfirmed(false);
      } else {
        await connect(request.key);
        clearBootstrap();
      }
    } catch (e) {
      // Server messages may echo credentials, so only show bounded local text.
      setError(
        e instanceof ApiError && e.status === 401
          ? t("Key 无效或已失效，请检查后重试原请求。")
          : e instanceof ApiError && e.status === 409
            ? t("初始化发生冲突，未将冲突视为成功，也未覆盖已有 Key。已有 Tenant 请返回使用已签发的普通 Key；保留的原请求可继续重试。")
            : e instanceof ApiError && e.status === 410
              ? t("原请求已过重试期限，请联系管理员核实 Tenant 和 Key 状态；不会自动签发新 Key。")
              : t("初始化或连接未完成。请检查服务后重试原请求；已完成的步骤会保留，不会生成新的 Key。"),
      );
      if (request) setPending({ ...request });
    } finally {
      locked.current = false;
      setBusy(false);
    }
  }
  return (
    <form
      className="connect-form"
      onSubmit={(e) => {
        e.preventDefault();
        void submit();
      }}
    >
      <h3>{t("首次初始化")}</h3>
      <p className="field-help">
        {t("创建新 Tenant 并签发普通 Key，然后进入工作区。已有 Tenant 请返回使用管理员签发的 Tenant API Key。")}</p>
      <label>
        {t("Tenant 名称")}<input
          required
          pattern="[a-z][a-z0-9_\-]{0,62}"
          maxLength={63}
          value={tenant}
          disabled={busy || !!pending}
          onChange={(e) => setTenant(e.target.value)}
          placeholder={t("例如 my_workspace")}
        />
      </label>
      <p className="field-help">
        {t("以小写字母开头，仅含小写字母、数字、下划线或连字符。创建后不能改名或删除。")}</p>
      {pending?.stage === "tenant" && (
        <div>
          <p className="field-help">
            {t("名称已锁定，以便重试原请求。尚未开始签发普通 Key，可重新填写；这只清除本地请求，不会删除服务端可能已创建的 Tenant。")}
          </p>
          <button type="button" className="text-button" disabled={busy} onClick={editTenant}>
            {t("重新填写 Tenant")}
          </button>
        </div>
      )}
      {pending && pending.stage !== "tenant" && (
        <p className="field-help">
          {t("已进入普通 Key 签发流程，Tenant 名称不能修改。请继续重试原请求或保存已签发的 Key。")}
        </p>
      )}
      {pending?.stage !== "connect" && (
        <label>
          Master Key
          <input
            type="password"
            autoComplete="off"
            required
            value={master}
            disabled={busy}
            onChange={(e) => setMaster(e.target.value)}
          />
        </label>
      )}
      <p className="field-help">
        {t("Master Key 仅临时用于管理请求，不保存。原初始化请求及普通 Key 保留在当前标签页会话中，以便返回或刷新后重试；请勿关闭此标签页或切换服务。")}</p>
      {pending && (
        <p role="status">
          Tenant：{pending.tenant} ·{" "}
          {pending.stage === "tenant"
            ? t("待确认 Tenant 创建结果")
            : pending.stage === "key"
              ? t("Tenant 已创建，待确认 Key 签发结果")
              : t("普通 Key 已签发，待连接")}
        </p>
      )}
      {pending?.stage === "connect" && (
        <label>
          {t("新 Tenant API Key")}<input
            readOnly
            value={pending.key}
            onFocus={(e) => e.target.select()}
          />
          <span className="field-help">
            {t("请复制并妥善保存此 Key，以便以后连接；关闭标签页后控制台不再保留。")}</span>
        </label>
      )}
      <label className="checkbox">
        <input
          type="checkbox"
          checked={confirmed}
          disabled={busy}
          onChange={(e) => setConfirmed(e.target.checked)}
        />
        {pending?.stage === "connect"
          ? t("我已保存普通 Key，确认进入工作区")
          : t("我确认创建 Tenant 并签发普通 Key（重试时复用原请求）")}
      </label>
      <Notice>{error}</Notice>
      <button className="primary" disabled={busy || !confirmed}>
        {busy ? (
          <Busy label={t("初始化中")} />
        ) : pending?.stage === "connect" ? (
          t("进入工作区")
        ) : pending ? (
          t("重试原初始化请求")
        ) : (
          t("确认初始化")
        )}
      </button>
      <button
        className="text-button"
        type="button"
        disabled={busy}
        onClick={() => {
          setMaster("");
          back();
        }}
      >
        {t("返回普通 Key 连接")}</button>
      {pending && (
        <p className="field-help">
          {t("返回不会撤销已创建的 Tenant 或 Key，原请求会保留。重试须在服务允许的期限内完成（当前或前一 UTC 日）。")}</p>
      )}
    </form>
  );
}
