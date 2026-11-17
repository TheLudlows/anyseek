import { t } from "./i18n.ts";
import { useState } from "react";
import { api, message, newOperationId, ownerFor, storage } from "./api.ts";
import { Busy, JsonView, Notice } from "./components.tsx";
import type { Json } from "./types.ts";

export function Recovery({tenant}: {tenant: string}) {
  const [backup, setBackup] = useState("");
  const [busy, setBusy] = useState(false),
    [result, setResult] = useState<Json>(null),
    [error, setError] = useState("");
  const [confirmRestore, setConfirmRestore] = useState(false);
  async function run(action: "backup" | "restore") {
    setBusy(true);
    setError("");
    setResult(null);
    try {
      const owner = await ownerFor(tenant);
      let pending = storage.get<{ owner: string; body: string }>(
        "service." + action,
      );
      if (pending && pending.owner !== owner)
        throw new Error(
          t("已有另一租户连接的待确认操作，请切回原租户重试。"),
        );
      if (!pending) {
        if (action === "restore" && !/^[0-9a-f]{64}$/.test(backup))
          throw new Error(t("备份 ID 应为 64 位小写十六进制字符串。"));
        pending = {
          owner,
          body: JSON.stringify({
            operation_id: newOperationId(),
            ...(action === "restore" ? { backup_id: backup } : {}),
          }),
        };
        storage.set("service." + action, pending);
      }
      const value = await api(
        tenant,
        action === "backup" ? "service-backups" : "service-restores",
        pending.body,
      );
      storage.remove("service." + action);
      setResult(value);
      if (action === "backup") setBackup(value.backup_id);
      setConfirmRestore(false);
    } catch (e) {
      setError(message(e));
    } finally {
      setBusy(false);
    }
  }
  return (
    <section className="panel recovery-panel">
      <div className="panel-toolbar">
        <div>
          <h2>{t("服务备份与恢复")}</h2>
          <p>{t("管理员操作，权限由上游网关校验。")}</p>
        </div>
        <span className="tag">{t("管理员")}</span>
      </div>
      <div className="tool-body">
        <Notice kind="info">
          {t("恢复会创建新的停止状态数据根目录，不替换运行中的服务；服务凭据由目标进程配置提供。")}</Notice>
        <label>
          {t("服务备份 ID")}<input
            className="mono"
            value={backup}
            onChange={(e) => setBackup(e.target.value.trim())}
            placeholder={t("创建备份后获取，或粘贴已有备份 ID")}
          />
        </label>
        <div className="inline-actions">
          <button
            className="secondary"
            disabled={busy}
            onClick={() => run("backup")}
          >
            {busy ? <Busy label={t("处理中")} /> : t("创建 / 重试备份")}
          </button>
          <button
            className="secondary"
            disabled={busy}
            onClick={() => setConfirmRestore(true)}
          >
            {t("恢复 / 重试到新目录")}</button>
        </div>
        {confirmRestore && (
          <div className="restore-confirm">
            <Notice kind="info">
              {t("确认使用上方备份创建新的恢复目录？如果有待确认恢复，将复用其原始请求。")}</Notice>
            <button
              className="primary"
              disabled={busy}
              onClick={() => run("restore")}
            >
              {t("确认恢复")}</button>
            <button
              className="text-button"
              disabled={busy}
              onClick={() => setConfirmRestore(false)}
            >
              {t("取消")}</button>
          </div>
        )}
        <Notice>{error}</Notice>
        {result && <JsonView value={result} label={t("服务响应")} />}
        <p className="field-help">
          {t("网络中断时保留原始请求；使用相同租户连接重试，避免重复操作。")}</p>
      </div>
    </section>
  );
}
