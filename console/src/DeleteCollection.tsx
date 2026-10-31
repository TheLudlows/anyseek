import { useState } from "react";
import { message, storage } from "./api.ts";
import { Busy, Modal, Notice } from "./components.tsx";
import { deleteCollection, type PendingDelete } from "./collection-delete.ts";

export function DeleteCollection({ owner, apiKey, collection, close, deleted }: {
  owner: string; apiKey: string; collection: string; close: () => void; deleted: () => Promise<void>;
}) {
  const [confirmation, setConfirmation] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [pending, setPending] = useState(() => storage.get<PendingDelete>("pendingDelete"));
  const retry = pending?.owner === owner && pending.collection === collection;
  return <Modal title={`删除集合 · ${collection}`} close={close} busy={busy}>
    <p>删除后，此集合及其记录将无法继续访问，界面不支持撤销。底层文件稍后回收。</p>
    {retry ? <Notice kind="info">已有待确认删除，重试将复用原始请求，不重新创建删除操作。</Notice> :
      <label>输入集合名称以确认：{collection}
        <input aria-label="确认删除的集合名称" autoComplete="off" value={confirmation} disabled={busy} onChange={e => setConfirmation(e.target.value)} />
      </label>}
    <Notice>{error}</Notice>
    <div className="modal-actions">
      <button type="button" className="secondary" disabled={busy} onClick={close}>关闭</button>
      <button type="button" className="primary danger" disabled={busy || (!retry && confirmation !== collection)} onClick={async () => {
        setBusy(true); setError("");
        try { await deleteCollection(owner, apiKey, collection); await deleted(); close(); }
        catch (e) { setPending(storage.get<PendingDelete>("pendingDelete")); setError(message(e)); }
        finally { setBusy(false); }
      }}>{busy ? <Busy label="正在删除" /> : retry ? "重试原始删除" : "确认删除集合"}</button>
    </div>
  </Modal>;
}
