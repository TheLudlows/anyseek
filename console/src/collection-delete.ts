import { api, collectionPath, newOperationId, storage } from "./api.ts";
import { clearEmbeddingConfig } from "./embeddings.ts";
import type { PendingWrites } from "./writes.ts";

export type PendingDelete = { owner: string; collection: string; body: string; confirmed?: boolean };
export async function deleteCollection(owner: string, apiKey: string, collection: string) {
  let pending = storage.get<PendingDelete>("pendingDelete");
  if (pending && (pending.owner !== owner || pending.collection !== collection))
    throw new Error("请先回到原连接与集合处理待确认删除。");
  const writes = storage.get<PendingWrites>("pendingWrites");
  if (!pending && writes?.owner === owner && writes.collection === collection)
    throw new Error("此集合有待确认写入，请先在数据导入界面处理原任务。");
  if (!pending) {
    pending = { owner, collection, body: JSON.stringify({ operation_id: newOperationId() }) };
    storage.set("pendingDelete", pending);
  }
  if (!pending.confirmed) {
    const response = await api(apiKey, collectionPath(collection), pending.body, "DELETE");
    if (response?.deleted !== true) throw new Error("未收到删除确认，请使用原始请求重试。");
    pending.confirmed = true;
    storage.set("pendingDelete", pending);
  }
  clearEmbeddingConfig(owner, collection);
  if (storage.get<string>("collection") === collection) storage.remove("collection");
  storage.remove("pendingDelete");
}
