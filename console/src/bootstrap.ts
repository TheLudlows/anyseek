import { api, newIdentity, newOperationId, storage } from "./api.ts";
export type Bootstrap = {
  tenant: string;
  tenantRequest: string;
  keyRequest: string;
  key: string;
  stage: "tenant" | "key" | "connect";
};
export function prepareBootstrap(tenant: string): Bootstrap {
  if (!/^[a-z][a-z0-9_-]{0,62}$/.test(tenant))
    throw new Error("Invalid tenant name");
  const key = btoa(
    String.fromCharCode(...crypto.getRandomValues(new Uint8Array(32))),
  );
  return {
    tenant,
    key,
    stage: "tenant",
    tenantRequest: JSON.stringify({
      operation_id: newOperationId(),
      tenant_id: tenant,
    }),
    keyRequest: JSON.stringify({
      operation_id: newOperationId(),
      tenant_id: tenant,
      key_id: "console-" + newIdentity(),
      apikey_base64: key,
    }),
  };
}
export const loadBootstrap = () => storage.get<Bootstrap>("bootstrap");
export const saveBootstrap = (pending: Bootstrap) =>
  storage.set("bootstrap", pending);
export const clearBootstrap = () => storage.remove("bootstrap");
// Save exact bodies before sending; lost responses replay the same identities.
// Only the ordinary key is persisted. Master credentials never enter this state.
export async function runBootstrap(pending: Bootstrap, master: string) {
  saveBootstrap(pending);
  if (pending.stage === "tenant") {
    await api(master, "tenants", pending.tenantRequest);
    pending.stage = "key";
    saveBootstrap(pending);
  }
  if (pending.stage === "key") {
    await api(master, "apikey", pending.keyRequest);
    pending.stage = "connect";
    saveBootstrap(pending);
  }
  return pending.key;
}
