import { storage } from "./api.ts";
import type { EmbeddingConfig } from "./embeddings.ts";

type Outcome = "committed" | "rejected" | "unknown" | "not_attempted";
type PendingRequest = { body: string; embedded?: boolean; outcome?: Outcome };
export type PendingWrites = {
  owner: string;
  collection: string;
  requests: PendingRequest[];
  delivery?: "ingest";
  embedding?: { config: EmbeddingConfig; dimensions: number };
  completed: number;
  unresolved: boolean;
  rejected?: boolean;
};

// Existing /doc tasks store a confirmed prefix and the next request's state,
// rather than per-request outcomes. Keep those identities and confirmations.
export function restoreWriteOutcomes(pending: PendingWrites) {
  if (pending.delivery === "ingest") return;
  pending.requests.forEach((request, i) => {
    request.outcome ??= i < pending.completed ? "committed"
      : i === pending.completed && pending.unresolved ? "unknown"
      : i === pending.completed && pending.rejected ? "rejected" : "not_attempted";
  });
}

export function loadPendingWrites() {
  return storage.get<PendingWrites>("pendingWrites");
}
export function savePendingWrites(pending: PendingWrites) {
  storage.set("pendingWrites", pending);
}
export function clearPendingWrites() {
  storage.remove("pendingWrites");
}

function saveOutcomes(pending: PendingWrites) {
  pending.completed = pending.requests.filter((item) => item.outcome === "committed").length;
  pending.unresolved = pending.requests.some((item) => item.outcome === "unknown");
  pending.rejected = !pending.unresolved && pending.requests.some((item) => item.outcome === "rejected");
  savePendingWrites(pending);
}

export function beginWriteAttempt(pending: PendingWrites, batch: PendingRequest[]) {
  const unknownBefore = batch.map((item) => item.outcome === "unknown");
  batch.forEach((item) => { item.outcome = "unknown"; });
  // Persist original identities and unknown outcomes before either transport.
  // A storage failure prevents dispatch; a lost response remains retryable.
  saveOutcomes(pending);
  return unknownBefore;
}

export function finishWriteAttempt(
  pending: PendingWrites, batch: PendingRequest[], unknownBefore: boolean[], outcomes: Outcome[],
) {
  batch.forEach((item, i) => {
    // A later rejection cannot prove that an earlier unknown attempt failed.
    item.outcome = outcomes[i] === "committed" ? "committed"
      : unknownBefore[i] ? "unknown" : outcomes[i];
  });
  saveOutcomes(pending);
}
