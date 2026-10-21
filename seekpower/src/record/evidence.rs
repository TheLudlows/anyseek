//! Bounded, same-partition evidence semantics. Authority is the metadata on each
//! Record, never a second journal or an independently committed dependency graph.
use super::{Record, RecordError, RecordWrite};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

const MAX_REFERENCES: usize = 32;
const MAX_DEPTH: usize = 16;
const MAX_VISITED: usize = 10_000;

/// Immutable source or derived content in the first evidence slice.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceKind {
    /// Caller-supplied original content; parsing is an upper-layer concern.
    Source,
    /// Caller-supplied content supported by precise source/derived references.
    Derived,
}
/// A precise reference in the current collection. Cross-collection references
/// are not accepted; routing is verified rather than inferred from an opaque ID.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EvidenceRef {
    /// Stable identity of the evidence record.
    pub record_id: String,
    /// Immutable routing key of the referenced record.
    pub routing_key: String,
    /// Exact immutable revision, never implicitly rebound to a newer revision.
    pub version: u64,
}
/// Engine-validated metadata, distinct from arbitrary business document fields.
/// Update/recreate, generations, TTL and feed policies are intentionally not
/// exposed until their business semantics are decided.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EvidenceMetadata {
    /// Source or derived record.
    pub kind: EvidenceKind,
    /// Direct dependencies; indirect dependencies are checked transitively.
    #[serde(default)]
    pub evidence_refs: Vec<EvidenceRef>,
}

fn rejected(message: &str) -> RecordError {
    RecordError::Rejected(message.into())
}

/// Check visibility of a bounded immutable revision graph. Dangling or stale
/// references hide content; cycles/structural budget violations fail closed.
fn visit(
    id: &str,
    records: &BTreeMap<String, Record>,
    depth: usize,
    path: &mut BTreeSet<String>,
    memo: &mut BTreeMap<String, (bool, usize)>,
) -> Result<bool, RecordError> {
    if depth > MAX_DEPTH {
        return Err(rejected("evidence depth budget exceeded"));
    }
    if let Some((value, height)) = memo.get(id) {
        if depth + height > MAX_DEPTH {
            return Err(rejected("evidence depth budget exceeded"));
        }
        return Ok(*value);
    }
    if memo.len() + path.len() >= MAX_VISITED {
        return Err(rejected("evidence traversal budget exceeded"));
    }
    if !path.insert(id.into()) {
        return Err(rejected("evidence cycle"));
    }
    let mut height = 0;
    let result = if let Some(record) = records.get(id) {
        let mut visible = record.document.is_some();
        if let Some(system) = &record.system {
            if system.evidence_refs.len() > MAX_REFERENCES {
                return Err(rejected("evidence reference budget exceeded"));
            }
            for reference in &system.evidence_refs {
                let matches = records.get(&reference.record_id).is_some_and(|target| {
                    target.system.is_some()
                        && target.version == reference.version
                        && target.routing_key == reference.routing_key
                });
                // Traverse even a deleted ancestor so cycles cannot be concealed
                // behind tombstones in a recovered authoritative graph.
                let parent_visible = visit(&reference.record_id, records, depth + 1, path, memo)?;
                height = height.max(1 + memo.get(&reference.record_id).map_or(0, |v| v.1));
                visible &= matches && parent_visible;
            }
        }
        visible
    } else {
        false
    };
    path.remove(id);
    memo.insert(id.into(), (result, height));
    Ok(result)
}

pub(super) fn visibility(
    records: &BTreeMap<String, Record>,
) -> Result<BTreeMap<String, bool>, RecordError> {
    let mut memo = BTreeMap::new();
    for id in records.keys() {
        visit(id, records, 0, &mut BTreeSet::new(), &mut memo)?;
    }
    Ok(memo
        .into_iter()
        .map(|(id, (visible, _))| (id, visible))
        .collect())
}

pub(super) fn validate(
    records: &BTreeMap<String, Record>,
    request: &RecordWrite,
) -> Result<(), RecordError> {
    let old = records.get(&request.record_id);
    if request.system.is_some() || old.is_some_and(|r| r.system.is_some()) {
        if request.record_id.len() > 256
            || request.routing_key.len() > 256
            || request.operation_id.nonce.len() > 128
        {
            return Err(rejected(
                "evidence identity exceeds reserved deletion budget",
            ));
        }
    }
    if old.is_some_and(|r| r.system.is_some()) {
        if request.document.is_some() {
            return Err(rejected(
                "source/derived update or recreate unsupported pending policy decision",
            ));
        }
        if old.is_some_and(|r| r.document.is_none()) {
            return Err(RecordError::Conflict(
                "evidence identity already deleted".into(),
            ));
        }
        if request.system.is_some() {
            return Err(rejected("evidence deletion must omit system metadata"));
        }
        return Ok(());
    }
    let Some(system) = &request.system else {
        return Ok(());
    };
    if old.is_some() || request.document.is_none() {
        return Err(rejected("evidence metadata requires a new live identity"));
    }
    if system.evidence_refs.len() > MAX_REFERENCES {
        return Err(rejected("evidence reference budget exceeded"));
    }
    match system.kind {
        EvidenceKind::Source if !system.evidence_refs.is_empty() => {
            return Err(rejected("source cannot carry derived evidence"));
        }
        EvidenceKind::Derived if system.evidence_refs.is_empty() => {
            return Err(rejected("derived record requires evidence"));
        }
        _ => {}
    }
    let mut seen = BTreeSet::new();
    let mut memo = BTreeMap::new();
    for reference in &system.evidence_refs {
        if reference.record_id == request.record_id || !seen.insert(&reference.record_id) {
            return Err(rejected("self or duplicate evidence reference"));
        }
        let target = records
            .get(&reference.record_id)
            .ok_or_else(|| rejected("evidence not found in this collection"))?;
        if target.version != reference.version || target.routing_key != reference.routing_key {
            return Err(RecordError::Conflict(
                "evidence version or route mismatch".into(),
            ));
        }
        if target.system.is_none()
            || !visit(
                &reference.record_id,
                records,
                1,
                &mut BTreeSet::new(),
                &mut memo,
            )?
        {
            return Err(rejected("evidence is not visible typed content"));
        }
    }
    Ok(())
}

/// Build only a bounded ID dependency view; no text/index rebuild on deletion.
pub(super) fn descendants(
    records: &BTreeMap<String, Record>,
    root: &str,
) -> Result<BTreeSet<String>, RecordError> {
    let mut reverse: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (id, record) in records {
        if let Some(system) = &record.system {
            if system.evidence_refs.len() > MAX_REFERENCES {
                return Err(rejected("evidence reference budget exceeded"));
            }
            for reference in &system.evidence_refs {
                reverse.entry(&reference.record_id).or_default().push(id);
            }
        }
    }
    let mut affected = BTreeSet::new();
    let mut queue = vec![root];
    while let Some(id) = queue.pop() {
        if affected.insert(id.to_owned()) {
            if affected.len() > MAX_VISITED {
                return Err(rejected("evidence traversal budget exceeded"));
            }
            if let Some(children) = reverse.get(id) {
                queue.extend(children.iter().copied());
            }
        }
    }
    Ok(affected)
}
