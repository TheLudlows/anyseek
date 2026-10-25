//! Bounded, same-partition evidence semantics. Authority is the metadata on each
//! Record, never a second journal or an independently committed dependency graph.
use super::{ExpectedVersion, Record, RecordError, RecordWrite};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

const MAX_REFERENCES: usize = 32;
const MAX_DEPTH: usize = 16;
const MAX_VISITED: usize = super::capacity::MAX_RECORDS;

/// Versioned original source or immutable derived content.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceKind {
    /// Caller-supplied original content; parsing is an upper-layer concern.
    Source,
    /// Caller-supplied content supported by precise source/derived references.
    Derived,
    /// Explicit caller-declared entity, without model resolution.
    Entity,
    /// Explicit directed edge with separately declared evidence.
    Relation,
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
    /// Optional exact inline source location; never interpreted on ordinary endpoints.
    #[serde(default, skip_serializing_if="Option::is_none")]
    pub locator: Option<super::EvidenceLocator>,
}
/// Engine-validated metadata, distinct from arbitrary business document fields.
/// Sources support exact-version updates; derived recomputation uses a new
/// identity. Generations, TTL and feed are separate implementation slices.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EvidenceMetadata {
    /// Source or derived record.
    pub kind: EvidenceKind,
    /// Required only for explicit relation records.
    #[serde(default, skip_serializing_if="Option::is_none")]
    pub relation: Option<super::RelationMetadata>,
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
        let mut visible = record.document.is_some() && !record.retired_generation;
        if let Some(system) = &record.system {
            super::graph::validate_metadata(system)?;
            if system.evidence_refs.len() > MAX_REFERENCES {
                return Err(rejected("evidence reference budget exceeded"));
            }
            for (reference,typed) in system.evidence_refs.iter().map(|r|(r,true)).chain(super::graph::endpoints(system).map(|r|(r,false))) {
                let matches = records.get(&reference.record_id).is_some_and(|target| {
                    (target.system.is_some() || !typed)
                        && (typed || target.system.as_ref().is_none_or(|s|s.kind!=EvidenceKind::Relation))
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
    if let Some(system)=&request.system {super::graph::validate_metadata(system)?;}
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
            let previous = old.unwrap();
            if previous.document.is_none() { return Err(rejected("deleted evidence cannot be recreated")); }
            if !matches!(previous.system.as_ref().unwrap().kind,EvidenceKind::Source|EvidenceKind::Entity|EvidenceKind::Relation) {
                return Err(rejected("derived recomputation requires a new identity and current evidence"));
            }
            if !matches!(request.expected, ExpectedVersion::Match(v) if v == previous.version) {
                return Err(RecordError::Conflict("source update requires exact current version".into()));
            }
            if request.system.as_ref().is_some_and(|system| Some(system) != previous.system.as_ref()) {
                return Err(rejected("source metadata kind/dependencies cannot change"));
            }
            if previous.system.as_ref().unwrap().kind!=EvidenceKind::Source && !visit(&request.record_id,records,0,&mut BTreeSet::new(),&mut BTreeMap::new())? {
                return Err(rejected("hidden declaration requires a new identity and current references"));
            }
            return Ok(());
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
    super::graph::validate_new(records,system)?;
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
        super::locator::validate(reference,target)?;
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
            for reference in system.evidence_refs.iter().chain(super::graph::endpoints(system)) {
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


pub(super) fn retained_revision(record: &Record) -> bool {
    record.document.is_some() && record.system.as_ref().is_some_and(|s| s.kind == EvidenceKind::Source)
}


/// Visibility reason derived from current authoritative versions, never supplied
/// or cleared by clients. Deleted ancestry always overrides stale ancestry.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceVisibility {
    /// The record contributes to every applicable retrieval channel.
    Visible,
    /// Some exact evidence version has been superseded; default reads hide it.
    Stale,
    /// This record or an evidence ancestor was explicitly deleted.
    Deleted,
    /// This record or its ancestry reached an irreversible TTL deadline.
    Expired,
    /// Missing or invalid evidence; fail closed.
    Unavailable,
    /// Independent restored evidence lacks current deletion authority.
    Quarantined,
}
pub(super) fn status(records: &BTreeMap<String, Record>, id: &str) -> Result<EvidenceVisibility, RecordError> {
    use EvidenceVisibility::*;
    if visit(id, records, 0, &mut BTreeSet::new(), &mut BTreeMap::new())? { return Ok(Visible); }
    let mut seen = BTreeSet::new(); let mut queue = vec![id];
    let mut stale = false; let mut unavailable = false; let mut deleted = false; let mut expired = false;
    while let Some(id) = queue.pop() {
        if !seen.insert(id) { continue; }
        if seen.len() > MAX_VISITED { return Err(rejected("evidence traversal budget exceeded")); }
        let Some(record) = records.get(id) else { unavailable = true; continue; };
        deleted |= record.document.is_none() && record.expired_at_ms.is_none();
        expired |= record.expired_at_ms.is_some();
        stale |= record.retired_generation;
        if let Some(system) = &record.system {
            for (reference,typed) in system.evidence_refs.iter().map(|r|(r,true)).chain(super::graph::endpoints(system).map(|r|(r,false))) {
                match records.get(&reference.record_id) {
                    Some(target) if (target.system.is_some() || !typed) && target.routing_key == reference.routing_key => stale |= target.version != reference.version,
                    _ => unavailable = true,
                }
                queue.push(&reference.record_id);
            }
        }
    }
    Ok(if deleted { Deleted } else if expired { Expired } else if unavailable { Unavailable } else if stale { Stale } else { Unavailable })
}

pub(super) fn validate_history(records: &BTreeMap<String, Record>, history: &[Record]) -> Result<(), RecordError> {
    let mut seen = BTreeSet::new();
    let mut counts = BTreeMap::<&str, u64>::new();
    for revision in history {
        let Some(current) = records.get(&revision.record_id) else { return Err(rejected("orphan source revision")); };
        if !retained_revision(revision) || revision.version == 0 || revision.version >= current.version
            || revision.routing_key != current.routing_key || revision.system != current.system
            || !seen.insert((&revision.record_id, revision.version)) {
            return Err(rejected("inconsistent source revision history"));
        }
        *counts.entry(&revision.record_id).or_default() += 1;
    }
    for record in records.values().filter(|r| r.system.as_ref().is_some_and(|s| s.kind == EvidenceKind::Source)) {
        if counts.get(record.record_id.as_str()).copied().unwrap_or(0) != record.version.saturating_sub(1) {
            return Err(rejected("incomplete source revision history"));
        }
    }
    Ok(())
}

/// An edge adds a visibility dependency level beyond its endpoint's own graph.
pub(super) fn endpoint_visible(records:&BTreeMap<String,Record>,id:&str)->Result<bool,RecordError> {
    visit(id,records,1,&mut BTreeSet::new(),&mut BTreeMap::new())
}
