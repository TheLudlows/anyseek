//! Exact inline source locations. No parser, remote fetch, or second content store.
use super::*;
use sha2::{Digest, Sha256};

/// Location in an immutable inline source revision. Offsets are bytes, not chars.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag="kind", rename_all="snake_case", deny_unknown_fields)]
pub enum EvidenceLocator {
    /// Half-open UTF-8 byte interval in one string-valued document field.
    Utf8 { /// Exact top-level source field.
        field:String, /// Inclusive byte offset, on a UTF-8 boundary.
        start:u64, /// Exclusive byte offset, on a UTF-8 boundary.
        end:u64 },
    /// One-based page in a source field containing caller-supplied text pages.
    Page { /// Exact top-level field containing an array of strings.
        field:String, /// One-based page; the engine never parses a PDF.
        page:u64 },
}
/// Protected exact source excerpt, tied to a current authorization/data view.
#[derive(Clone, Debug, Serialize)]
pub struct EvidenceExcerpt {
    /// Exact source revision and requested location.
    pub reference:EvidenceRef,
    /// Selected text (not a URL fetch or inferred original file).
    pub text:String,
    /// SHA-256 of the full selected source field or full selected page, UTF-8 bytes.
    pub content_sha256:String,
    /// Supported inline representation only, not a caller-asserted file MIME type.
    pub media_type:String,
    /// Common partition position.
    pub visible_seq:u64,
    /// Fixed UTC Unix milliseconds.
    pub evaluation_time_ms:u64,
}
fn reject(message:&str)->RecordError {RecordError::Rejected(message.into())}
fn selection<'a>(record:&'a Record,locator:&EvidenceLocator)->Result<(&'a str,&'a str),RecordError> {
    if !record.system.as_ref().is_some_and(|s|s.kind==EvidenceKind::Source) {return Err(reject("locators require an inline source revision"));}
    let (EvidenceLocator::Utf8{field,..}|EvidenceLocator::Page{field,..})=locator;
    if field.is_empty()||field.len()>256{return Err(reject("invalid locator field"));}
    let value=record.document.as_ref().and_then(|d|d.get(field)).ok_or_else(||reject("source locator field absent"))?;
    match locator {
        EvidenceLocator::Utf8{start,end,..}=>{
            let text=value.as_str().ok_or_else(||reject("UTF-8 locator requires a string field"))?;
            let start=usize::try_from(*start).map_err(|_|reject("locator offset overflow"))?;
            let end=usize::try_from(*end).map_err(|_|reject("locator offset overflow"))?;
            if start>=end || end>text.len() || !text.is_char_boundary(start) || !text.is_char_boundary(end){return Err(reject("invalid UTF-8 half-open byte range"));}
            Ok((text,&text[start..end]))
        }
        EvidenceLocator::Page{page,..}=>{
            let pages=value.as_array().ok_or_else(||reject("page locator requires inline text pages"))?;
            if pages.iter().any(|p|!p.is_string()){return Err(reject("source pages must all be strings"));}
            let index=page.checked_sub(1).and_then(|p|usize::try_from(p).ok()).ok_or_else(||reject("page must be one-based"))?;
            let text=pages.get(index).and_then(|p|p.as_str()).ok_or_else(||reject("page outside source page count"))?;
            Ok((text,text))
        }
    }
}
pub(super) fn validate(reference:&EvidenceRef,target:&Record)->Result<(),RecordError> {
    if let Some(locator)=&reference.locator {selection(target,locator)?;}Ok(())
}
/// Validate retained locations against their exact source body even when the
/// current source is newer/deleted. History is authority, never refetched.
pub(super) fn validate_history(state:&State)->Result<(),RecordError> {
    for record in state.records.values() {
        if let Some(system)=&record.system {
            for reference in &system.evidence_refs {
                if reference.locator.is_none(){continue;}
                validate_retained(state,reference)?;
            }
        }
    }Ok(())
}
pub(super) fn validate_retained(state:&State,reference:&EvidenceRef)->Result<(),RecordError> {
    if reference.locator.is_none(){return Ok(());}
    let target=state.records.get(&reference.record_id).filter(|r|r.version==reference.version)
        .or_else(||state.source_revisions.iter().find(|r|r.record_id==reference.record_id&&r.version==reference.version))
        .ok_or_else(||reject("missing located source revision"))?;
    validate(reference,target)
}
impl RecordPartition {
    /// Read an exact inline source excerpt. Current source must still be visible:
    /// retained history is not a bypass for deletion, TTL or recovery quarantine.
    pub async fn evidence_excerpt(&self,reference:EvidenceRef)->Result<EvidenceExcerpt,RecordError> {
        self.advance_time().await?;
        let _admission=self.admission.clone().try_acquire_owned().map_err(|_|RecordError::Busy)?;
        let inner=self.inner.lock().await;
        if !inner.healthy{return Err(RecordError::RecoveryRequired);}
        let current=inner.state.records.get(&reference.record_id).filter(|r|inner.locations.contains_key(&r.record_id))
            .ok_or_else(||RecordError::Conflict("source is not currently visible".into()))?;
        if !current.system.as_ref().is_some_and(|s|s.kind==EvidenceKind::Source) {return Err(reject("source record required"));}
        let target=if current.version==reference.version {current}else{inner.state.source_revisions.iter().find(|r|r.record_id==reference.record_id&&r.version==reference.version).ok_or_else(||RecordError::Conflict("source revision absent".into()))?};
        let locator=reference.locator.as_ref().ok_or_else(||reject("evidence excerpt requires locator"))?;
        let (whole,selected)=selection(target,locator)?;
        let content_sha256=Sha256::digest(whole.as_bytes()).iter().map(|b|format!("{b:02x}")).collect();
        Ok(EvidenceExcerpt{reference,text:selected.into(),content_sha256,media_type:"text/plain; charset=utf-8".into(),visible_seq:inner.state.seq,evaluation_time_ms:inner.evaluation_time_ms})
    }
}
