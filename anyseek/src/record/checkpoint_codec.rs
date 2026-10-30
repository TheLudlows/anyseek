//! Stream the existing checkpoint representation without cloning State or
//! allocating a complete JSON buffer merely to count or authenticate it.
use super::*;
use sha2::{Digest, Sha256};
use std::io::{self, Write};

#[derive(Serialize)]
enum BorrowedEntry<'a, S: ?Sized> {
    #[serde(rename = "RecordCheckpointV15")]
    Checkpoint {
        config: &'a RecordConfig,
        state: &'a S,
    },
}

pub(super) fn length(config: &RecordConfig, state: &State) -> Result<usize, RecordError> {
    // Shared immutable payloads were measured when admitted. Only serialize the
    // small envelope and metadata; counting a checkpoint must not traverse all
    // documents and retained request bodies under the foreground Record guard.
    let state_length = super::state_proof::length(state)?;
    json_length(&BorrowedEntry::Checkpoint { config, state: &() })?
        .checked_sub(4)
        .and_then(|n| n.checked_add(state_length))
        .ok_or_else(|| storage("checkpoint length overflow"))
}

/// Attempt-local encoding evidence. The borrow prevents changing this State
/// while the evidence is reused; no seq-keyed or cross-publication cache exists.
pub(super) struct StateEncoding<'a> {
    state: &'a State,
    length: usize,
    digest: [u8; 32],
}
impl<'a> StateEncoding<'a> {
    pub(super) fn new(state: &'a State) -> Result<Self, RecordError> {
        let (length, digest) = json_measure(state)?;
        Ok(Self { state, length, digest })
    }
    pub(super) fn state(&self) -> &'a State { self.state }
    pub(super) fn length(&self) -> usize { self.length }
    pub(super) fn digest(&self) -> [u8; 32] { self.digest }
    pub(super) fn checkpoint_length(&self, config: &RecordConfig) -> Result<usize, RecordError> {
        // Serialize the identical envelope with JSON null in the State position.
        // Replacing its four bytes preserves exact config escaping and framing.
        json_length(&BorrowedEntry::Checkpoint { config, state: &() })?
            .checked_sub(4).and_then(|n| n.checked_add(self.length))
            .ok_or_else(|| storage("checkpoint length overflow"))
    }
}
struct DigestCounter { length: usize, digest: Sha256 }
impl Write for DigestCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.length = self.length.checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("checkpoint length overflow"))?;
        self.digest.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}
/// One canonical serialization, no complete encoded buffer or cloned State.
fn json_measure(value: &impl Serialize) -> Result<(usize, [u8; 32]), RecordError> {
    let mut output = DigestCounter { length: 0, digest: Sha256::new() };
    serde_json::to_writer(&mut output, value).map_err(storage)?;
    Ok((output.length, output.digest.finalize().into()))
}

pub(super) fn json_length(value: &impl Serialize) -> Result<usize, RecordError> {
    struct Counter(usize);
    impl Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| io::Error::other("checkpoint length overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut count = Counter(0);
    serde_json::to_writer(&mut count, value).map_err(storage)?;
    Ok(count.0)
}

/// Count only up to the persisted bound. This allocates no encoded buffer and
/// distinguishes exceeding the bound from an unrelated serialization failure.
pub(super) fn json_fits(value: &impl Serialize, limit: u64) -> Result<bool, RecordError> {
    struct Bounded { remaining:u64, exceeded:bool }
    impl Write for Bounded {
        fn write(&mut self, bytes:&[u8])->io::Result<usize> {
            let length=u64::try_from(bytes.len()).map_err(io::Error::other)?;
            if length>self.remaining {
                self.exceeded=true;
                return Err(io::Error::other("metadata byte limit reached"));
            }
            self.remaining-=length;
            Ok(bytes.len())
        }
        fn flush(&mut self)->io::Result<()> {Ok(())}
    }
    let mut counter=Bounded {remaining:limit,exceeded:false};
    match serde_json::to_writer(&mut counter,value) {
        Ok(())=>Ok(true),
        Err(_) if counter.exceeded=>Ok(false),
        Err(error)=>Err(storage(error)),
    }
}

pub(super) fn digest(config: &RecordConfig, state: &State) -> Result<[u8; 32], RecordError> {
    json_digest(&BorrowedEntry::Checkpoint { config, state })
}

pub(super) fn json_digest(value: &impl Serialize) -> Result<[u8; 32], RecordError> {
    struct Hasher(Sha256);
    impl Write for Hasher {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut digest = Hasher(Sha256::new());
    serde_json::to_writer(&mut digest, value).map_err(storage)?;
    Ok(digest.0.finalize().into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn measurement_uses_one_serialization_and_preserves_errors_and_overflow() {
        struct Counted<'a>(&'a std::cell::Cell<usize>);
        impl Serialize for Counted<'_> {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                self.0.set(self.0.get() + 1);
                serializer.serialize_str("繁體 \\\"\n\0")
            }
        }
        let calls = std::cell::Cell::new(0);
        let value = Counted(&calls);
        let (length, digest) = json_measure(&value).unwrap();
        assert_eq!(calls.get(), 1, "length and SHA must share one serialization");
        let bytes = serde_json::to_vec(&value).unwrap();
        assert_eq!(length, bytes.len());
        assert_eq!(digest, <[u8; 32]>::from(Sha256::digest(&bytes)));
        struct Invalid;
        impl Serialize for Invalid {
            fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("deliberate measurement error"))
            }
        }
        assert!(json_measure(&Invalid).is_err());
        let mut counter = DigestCounter { length: usize::MAX, digest: Sha256::new() };
        assert!(counter.write(b"x").is_err());
        assert_eq!(counter.length, usize::MAX);
    }
    #[test]
    fn encoding_is_bound_to_full_state_not_business_sequence() {
        let mut first = State::default();
        first.time_watermark_ms = 1;
        let mut second = State::default();
        second.time_watermark_ms = 2;
        let a = StateEncoding::new(&first).unwrap();
        let b = StateEncoding::new(&second).unwrap();
        assert_eq!(first.seq, second.seq);
        assert_eq!(a.length(), b.length());
        assert_ne!(a.digest(), b.digest(), "same-seq maintenance is a different cut");
        assert!(std::ptr::eq(a.state(), &first));
        assert!(std::ptr::eq(b.state(), &second));
        let config = super::super::tests::config();
        for encoded in [&a, &b] {
            assert_eq!(encoded.checkpoint_length(&config).unwrap(), length(&config, encoded.state()).unwrap());
        }
    }
    #[test]
    fn bounded_json_counts_exact_limit_escaped_references_and_errors() {
        for value in [
            serde_json::json!({"reference":"anchor-01234567890123456789012345678901/state-0000"}),
            serde_json::json!({"reference":"\u{1}\\\"繁體".repeat(4096)}),
        ] {
            let bytes=serde_json::to_vec(&value).unwrap().len() as u64;
            assert!(json_fits(&value,bytes).unwrap());
            assert!(!json_fits(&value,bytes-1).unwrap());
            assert!(json_fits(&value,bytes+1).unwrap());
            assert!(json_fits(&value,u64::MAX).unwrap());
            assert!(!json_fits(&value,0).unwrap());
        }
        struct Invalid;
        impl Serialize for Invalid {
            fn serialize<S:serde::Serializer>(&self,_:S)->Result<S::Ok,S::Error>{
                Err(serde::ser::Error::custom("deliberate serialization failure"))
            }
        }
        assert!(json_fits(&Invalid,u64::MAX).is_err());
    }
    #[tokio::test]
    async fn borrowed_checkpoint_matches_existing_wire_bytes_and_digest() {
        let root = tempfile::tempdir().unwrap();
        let p = RecordPartition::create(
            &root.path().join("partition"),
            super::super::tests::config(),
        )
        .await
        .unwrap();
        for i in 0..3 {
            p.write(super::super::tests::write(
                &format!("op-{i}"),
                &format!("id-{i}"),
                "繁體 alpha \\\"\n\0 beta",
                ExpectedVersion::Absent,
            ))
            .await
            .unwrap();
        }
        let inner = p.inner.lock().await;
        assert_eq!(inner.state.operation_lookup.len(), 3);
        let owned = encode_entry(&Entry::Checkpoint {
            config: inner.config.clone(),
            state: inner.state.clone(),
        })
        .unwrap();
        let borrowed = serde_json::to_vec(&BorrowedEntry::Checkpoint {
            config: &inner.config,
            state: &inner.state,
        })
        .unwrap();
        assert_eq!(
            borrowed, owned,
            "wire format and insertion order must not change"
        );
        assert_eq!(length(&inner.config, &inner.state).unwrap(), owned.len());
        assert_eq!(
            digest(&inner.config, &inner.state).unwrap(),
            <[u8; 32]>::from(Sha256::digest(&owned))
        );
        let encoding = StateEncoding::new(&inner.state).unwrap();
        assert_eq!(encoding.checkpoint_length(&inner.config).unwrap(), owned.len());
        let state_bytes = serde_json::to_vec(&inner.state).unwrap();
        assert_eq!(encoding.length(), state_bytes.len());
        assert_eq!(encoding.digest(), <[u8; 32]>::from(Sha256::digest(&state_bytes)));
        assert_eq!(
            json_digest(&inner.state).unwrap(),
            <[u8; 32]>::from(Sha256::digest(&state_bytes))
        );
    }
}

/// The logical state is chunked, but any reserved future business event must
/// still fit one journal frame. Keep that conservative pre-WAL guarantee.
pub(super) fn validate_size(bytes: usize, reserve: usize, budget: u64) -> std::io::Result<()> {
    let total = bytes
        .checked_add(reserve)
        .ok_or_else(|| std::io::Error::other("state reserve overflow"))?;
    PartitionJournal::validate_checkpoint_budget(total, budget)?;
    PartitionJournal::validate_initial(reserve, budget)
}
