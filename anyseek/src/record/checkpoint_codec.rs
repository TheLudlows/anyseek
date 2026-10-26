//! Stream the existing checkpoint representation without cloning State or
//! allocating a complete JSON buffer merely to count or authenticate it.
use super::*;
use sha2::{Digest, Sha256};
use std::io::{self, Write};

#[derive(Serialize)]
enum BorrowedEntry<'a> {
    #[serde(rename = "RecordCheckpointV14")]
    Checkpoint {
        config: &'a RecordConfig,
        state: &'a State,
    },
}

pub(super) fn length(config: &RecordConfig, state: &State) -> Result<usize, RecordError> {
    json_length(&BorrowedEntry::Checkpoint { config, state })
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
        let state_bytes = serde_json::to_vec(&inner.state).unwrap();
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
