//! Immutable object-store adapter for S3-compatible services.
//!
//! Object creation is conditional (never overwrite), then read back and verify the
//! bytes before returning an ObjectRef. Callers may register that reference with
//! SQLite only after this method succeeds. This module deliberately exposes no
//! delete method; deletion requires the authority/backup/pin reachability protocol.
use super::sqlite_authority::ObjectRef;
use object_store::{
    aws::AmazonS3Builder,
    path::Path as ObjectPath,
    ObjectStore, PutMode, PutOptions, PutPayload,
};
use sha2::{Digest, Sha256};
use std::{fmt, sync::Arc};
use super::sqlite_authority::SqliteAuthority;

/// Error from the object-first persistence stage.
#[derive(Debug)]
pub(crate) enum UploadRegisterError {
    ObjectStore(object_store::Error),
    Metadata(rusqlite::Error),
}
impl fmt::Display for UploadRegisterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ObjectStore(error) => write!(f, "object-store operation failed: {error}"),
            Self::Metadata(error) => write!(f, "SQLite object registration failed: {error}"),
        }
    }
}
impl std::error::Error for UploadRegisterError {}


/// S3-compatible immutable object store with write-after-read verification.
pub(crate) struct S3CompatibleStore {
    store: Arc<dyn ObjectStore>,
    max_object_bytes: usize,
}

impl S3CompatibleStore {
    /// Build an S3-compatible client. Endpoint, bucket and credentials are supplied
    /// by deployment configuration; credentials must never be persisted in SQLite.
    pub(crate) fn connect(
        endpoint: &str,
        bucket: &str,
        region: &str,
        access_key_id: &str,
        secret_access_key: &str,
        allow_http: bool,
        max_object_bytes: usize,
    ) -> object_store::Result<Self> {
        let store = AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_bucket_name(bucket)
            .with_region(region)
            .with_access_key_id(access_key_id)
            .with_secret_access_key(secret_access_key)
            .with_allow_http(allow_http)
            .build()?;
        Ok(Self { store: Arc::new(store), max_object_bytes })
    }

    fn object_path(key: &str) -> object_store::Result<ObjectPath> {
        if key.is_empty() || key.starts_with('/') || key.contains('\\') {
            return Err(object_store::Error::InvalidPath {
                source: "object key must be a non-empty relative slash-separated key".into(),
            });
        }
        ObjectPath::parse(key)
    }

    /// Conditionally create a new object, then read it back and verify exact bytes.
    /// A pre-existing key is an error even if the caller believes the bytes match;
    /// callers must resolve retries by deterministic key plus metadata, not overwrite.
    pub(crate) async fn put_verified(
        &self,
        key: &str,
        bytes: &[u8],
    ) -> object_store::Result<ObjectRef> {
        if bytes.len() > self.max_object_bytes {
            return Err(object_store::Error::Generic {
                store: "s3-compatible",
                source: format!("object size {} exceeds configured limit {}", bytes.len(), self.max_object_bytes).into(),
            });
        }
        let location = Self::object_path(key)?;
        let digest: [u8; 32] = Sha256::digest(bytes).into();
        let put_result = self.store.put_opts(
            &location,
            PutPayload::from(bytes.to_vec()),
            PutOptions { mode: PutMode::Create, ..Default::default() },
        ).await;

        // If the response was lost after the service committed the PUT, a retry
        // may observe an existing key. Resolve that ambiguity only by reading
        // the existing bytes and proving they are byte-identical.
        if let Err(put_error) = put_result {
            match self.store.get(&location).await {
                Ok(result) => {
                    let existing = result.bytes().await?;
                    let existing_digest: [u8; 32] = Sha256::digest(&existing).into();
                    if existing.len() == bytes.len()
                        && existing_digest == digest
                        && existing.as_ref() == bytes {
                        return Ok(ObjectRef { key: key.to_owned(), sha256: digest, length: bytes.len() as u64 });
                    }
                    return Err(put_error);
                }
                Err(_) => return Err(put_error),
            }
        }

        // Successful PUT alone is not enough: verify visibility and exact content
        // through the read path used by later readers before publishing metadata.
        let downloaded = self.store.get(&location).await?.bytes().await?;
        let actual: [u8; 32] = Sha256::digest(&downloaded).into();
        if downloaded.len() != bytes.len() || actual != digest || downloaded.as_ref() != bytes {
            return Err(object_store::Error::Generic {
                store: "s3-compatible",
                source: "read-after-write verification failed".into(),
            });
        }
        Ok(ObjectRef { key: key.to_owned(), sha256: digest, length: bytes.len() as u64 })
    }

    /// Execute the object-first stage and register the verified reference in SQLite.
    /// This intentionally does not publish a committed sequence; the caller must
    /// publish the reference and operation receipt in a separate SQLite transaction.
    pub(crate) async fn put_and_register(
        &self,
        authority: &mut SqliteAuthority,
        key: &str,
        bytes: &[u8],
        created_at_ms: i64,
    ) -> Result<ObjectRef, UploadRegisterError> {
        let reference = self.put_verified(key, bytes).await.map_err(UploadRegisterError::ObjectStore)?;
        authority.register_durable_object(&reference, created_at_ms)
            .map_err(UploadRegisterError::Metadata)?;
        Ok(reference)
    }

    /// Fetch and verify an object against an authority-published reference.
    pub(crate) async fn get_verified(
        &self,
        reference: &ObjectRef,
    ) -> object_store::Result<Vec<u8>> {
        if reference.length > self.max_object_bytes as u64 {
            return Err(object_store::Error::Generic {
                store: "s3-compatible",
                source: "object reference exceeds configured size limit".into(),
            });
        }
        let location = Self::object_path(&reference.key)?;
        let bytes = self.store.get(&location).await?.bytes().await?;
        let digest: [u8; 32] = Sha256::digest(&bytes).into();
        if bytes.len() as u64 != reference.length || digest != reference.sha256 {
            return Err(object_store::Error::Generic {
                store: "s3-compatible",
                source: "object length or SHA-256 does not match committed reference".into(),
            });
        }
        Ok(bytes.to_vec())
    }

    #[cfg(test)]
    fn from_store(store: Arc<dyn ObjectStore>, max_object_bytes: usize) -> Self {
        Self { store, max_object_bytes }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn create_then_read_back_and_verify() {
        let store = S3CompatibleStore::from_store(Arc::new(InMemory::new()), 1024);
        let body = b"durable record bytes";
        let reference = store.put_verified("partition/p0/log/0001", body).await.unwrap();
        assert_eq!(reference.length, body.len() as u64);
        assert_eq!(store.get_verified(&reference).await.unwrap(), body);
    }

    #[tokio::test]
    async fn immutable_key_and_size_limit_are_enforced() {
        let store = S3CompatibleStore::from_store(Arc::new(InMemory::new()), 8);
        let reference = store.put_verified("segments/s1", b"12345678").await.unwrap();
        assert_eq!(store.get_verified(&reference).await.unwrap(), b"12345678");
        assert!(store.put_verified("segments/s1", b"different").await.is_err());
        assert!(store.put_verified("segments/too-large", b"123456789").await.is_err());
        let wrong = ObjectRef { sha256: [0; 32], ..reference };
        assert!(store.get_verified(&wrong).await.is_err());
    }
}
