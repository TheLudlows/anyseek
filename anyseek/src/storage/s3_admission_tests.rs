//! Deterministic local admission boundaries; not real-cloud acceptance evidence.
use super::*;
use std::time::Duration;

fn scope() -> Scope {
    Arc::new(resources::Governor::default()).admit("s3-admission".into(), Lane::Background, 0).unwrap()
}
fn planned(key: &str, bytes: &[u8]) -> ObjectRef {
    ObjectRef { key: key.into(), sha256: Sha256::digest(bytes).into(), length: bytes.len() as u64 }
}

#[tokio::test]
async fn io_refusal_precedes_registered_put_and_preserves_head() {
    let directory = tempfile::tempdir().unwrap();
    let mut authority = SqliteAuthority::open(&directory.path().join("authority.sqlite"), Duration::from_secs(1)).unwrap();
    let head = authority.create_partition("test", 1).unwrap();
    let backend = Arc::new(object_store::memory::InMemory::new());
    let store = S3CompatibleStore::from_store(backend.clone(), 4096);
    let key = format!("partitions/test/snapshot/{:020}-build/manifest", head.head_version + 1);
    let reference = planned(&key, b"candidate");
    let scope = scope();
    scope.clone().run(async {
        let held = scope.io().unwrap();
        assert!(matches!(store.put_and_register(&mut authority, &key, b"candidate", 0).await, Err(UploadRegisterError::Admission(_))));
        assert!(matches!(backend.get(&ObjectPath::from(key.clone())).await, Err(object_store::Error::NotFound { .. })));
        assert_eq!(authority.read_head("test").unwrap().unwrap(), head);
        // Refused work has not started an upload or mutated the authoritative head.
        assert!(!authority.is_durable_object(&reference).unwrap());
        drop(held);
        assert_eq!(store.put_and_register(&mut authority, &key, b"candidate", 1).await.unwrap(), reference);
    }).await;
}

#[tokio::test]
async fn verified_buffer_releases_io_and_delete_refusal_keeps_retirement() {
    let directory = tempfile::tempdir().unwrap();
    let mut authority = SqliteAuthority::open(&directory.path().join("authority.sqlite"), Duration::from_secs(1)).unwrap();
    authority.create_partition("test", 1).unwrap();
    let backend = Arc::new(object_store::memory::InMemory::new());
    let store = S3CompatibleStore::from_store(backend.clone(), 4096);
    let scope = scope();
    scope.clone().run(async {
        let reference = store.put_and_register(&mut authority, "test/orphan", b"body", 0).await.unwrap();
        let buffer = store.get_verified(&reference).await.unwrap();
        let held = scope.io().unwrap(); // Returned Buffer retains bytes, never I/O capacity.
        assert_eq!(&buffer[..], b"body");
        assert!(authority.retire_object(&reference.key, 1, 1).unwrap());
        assert!(matches!(store.delete_retired(&mut authority, &reference.key, 1).await, Err(UploadRegisterError::Admission(_))));
        assert!(authority.collectible_objects(1).unwrap().contains(&reference));
        assert!(backend.get(&ObjectPath::from(reference.key.clone())).await.is_ok());
        drop(held);
        assert!(store.delete_retired(&mut authority, &reference.key, 2).await.unwrap());
        assert!(!authority.collectible_objects(2).unwrap().contains(&reference));
        assert!(matches!(backend.get(&ObjectPath::from(reference.key.clone())).await, Err(object_store::Error::NotFound { .. })));
    }).await;
}

#[tokio::test]
async fn http_put_success_then_readback_failure_is_not_admission_refusal() {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let server = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut methods = Vec::new();
        while methods.len() < 2 {
            let (mut stream, _) = match listener.accept() {
                Ok(value) => value,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(std::time::Instant::now() < deadline, "fixture request deadline");
                    std::thread::sleep(Duration::from_millis(5)); continue;
                }
                Err(error) => panic!("fixture accept: {error}"),
            };
            // Windows inherits nonblocking mode from the listener.
            stream.set_nonblocking(false).unwrap();
            stream.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
            let mut bytes = Vec::new();
            let end = loop {
                let mut chunk = [0; 4096];
                let count = stream.read(&mut chunk).unwrap(); assert!(count > 0);
                bytes.extend_from_slice(&chunk[..count]);
                if let Some(position) = bytes.windows(4).position(|value| value == b"\r\n\r\n") { break position + 4; }
                assert!(bytes.len() < 65536);
            };
            let headers = String::from_utf8_lossy(&bytes[..end]);
            let method = headers.split_whitespace().next().unwrap().to_owned();
            let length = headers.lines().find_map(|line| line.split_once(':').and_then(|(key, value)|
                key.eq_ignore_ascii_case("content-length").then(|| value.trim().parse::<usize>().unwrap()))).unwrap_or(0);
            while bytes.len() < end + length {
                let mut chunk = [0; 4096]; let count = stream.read(&mut chunk).unwrap(); assert!(count > 0);
                bytes.extend_from_slice(&chunk[..count]);
            }
            if method == "PUT" {
                assert!(length > 0);
                stream.write_all(b"HTTP/1.1 200 OK\r\nETag: \"accepted\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
            } else {
                assert_eq!(method, "GET");
                stream.write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
            }
            methods.push(method);
        }
        methods
    });
    let backend = AmazonS3Builder::new().with_endpoint(format!("http://{address}")).with_bucket_name("fixture")
        .with_region("local").with_access_key_id("fixture").with_secret_access_key("fixture")
        .with_allow_http(true).with_retry(object_store::RetryConfig { max_retries: 0, ..Default::default() }).build().unwrap();
    let store = S3CompatibleStore::from_store(Arc::new(backend), 4096);
    let directory = tempfile::tempdir().unwrap();
    let mut authority = SqliteAuthority::open(&directory.path().join("authority.sqlite"), Duration::from_secs(1)).unwrap();
    let head = authority.create_partition("test", 1).unwrap();
    let key = "partitions/test/wal/00000000000000000001-fixture";
    let reference = planned(key, b"accepted remote bytes");
    let error = store.put_and_register(&mut authority, key, b"accepted remote bytes", 0).await.unwrap_err();
    assert!(matches!(error, UploadRegisterError::ObjectStore(ref error) if !is_resource_error(error)));
    assert!(matches!(crate::storage::object_log::TailError::from(error), crate::storage::object_log::TailError::ObjectStore(_)));
    assert_eq!(server.join().unwrap(), ["PUT", "GET"]);
    assert_eq!(authority.read_head("test").unwrap().unwrap(), head);
    assert!(!authority.is_durable_object(&reference).unwrap());
    assert!(authority.retire_object(key, 0, 1).unwrap());
    assert!(authority.mark_object_deleted(&reference).unwrap());
    assert!(authority.collectible_objects(2).unwrap().contains(&reference), "unknown PUT identity must remain repeatably collectible");
}
