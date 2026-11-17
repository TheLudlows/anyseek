//! Existing workers must read metadata without taking a schema writer lock.
use super::*;

#[test]
fn established_connections_open_while_another_writer_is_active() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("metadata.sqlite");
    let mut writer = SqliteAuthority::open(&path, Duration::ZERO).unwrap();
    let expected = writer.create_partition("partition", 1).unwrap();
    writer.connection.execute_batch("BEGIN IMMEDIATE").unwrap();
    for reader in [
        SqliteAuthority::open_existing(&path, Duration::ZERO),
        SqliteAuthority::open(&path, Duration::ZERO),
    ] {
        let reader = reader.unwrap();
        assert_eq!(
            reader.read_head("partition").unwrap(),
            Some(expected.clone())
        );
        let synchronous: i64 = reader
            .connection
            .pragma_query_value(None, "synchronous", |r| r.get(0))
            .unwrap();
        assert_eq!(synchronous, 2);
    }
    writer.connection.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn existing_connections_never_create_or_repair_authority_metadata() {
    let root = tempfile::tempdir().unwrap();
    let missing = root.path().join("absent.sqlite");
    assert!(SqliteAuthority::open_existing(&missing, Duration::ZERO).is_err());
    assert!(!missing.exists());

    let path = root.path().join("shared.sqlite");
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE catalog_marker(value TEXT); INSERT INTO catalog_marker VALUES('keep');",
        )
        .unwrap();
    assert!(SqliteAuthority::open_existing(&path, Duration::ZERO).is_err());
    let authority = SqliteAuthority::open(&path, Duration::ZERO).unwrap();
    assert_eq!(
        authority
            .connection
            .query_row("SELECT value FROM catalog_marker", [], |r| r
                .get::<_, String>(0))
            .unwrap(),
        "keep"
    );
    drop(authority);
    connection
        .execute_batch("DROP TABLE operation_receipts")
        .unwrap();
    assert!(SqliteAuthority::open_existing(&path, Duration::ZERO).is_err());
    assert!(SqliteAuthority::open(&path, Duration::ZERO).is_err());
    assert!(
        connection
            .prepare("SELECT * FROM operation_receipts")
            .is_err()
    );

    let path = root.path().join("unsupported.sqlite");
    let authority = SqliteAuthority::open(&path, Duration::ZERO).unwrap();
    authority
        .connection
        .execute("UPDATE authority_schema SET version=99", [])
        .unwrap();
    assert!(SqliteAuthority::open_existing(&path, Duration::ZERO).is_err());
    assert!(SqliteAuthority::open(&path, Duration::ZERO).is_err());
    let version: i64 = authority
        .connection
        .query_row("SELECT version FROM authority_schema", [], |r| r.get(0))
        .unwrap();
    assert_eq!(version, 99);

    let path = root.path().join("wrong-journal-mode.sqlite");
    drop(SqliteAuthority::open(&path, Duration::ZERO).unwrap());
    let connection = Connection::open(&path).unwrap();
    connection
        .pragma_update(None, "journal_mode", "DELETE")
        .unwrap();
    assert!(SqliteAuthority::open_existing(&path, Duration::ZERO).is_err());
    let mode: String = connection
        .pragma_query_value(None, "journal_mode", |r| r.get(0))
        .unwrap();
    assert_eq!(mode, "delete");
}

#[test]
fn reservation_observations_never_override_later_retirement() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("metadata.sqlite");
    let mut first = SqliteAuthority::open(&path, Duration::ZERO).unwrap();
    let object = ObjectRef {
        key: "reserved-object".into(),
        sha256: [7; 32],
        length: 128,
    };
    assert!(!first.reserve_upload(&object, 1).unwrap());
    first.register_durable_object(&object, 2).unwrap();
    assert!(first.reserve_upload(&object, 3).unwrap());
    let conflicting = ObjectRef {
        sha256: [8; 32],
        ..object.clone()
    };
    assert!(first.reserve_upload(&conflicting, 3).is_err());
    let mut other = SqliteAuthority::open_existing(&path, Duration::ZERO).unwrap();
    assert!(other.retire_object(&object.key, 1, 4).unwrap());
    assert!(first.register_durable_object(&object, 5).is_err());
    assert!(first.reserve_upload(&object, 6).is_err());
    assert!(!first.is_durable_object(&object).unwrap());
}
