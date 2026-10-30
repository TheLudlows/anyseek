//! Immutable object names. Service partition identities are tenant/name/generation;
//! the generation belongs to object IDs, not an extra bucket namespace.

pub(crate) fn valid_partition(id: &str) -> bool {
    let parts: Vec<_> = id.split('/').collect();
    let component = |part: &&str| {
        !part.is_empty()
            && part
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    };
    (parts.len() == 1
        || (parts.len() == 3
            && parts[2].len() == 16
            && parts[2].bytes().all(|b| b.is_ascii_hexdigit())))
        && parts.iter().all(component)
}

pub(crate) fn key(partition: &str, category: &str, object: &str) -> String {
    match partition.rsplit_once('/') {
        Some((collection, generation)) => {
            format!("anyseek/{collection}/{category}/{generation}-{object}")
        }
        None => format!("anyseek/{partition}/{category}/{object}"),
    }
}

pub(crate) fn owns(partition: &str, object: &str) -> bool {
    match partition.rsplit_once('/') {
        Some((collection, generation)) => object
            .strip_prefix(&format!("anyseek/{collection}/"))
            .and_then(|tail| tail.split_once('/'))
            .is_some_and(|(_, id)| id.starts_with(&format!("{generation}-"))),
        None => object.starts_with(&format!("anyseek/{partition}/")),
    }
}

pub(crate) fn file_category(path: &str) -> &'static str {
    if path.split('/').any(|part| part == "native") {
        "lexical"
    } else if path.ends_with("/vectors.bin") {
        "vector"
    } else {
        "records"
    }
}
