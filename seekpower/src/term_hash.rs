//! Index-format-8 term keys. This module must not use Rust's `Hash` encoding or
//! architecture-selected hashing. Input is the tokenizer's exact UTF-8 bytes,
//! with no length prefix, terminator, or additional Unicode normalization.
//! XXH64 uses its specified little-endian lane decoding on every target.

const SEGMENT_SEED: u64 = 0x9e37_79b1_85eb_ca87;
const TERM_SEED: u64 = 0;

#[inline]
pub(crate) fn segment_hash(bytes: &[u8]) -> u32 {
    xxhash_rust::xxh64::xxh64(bytes, SEGMENT_SEED) as u32
}

#[inline]
pub(crate) fn term_key(bytes: &[u8]) -> u64 {
    // Keep the existing low-three-bit n-gram tag representation.
    xxhash_rust::xxh64::xxh64(bytes, TERM_SEED) & !7
}

#[cfg(test)]
mod tests {
    use super::*;

    // Fixed vectors generated with the independent C libxxhash reference,
    // XXH_versionNumber = 803. No reference library is needed to run tests.
    #[test]
    fn persistent_key_vectors() {
        let cases: &[(&str, u32, u64)] = &[
            ("", 0x61c7e7a7, 0xef46db3751d8e998),
            ("alpha", 0xda6f4c80, 0xc758e1011dda5848),
            ("中国", 0x463dcb23, 0x542d47d6954e3f08),
            ("中华人民共和国", 0xed954d37, 0xa4fbcd6f198f9570),
            ("é", 0xbbc3e3a2, 0x17d757dfb8b46f78),
            ("e\u{301}", 0x41ed6b8d, 0x739d9d425d7073c8),
            ("🦀検索", 0xf705f463, 0x505310c8d1477728),
            ("a\0b", 0xcfc2acf6, 0xb51b25d68d1338c0),
            (&"a".repeat(31), 0xeeca75d9, 0xfe47067cda802910),
            (&"a".repeat(32), 0x79c0e494, 0x856e843298f99ad0),
            (&"a".repeat(33), 0xe3ae9d31, 0x18f3ff0c21e3b248),
            (&"长词项".repeat(1024), 0xb1f4af94, 0xc00ddff51f360fa0),
        ];
        for &(term, segment, key) in cases {
            assert_eq!(segment_hash(term.as_bytes()), segment, "segment: {term:?}");
            assert_eq!(term_key(term.as_bytes()), key, "term: {term:?}");
            assert_eq!(key & 7, 0);
        }
        // The tokenizer owns normalization; hashing must not silently normalize.
        assert_ne!(term_key("é".as_bytes()), term_key("e\u{301}".as_bytes()));
    }
}
