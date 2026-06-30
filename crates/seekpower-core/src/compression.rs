use seekpower_schema::DocCompression;

pub fn compress_document(json: &[u8], compression: &DocCompression) -> Vec<u8> {
    match compression {
        DocCompression::None => json.to_vec(),
        DocCompression::Snappy => snap::raw::Encoder::new().compress_vec(json).unwrap_or_else(|_| json.to_vec()),
        DocCompression::Lz4 => lz4_flex::compress(json),
        DocCompression::Zstd => zstd::bulk::compress(json, 3).unwrap_or_else(|_| json.to_vec()),
    }
}

pub fn decompress_document(data: &[u8], compression: &DocCompression) -> Vec<u8> {
    match compression {
        DocCompression::None => data.to_vec(),
        DocCompression::Snappy => snap::raw::Decoder::new().decompress_vec(data).unwrap_or_default(),
        DocCompression::Lz4 => lz4_flex::decompress(data, data.len() * 4).unwrap_or_default(),
        DocCompression::Zstd => zstd::bulk::decompress(data, data.len() * 4).unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_compress_decompress_roundtrip() {
        let json = br#"{"title":"hello","body":"world"}"#;
        for compression in &[DocCompression::None, DocCompression::Snappy, DocCompression::Lz4, DocCompression::Zstd] {
            let compressed = compress_document(json, compression);
            let decompressed = decompress_document(&compressed, compression);
            assert_eq!(decompressed, json.to_vec(), "roundtrip failed for {:?}", compression);
        }
    }
}
