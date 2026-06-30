use seekpower_error::Result;

/// Compression type for posting lists
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionType {
    Array = 1,
    Bitmap = 2,
    Rle = 3,
}

/// In-memory posting entry
#[derive(Debug, Clone)]
pub struct PostingEntry {
    pub doc_id: u64,
    pub field_id: u16,
    pub positions: Vec<u16>,
}

/// Serialized block metadata
#[derive(Debug, Clone)]
pub struct BlockHeader {
    pub max_block_score: f32,
    pub posting_count: u32,
    pub max_doc_id: u64,
}

/// Main serializable posting list structure
#[derive(Debug, Clone)]
pub struct PostingListBlock {
    pub posting_count: u32,
    pub max_score: f32,
    pub compression_type: CompressionType,
    pub blocks: Vec<PostingBlock>,
    pub data: Vec<u8>,
}

/// Block within a posting list
#[derive(Debug, Clone)]
pub struct PostingBlock {
    pub header: BlockHeader,
    pub offset: usize,
    pub len: usize,
}

/// Codec for encoding/decoding posting lists
pub struct PostingCodec;

impl PostingCodec {
    /// Encode a sorted list of PostingEntry into a PostingListBlock
    pub fn encode(entries: &[PostingEntry]) -> PostingListBlock {
        let mut data = Vec::new();
        let mut last_doc_id = 0u64;

        for entry in entries {
            // Delta encode doc_id
            let delta = entry.doc_id - last_doc_id;
            encode_varint(delta, &mut data);
            last_doc_id = entry.doc_id;

            // Encode field_id
            data.extend_from_slice(&entry.field_id.to_le_bytes());

            // Delta encode positions
            let pos_count = entry.positions.len() as u16;
            data.extend_from_slice(&pos_count.to_le_bytes());

            let mut last_pos = 0u16;
            for &pos in &entry.positions {
                let delta = pos - last_pos;
                encode_varint(delta as u64, &mut data);
                last_pos = pos;
            }
        }

        let max_doc_id = entries.last().map(|e| e.doc_id).unwrap_or(0);
        let max_score = 0.0f32; // TODO: calculate from entries

        PostingListBlock {
            posting_count: entries.len() as u32,
            max_score,
            compression_type: CompressionType::Array,
            blocks: vec![PostingBlock {
                header: BlockHeader {
                    max_block_score: max_score,
                    posting_count: entries.len() as u32,
                    max_doc_id,
                },
                offset: 0,
                len: data.len(),
            }],
            data,
        }
    }

    /// Decode a PostingListBlock back to PostingEntry list
    pub fn decode(block: &PostingListBlock) -> Vec<PostingEntry> {
        let mut entries = Vec::new();
        let mut offset = 0;
        let mut last_doc_id = 0u64;

        for _ in 0..block.posting_count {
            // Decode doc_id delta
            let delta = decode_varint(&block.data, &mut offset);
            let doc_id = last_doc_id + delta;
            last_doc_id = doc_id;

            // Decode field_id
            let field_id = u16::from_le_bytes([block.data[offset], block.data[offset + 1]]);
            offset += 2;

            // Decode position count
            let pos_count = u16::from_le_bytes([block.data[offset], block.data[offset + 1]]) as usize;
            offset += 2;

            // Decode positions
            let mut positions = Vec::with_capacity(pos_count);
            let mut last_pos = 0u16;
            for _ in 0..pos_count {
                let delta = decode_varint(&block.data, &mut offset) as u16;
                let pos = last_pos + delta;
                positions.push(pos);
                last_pos = pos;
            }

            entries.push(PostingEntry {
                doc_id,
                field_id,
                positions,
            });
        }

        entries
    }

    /// Serialize PostingListBlock to binary bytes
    pub fn serialize(block: &PostingListBlock) -> Vec<u8> {
        let mut result = Vec::new();

        // Header (16 bytes)
        result.extend_from_slice(&block.posting_count.to_le_bytes());
        result.extend_from_slice(&block.max_score.to_le_bytes());
        result.push(block.compression_type as u8);
        result.push(block.blocks.len() as u8);
        result.extend_from_slice(&[0u8; 6]); // reserved

        // Block headers
        for posting_block in &block.blocks {
            result.extend_from_slice(&posting_block.header.max_block_score.to_le_bytes());
            result.extend_from_slice(&posting_block.header.posting_count.to_le_bytes());
            result.extend_from_slice(&posting_block.header.max_doc_id.to_le_bytes());
            result.extend_from_slice(&(posting_block.offset as u32).to_le_bytes());
            result.extend_from_slice(&(posting_block.len as u32).to_le_bytes());
        }

        // Data section
        result.extend_from_slice(&block.data);

        result
    }

    /// Deserialize bytes back to PostingListBlock
    pub fn deserialize(data: &[u8]) -> Result<PostingListBlock> {
        if data.len() < 16 {
            return Err(seekpower_error::SeekPowerError::Corrupted(
                "Data too short for header".to_string(),
            ));
        }

        let mut offset = 0;

        // Header
        let posting_count = u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]);
        offset += 4;
        let max_score = f32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]);
        offset += 4;
        let compression_type = match data[offset] {
            1 => CompressionType::Array,
            2 => CompressionType::Bitmap,
            3 => CompressionType::Rle,
            _ => {
                return Err(seekpower_error::SeekPowerError::Corrupted(
                    format!("Invalid compression type: {}", data[offset]),
                ))
            }
        };
        offset += 1;
        let block_count = data[offset] as usize;
        offset += 1;
        offset += 6; // reserved

        // Block headers
        let mut blocks = Vec::with_capacity(block_count);
        for _ in 0..block_count {
            if offset + 24 > data.len() {
                return Err(seekpower_error::SeekPowerError::Corrupted(
                    "Data too short for block header".to_string(),
                ));
            }

            let max_block_score = f32::from_le_bytes([
                data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
            ]);
            offset += 4;
            let posting_count = u32::from_le_bytes([
                data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
            ]);
            offset += 4;
            let max_doc_id = u64::from_le_bytes([
                data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
                data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7],
            ]);
            offset += 8;
            let block_offset = u32::from_le_bytes([
                data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
            ]) as usize;
            offset += 4;
            let block_len = u32::from_le_bytes([
                data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
            ]) as usize;
            offset += 4;

            blocks.push(PostingBlock {
                header: BlockHeader {
                    max_block_score,
                    posting_count,
                    max_doc_id,
                },
                offset: block_offset,
                len: block_len,
            });
        }

        // Data section
        let data_section = data[offset..].to_vec();

        Ok(PostingListBlock {
            posting_count,
            max_score,
            compression_type,
            blocks,
            data: data_section,
        })
    }
}

/// Merge existing posting list with new entries
pub fn merge_postings(
    existing: &PostingListBlock,
    new_entries: &[PostingEntry],
) -> PostingListBlock {
    let existing_entries = PostingCodec::decode(existing);
    let mut all_entries = existing_entries;
    all_entries.extend(new_entries.iter().cloned());

    // Sort by doc_id
    all_entries.sort_by_key(|e| e.doc_id);

    // Remove duplicates (keep first occurrence)
    all_entries.dedup_by_key(|e| e.doc_id);

    PostingCodec::encode(&all_entries)
}

/// Choose compression type (for future use)
#[allow(dead_code)]
fn choose_compression(_posting_count: u32) -> CompressionType {
    CompressionType::Array
}

/// Encode varint
fn encode_varint(mut value: u64, buf: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if value == 0 {
            break;
        }
    }
}

/// Decode varint
fn decode_varint(data: &[u8], offset: &mut usize) -> u64 {
    let mut result = 0u64;
    let mut shift = 0;

    loop {
        let byte = data[*offset];
        *offset += 1;
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_roundtrip() {
        let test_values = vec![0, 1, 127, 128, 255, 256, 16383, 16384, u64::MAX];

        for &value in &test_values {
            let mut buf = Vec::new();
            encode_varint(value, &mut buf);
            let mut offset = 0;
            let decoded = decode_varint(&buf, &mut offset);
            assert_eq!(value, decoded);
            assert_eq!(offset, buf.len());
        }
    }

    #[test]
    fn test_codec_encode_decode() {
        let entries = vec![
            PostingEntry {
                doc_id: 100,
                field_id: 1,
                positions: vec![3, 7, 15],
            },
            PostingEntry {
                doc_id: 105,
                field_id: 2,
                positions: vec![1, 5],
            },
            PostingEntry {
                doc_id: 107,
                field_id: 1,
                positions: vec![10],
            },
        ];

        let block = PostingCodec::encode(&entries);
        let decoded = PostingCodec::decode(&block);

        assert_eq!(decoded.len(), entries.len());
        for (orig, dec) in entries.iter().zip(decoded.iter()) {
            assert_eq!(orig.doc_id, dec.doc_id);
            assert_eq!(orig.field_id, dec.field_id);
            assert_eq!(orig.positions, dec.positions);
        }
    }

    #[test]
    fn test_codec_serialize_deserialize() {
        let entries = vec![
            PostingEntry {
                doc_id: 100,
                field_id: 1,
                positions: vec![3, 7, 15],
            },
            PostingEntry {
                doc_id: 105,
                field_id: 2,
                positions: vec![1, 5],
            },
        ];

        let block = PostingCodec::encode(&entries);
        let serialized = PostingCodec::serialize(&block);
        let deserialized = PostingCodec::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.posting_count, block.posting_count);
        assert_eq!(deserialized.max_score, block.max_score);
        assert_eq!(deserialized.compression_type, block.compression_type);
        assert_eq!(deserialized.blocks.len(), block.blocks.len());
        assert_eq!(deserialized.data, block.data);
    }

    #[test]
    fn test_merge_postings() {
        let existing_entries = vec![
            PostingEntry {
                doc_id: 100,
                field_id: 1,
                positions: vec![3, 7],
            },
            PostingEntry {
                doc_id: 105,
                field_id: 2,
                positions: vec![1],
            },
        ];

        let new_entries = vec![
            PostingEntry {
                doc_id: 103,
                field_id: 1,
                positions: vec![5, 10],
            },
            PostingEntry {
                doc_id: 107,
                field_id: 2,
                positions: vec![2],
            },
        ];

        let existing = PostingCodec::encode(&existing_entries);
        let merged = merge_postings(&existing, &new_entries);
        let decoded = PostingCodec::decode(&merged);

        assert_eq!(decoded.len(), 4);
        assert_eq!(decoded[0].doc_id, 100);
        assert_eq!(decoded[1].doc_id, 103);
        assert_eq!(decoded[2].doc_id, 105);
        assert_eq!(decoded[3].doc_id, 107);
    }

    #[test]
    fn test_empty_posting() {
        let entries: Vec<PostingEntry> = vec![];
        let block = PostingCodec::encode(&entries);
        let decoded = PostingCodec::decode(&block);
        assert_eq!(decoded.len(), 0);
    }

    #[test]
    fn test_single_entry() {
        let entries = vec![PostingEntry {
            doc_id: 42,
            field_id: 1,
            positions: vec![1, 2, 3],
        }];

        let block = PostingCodec::encode(&entries);
        let decoded = PostingCodec::decode(&block);

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].doc_id, 42);
        assert_eq!(decoded[0].field_id, 1);
        assert_eq!(decoded[0].positions, vec![1, 2, 3]);
    }

    #[test]
    fn test_large_posting_list() {
        let entries: Vec<PostingEntry> = (0..1000)
            .map(|i| PostingEntry {
                doc_id: i * 10,
                field_id: (i % 10) as u16,
                positions: vec![1, 2, 3, 4, 5],
            })
            .collect();

        let block = PostingCodec::encode(&entries);
        let decoded = PostingCodec::decode(&block);

        assert_eq!(decoded.len(), 1000);
        for (i, entry) in decoded.iter().enumerate() {
            assert_eq!(entry.doc_id, (i * 10) as u64);
            assert_eq!(entry.field_id, (i % 10) as u16);
            assert_eq!(entry.positions, vec![1, 2, 3, 4, 5]);
        }
    }
}
