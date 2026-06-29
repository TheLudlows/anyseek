// Posting list structures for the inverted index.
//
// PostingListObjectIndex  – on-disk / SlateDB-stored index (compressed blocks).
// PostingListObject0     – mutable in-memory buffer for uncommitted documents.

// ---------------------------------------------------------------------------
// Compression helpers
// ---------------------------------------------------------------------------

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    Delta = 0,
    Array = 1,
    Bitmap = 2,
    Rle = 3,
}

impl CompressionType {
    /// Decode the top-2 bits of a `u32` into a compression variant.
    pub fn from_u32_top2(val: u32) -> Self {
        match val >> 30 {
            0 => Self::Delta,
            1 => Self::Array,
            2 => Self::Bitmap,
            3 => Self::Rle,
            _ => unreachable!(),
        }
    }

    /// Pack a compression variant and a 30-bit offset into a single `u32`.
    pub fn pack_with_offset(self, offset: u32) -> u32 {
        (self as u32) << 30 | (offset & 0x3FFF_FFFF)
    }

    /// Extract the 30-bit offset from a packed `u32`.
    pub fn unpack_offset(packed: u32) -> u32 {
        packed & 0x3FFF_FFFF
    }
}

// ---------------------------------------------------------------------------
// Block-level index entry
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct BlockObjectIndex {
    pub max_block_score: f32,
    pub block_id: u32,
    pub compression_type_pointer: u32,
    pub posting_count: u16,
    pub max_docid: u16,
    pub max_p_docid: u16,
    pub pointer_pivot_p_docid: u16,
}

impl BlockObjectIndex {
    /// Construct a new block entry.
    pub fn new(
        max_block_score: f32,
        block_id: u32,
        compression_type_pointer: u32,
        posting_count: u16,
        max_docid: u16,
        max_p_docid: u16,
        pointer_pivot_p_docid: u16,
    ) -> Self {
        Self {
            max_block_score,
            block_id,
            compression_type_pointer,
            posting_count,
            max_docid,
            max_p_docid,
            pointer_pivot_p_docid,
        }
    }

    /// Serialise this block into bytes (little-endian).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(4 + 4 + 4 + 2 + 2 + 2 + 2);
        buf.extend_from_slice(&self.max_block_score.to_le_bytes());
        buf.extend_from_slice(&self.block_id.to_le_bytes());
        buf.extend_from_slice(&self.compression_type_pointer.to_le_bytes());
        buf.extend_from_slice(&self.posting_count.to_le_bytes());
        buf.extend_from_slice(&self.max_docid.to_le_bytes());
        buf.extend_from_slice(&self.max_p_docid.to_le_bytes());
        buf.extend_from_slice(&self.pointer_pivot_p_docid.to_le_bytes());
        buf
    }

    /// Deserialise a `BlockObjectIndex` from a byte slice.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let max_block_score = f32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let block_id = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        let compression_type_pointer = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        let posting_count = u16::from_le_bytes(bytes[12..14].try_into().unwrap());
        let max_docid = u16::from_le_bytes(bytes[14..16].try_into().unwrap());
        let max_p_docid = u16::from_le_bytes(bytes[16..18].try_into().unwrap());
        let pointer_pivot_p_docid = u16::from_le_bytes(bytes[18..20].try_into().unwrap());
        Self {
            max_block_score,
            block_id,
            compression_type_pointer,
            posting_count,
            max_docid,
            max_p_docid,
            pointer_pivot_p_docid,
        }
    }
}

// ---------------------------------------------------------------------------
// Posting list index (SlateDB-stored)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct PostingListObjectIndex {
    pub posting_count: u32,
    pub max_list_score: f32,
    pub blocks: Vec<BlockObjectIndex>,
    pub byte_array: Vec<u8>,
}

impl PostingListObjectIndex {
    /// Serialise to a `Vec<u8>` suitable for SlateDB storage.
    pub fn serialize(&self) -> Vec<u8> {
        let blocks_len = self.blocks.len();
        // Header: posting_count(4) + max_list_score(4) + blocks.len()(4)
        // Each block: 20 bytes
        // byte_array len(4) + bytes
        let mut buf = Vec::with_capacity(12 + blocks_len * 20 + 4 + self.byte_array.len());

        buf.extend_from_slice(&self.posting_count.to_le_bytes());
        buf.extend_from_slice(&self.max_list_score.to_le_bytes());
        buf.extend_from_slice(&(blocks_len as u32).to_le_bytes());

        for block in &self.blocks {
            buf.extend_from_slice(&block.to_bytes());
        }

        buf.extend_from_slice(&(self.byte_array.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.byte_array);

        buf
    }

    /// Deserialise from a byte slice produced by `serialize()`.
    pub fn deserialize(bytes: &[u8]) -> Self {
        let mut pos = 0;

        let posting_count = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap());
        pos += 4;

        let max_list_score = f32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap());
        pos += 4;

        let num_blocks = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        let mut blocks = Vec::with_capacity(num_blocks);
        for _ in 0..num_blocks {
            let block = BlockObjectIndex::from_bytes(&bytes[pos..pos + 20]);
            pos += 20;
            blocks.push(block);
        }

        let byte_array_len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        let byte_array = bytes[pos..pos + byte_array_len].to_vec();

        Self {
            posting_count,
            max_list_score,
            blocks,
            byte_array,
        }
    }
}

// ---------------------------------------------------------------------------
// In-memory mutable posting list buffer (level-0, uncommitted)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct PostingListObject0 {
    pub docids: Vec<u16>,
    pub positions: Vec<Vec<u16>>,
    pub posting_count: usize,
    pub max_block_score: f32,
    pub max_docid: u16,
    pub max_p_docid: u16,
}

// ---------------------------------------------------------------------------
// N-gram and query term types
// ---------------------------------------------------------------------------

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NgramType {
    None = 0,
    FF = 1,
    FR = 2,
    RF = 3,
    FFF = 4,
    FFR = 5,
    FRF = 6,
    RFF = 7,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryTypeTerm {
    Union = 0,
    Intersection = 1,
    Phrase = 2,
    Not = 3,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TermObject {
    pub term: String,
    pub key_hash: u64,
    pub key0: u32,
    pub field_positions_vec: Vec<Vec<u16>>,
    pub ngram_type: NgramType,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NonUniqueTermObject {
    pub term: String,
    pub query_type_term: QueryTypeTerm,
    pub key_hash: u64,
    pub key0: u32,
}

// ---------------------------------------------------------------------------
// Segment types
// ---------------------------------------------------------------------------

pub struct SegmentIndex {
    pub segment: ahash::AHashMap<u64, PostingListObjectIndex>,
    pub byte_array_blocks: Vec<Vec<u8>>,
}

pub struct SegmentLevel0 {
    pub segment: ahash::AHashMap<u64, PostingListObject0>,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- CompressionType pack / unpack roundtrip --

    #[test]
    fn test_compression_pack_unpack_roundtrip() {
        let variants = [
            CompressionType::Delta,
            CompressionType::Array,
            CompressionType::Bitmap,
            CompressionType::Rle,
        ];
        let offsets = [0u32, 1, 42, 0x3FFF_FFFF, 0x00FF_00FF, 0x1234_5678];

        for &ct in &variants {
            for &offset in &offsets {
                let packed = ct.pack_with_offset(offset);
                assert_eq!(CompressionType::from_u32_top2(packed), ct);
                assert_eq!(CompressionType::unpack_offset(packed), offset);
            }
        }
    }

    #[test]
    fn test_compression_type_values() {
        assert_eq!(CompressionType::Delta as u8, 0);
        assert_eq!(CompressionType::Array as u8, 1);
        assert_eq!(CompressionType::Bitmap as u8, 2);
        assert_eq!(CompressionType::Rle as u8, 3);
    }

    // -- BlockObjectIndex construction --

    #[test]
    fn test_block_object_index_new() {
        let block = BlockObjectIndex::new(0.75, 1, CompressionType::Delta.pack_with_offset(100), 5, 42, 3, 2);

        assert!((block.max_block_score - 0.75).abs() < f32::EPSILON);
        assert_eq!(block.block_id, 1);
        assert_eq!(block.posting_count, 5);
        assert_eq!(block.max_docid, 42);
        assert_eq!(block.max_p_docid, 3);
        assert_eq!(block.pointer_pivot_p_docid, 2);
    }

    #[test]
    fn test_block_object_index_serialize_roundtrip() {
        let block = BlockObjectIndex::new(1.23, 7, CompressionType::Bitmap.pack_with_offset(999), 10, 500, 5, 3);
        let bytes = block.to_bytes();
        assert_eq!(bytes.len(), 20);

        let decoded = BlockObjectIndex::from_bytes(&bytes);
        assert_eq!(decoded, block);
    }

    // -- PostingListObjectIndex serialize / deserialize roundtrip --

    #[test]
    fn test_posting_list_serialize_deserialize_empty() {
        let pli = PostingListObjectIndex {
            posting_count: 0,
            max_list_score: 0.0,
            blocks: vec![],
            byte_array: vec![],
        };
        let bytes = pli.serialize();
        let decoded = PostingListObjectIndex::deserialize(&bytes);
        assert_eq!(decoded, pli);
    }

    #[test]
    fn test_posting_list_serialize_deserialize() {
        let blocks = vec![
            BlockObjectIndex::new(0.9, 0, CompressionType::Delta.pack_with_offset(0), 10, 100, 5, 3),
            BlockObjectIndex::new(0.5, 1, CompressionType::Array.pack_with_offset(200), 20, 200, 10, 6),
            BlockObjectIndex::new(0.3, 2, CompressionType::Rle.pack_with_offset(500), 5, 500, 2, 1),
        ];
        let byte_array: Vec<u8> = (0..=255).collect();

        let pli = PostingListObjectIndex {
            posting_count: 35,
            max_list_score: 0.9,
            blocks,
            byte_array,
        };

        let bytes = pli.serialize();
        let decoded = PostingListObjectIndex::deserialize(&bytes);
        assert_eq!(decoded, pli);
    }

    #[test]
    fn test_posting_list_serialized_length() {
        // Header: 4 + 4 + 4 = 12
        // 2 blocks: 2 * 20 = 40
        // byte_array len: 4 + 10 = 14
        // Total = 66
        let blocks = vec![
            BlockObjectIndex::new(1.0, 0, CompressionType::Delta.pack_with_offset(0), 5, 50, 3, 2),
            BlockObjectIndex::new(0.8, 1, CompressionType::Array.pack_with_offset(100), 8, 80, 4, 2),
        ];
        let byte_array = vec![0u8; 10];

        let pli = PostingListObjectIndex {
            posting_count: 13,
            max_list_score: 1.0,
            blocks,
            byte_array,
        };

        assert_eq!(pli.serialize().len(), 66);
    }
}
