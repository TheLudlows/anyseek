// storm-db module

use crate::posting::CompressionType;

const BITMAP_SIZE: usize = 8192; // 65536 bits = one bit per possible local docid in a 64K block
const STOP_BIT: u8 = 0x80;
const VINT_MASK: u16 = 0x7F;
const VINT_MASK_U8: u8 = 0x7F;

// ---------------------------------------------------------------------------
// VInt position compression
// ---------------------------------------------------------------------------

/// Encode a single delta value into VInt bytes.
/// Returns the number of bytes written.
/// VInt byte count for a delta value.
fn vint_delta_bytes(delta: u16) -> usize {
    if delta < 128 {
        1
    } else if delta < 16384 {
        2
    } else {
        3
    }
}

fn encode_vint_delta(delta: u16) -> [u8; 3] {
    if delta < 128 {
        [(delta as u8) | STOP_BIT, 0, 0]
    } else if delta < 16384 {
        [
            ((delta >> 7) & VINT_MASK) as u8,
            ((delta & VINT_MASK) as u8) | STOP_BIT,
            0,
        ]
    } else {
        [
            ((delta >> 14) & VINT_MASK) as u8,
            ((delta >> 7) & VINT_MASK) as u8,
            ((delta & VINT_MASK) as u8) | STOP_BIT,
        ]
    }
}

/// Compress position lists into VInt format.
/// For each posting list entry, positions are delta-encoded then VInt compressed.
/// Position pointers: entries with index < pointer_pivot use 2-byte pointers;
/// entries >= pointer_pivot use 3-byte pointers.
///
/// Output layout per entry:
///   - 2-byte (or 3-byte) position data offset relative to start of position block
///   - VInt delta-encoded positions (starting from 0, with STOP_BIT markers)
///
/// The offsets are written as little-endian and point into the concatenated VInt data.
pub fn compress_positions(positions: &[Vec<u16>], pointer_pivot: u16) -> Vec<u8> {
    // First pass: compute sizes and offsets
    let mut vint_data: Vec<u8> = Vec::new();
    let mut offsets: Vec<usize> = Vec::with_capacity(positions.len());

    for pos_list in positions {
        offsets.push(vint_data.len());
        for pos in pos_list {
            let encoded = encode_vint_delta(*pos);
            if *pos < 128 {
                vint_data.push(encoded[0]);
            } else if *pos < 16384 {
                vint_data.push(encoded[0]);
                vint_data.push(encoded[1]);
            } else {
                vint_data.push(encoded[0]);
                vint_data.push(encoded[1]);
                vint_data.push(encoded[2]);
            }
        }
    }

    // Second pass: build output = [pointers | vint_data]
    let mut result: Vec<u8> = Vec::new();

    // Write position data pointers (offsets into the VInt data region)
    for (i, &offset) in offsets.iter().enumerate() {
        if (i as u16) < pointer_pivot {
            // 2-byte pointer
            result.extend_from_slice(&(offset as u16).to_le_bytes());
        } else {
            // 3-byte pointer
            result.push((offset & 0xFF) as u8);
            result.push(((offset >> 8) & 0xFF) as u8);
            result.push(((offset >> 16) & 0xFF) as u8);
        }
    }

    // Append the VInt encoded position data
    result.extend_from_slice(&vint_data);

    result
}

/// Decompress a single position list from VInt data at given offset.
/// `count` is the number of positions to read.
pub fn decompress_positions(data: &[u8], offset: usize, count: u16) -> Vec<u16> {
    let mut result = Vec::with_capacity(count as usize);
    let mut pos = offset;
    let mut positions_read: u16 = 0;

    while positions_read < count && pos < data.len() {
        let mut value: u16 = 0;
        loop {
            let byte = data[pos];
            pos += 1;
            value = (value << 7) | ((byte & VINT_MASK_U8) as u16);
            if byte & STOP_BIT != 0 {
                result.push(value);
                positions_read += 1;
                break;
            }
        }
    }

    result
}

// ---------------------------------------------------------------------------
// RLE helpers
// ---------------------------------------------------------------------------

/// Count the number of RLE runs in a sorted docid list.
/// A "run" is a maximal sequence of consecutive docids.
fn count_runs(docids: &[u16]) -> usize {
    if docids.is_empty() {
        return 0;
    }
    let mut runs = 1usize;
    for i in 1..docids.len() {
        if docids[i] != docids[i - 1] + 1 {
            runs += 1;
        }
    }
    runs
}

/// Encode docids into RLE format: [runs_count: u16, (start: u16, len: u16)*]
fn encode_rle(docids: &[u16]) -> Vec<u8> {
    if docids.is_empty() {
        return 0u16.to_le_bytes().to_vec();
    }

    // Collect runs
    let mut runs: Vec<(u16, u16)> = Vec::new();
    let mut run_start = docids[0];
    let mut run_len: u16 = 1;

    for i in 1..docids.len() {
        if docids[i] == docids[i - 1] + 1 {
            run_len += 1;
        } else {
            runs.push((run_start, run_len));
            run_start = docids[i];
            run_len = 1;
        }
    }
    runs.push((run_start, run_len));

    let mut data = Vec::with_capacity(2 + runs.len() * 4);
    data.extend_from_slice(&(runs.len() as u16).to_le_bytes());
    for &(start, len) in &runs {
        data.extend_from_slice(&start.to_le_bytes());
        data.extend_from_slice(&len.to_le_bytes());
    }
    data
}

/// Decode RLE docids from the given data slice.
fn decode_rle(docid_data: &[u8]) -> Vec<u16> {
    if docid_data.len() < 2 {
        return Vec::new();
    }
    let runs_count = u16::from_le_bytes([docid_data[0], docid_data[1]]) as usize;
    let mut result = Vec::new();
    let mut idx = 2;
    for _ in 0..runs_count {
        if idx + 3 >= docid_data.len() {
            break;
        }
        let start = u16::from_le_bytes([docid_data[idx], docid_data[idx + 1]]);
        let len = u16::from_le_bytes([docid_data[idx + 2], docid_data[idx + 3]]);
        idx += 4;
        for j in 0..len {
            result.push(start + j);
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Main compression / decompression API
// ---------------------------------------------------------------------------

/// Compress a posting list (docids + positions).
/// Returns (compressed_data, compression_type, pointer_pivot_p_docid, max_score).
pub fn compress_postinglist(
    docids: &[u16],
    positions: &[Vec<u16>],
) -> (Vec<u8>, CompressionType, u16, f32) {
    let posting_count = docids.len() as u16;
    let array_size = (posting_count as usize) * 2;

    // Determine RLE threshold and target compression type
    let (rle_threshold, preferred_fallback) = if posting_count < 4096 {
        (std::cmp::min(posting_count as usize / 2, array_size / 4), CompressionType::Array)
    } else {
        (2048, CompressionType::Bitmap)
    };

    // Try RLE first
    let runs = count_runs(docids);
    let (docid_data, compression_type) = if runs <= rle_threshold && runs > 0 {
        (encode_rle(docids), CompressionType::Rle)
    } else if preferred_fallback == CompressionType::Array {
        // Array: sorted docids as little-endian u16
        let mut data = Vec::with_capacity(array_size);
        for &docid in docids {
            data.extend_from_slice(&docid.to_le_bytes());
        }
        (data, CompressionType::Array)
    } else {
        // Bitmap: 8192 bytes, one bit per docid
        let mut bitmap = vec![0u8; BITMAP_SIZE];
        for &docid in docids {
            bitmap[(docid as usize) >> 3] |= 1 << (docid & 7);
        }
        (bitmap, CompressionType::Bitmap)
    };

    // Compress positions
    let position_data = compress_positions(positions, posting_count);

    // Concatenate: [compressed_docids | compressed_positions]
    let mut result = docid_data;
    result.extend_from_slice(&position_data);

    (result, compression_type, posting_count, 0.0)
}

/// Decompress docids from compressed data given compression type and count.
pub fn decompress_docids(
    data: &[u8],
    compression_type: CompressionType,
    posting_count: u16,
) -> Vec<u16> {
    match compression_type {
        CompressionType::Array => {
            let mut result = Vec::with_capacity(posting_count as usize);
            for i in 0..posting_count as usize {
                let offset = i * 2;
                if offset + 1 < data.len() {
                    let docid = u16::from_le_bytes([data[offset], data[offset + 1]]);
                    result.push(docid);
                }
            }
            result
        }
        CompressionType::Bitmap => {
            let bitmap = &data[..std::cmp::min(data.len(), BITMAP_SIZE)];
            let mut result = Vec::with_capacity(posting_count as usize);
            for byte_idx in 0..bitmap.len() {
                let b = bitmap[byte_idx];
                if b != 0 {
                    for bit in 0..8u8 {
                        if b & (1 << bit) != 0 {
                            result.push((byte_idx * 8 + bit as usize) as u16);
                        }
                    }
                }
            }
            result
        }
        CompressionType::Rle => decode_rle(data),
        CompressionType::Delta => Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Array compression roundtrip ----

    #[test]
    fn test_array_compress_decompress_sparse_docids() {
        // Sparse docids: gaps between them, not consecutive
        let docids = vec![5, 100, 200, 500, 1000, 2000, 5000, 10000, 30000, 50000];
        let positions: Vec<Vec<u16>> = docids.iter().map(|_| vec![0]).collect();

        let (data, ctype, pivot, score) = compress_postinglist(&docids, &positions);
        assert_eq!(ctype, CompressionType::Array);
        assert_eq!(pivot, docids.len() as u16);
        assert_eq!(score, 0.0);

        let decoded = decompress_docids(&data, ctype, docids.len() as u16);
        assert_eq!(decoded, docids);
    }

    #[test]
    fn test_array_roundtrip_manual() {
        let docids = vec![1, 3, 7, 15, 31, 63, 127, 255, 511, 1023];
        let positions: Vec<Vec<u16>> = docids.iter().map(|_| vec![0, 1, 2]).collect();

        let (data, ctype, _, _) = compress_postinglist(&docids, &positions);
        assert_eq!(ctype, CompressionType::Array);

        // Verify the first bytes are the docid array
        for (i, &docid) in docids.iter().enumerate() {
            let offset = i * 2;
            let decoded = u16::from_le_bytes([data[offset], data[offset + 1]]);
            assert_eq!(decoded, docid);
        }

        let decoded_docids = decompress_docids(&data, ctype, docids.len() as u16);
        assert_eq!(decoded_docids, docids);
    }

    // ---- Bitmap compression roundtrip ----

    #[test]
    fn test_bitmap_compress_decompress_dense_nonconsecutive() {
        // Many docids (>= 4096), non-consecutive => should pick Bitmap
        let mut docids: Vec<u16> = Vec::new();
        // Spread docids across the range, non-consecutive pairs
        for i in (0..65536).step_by(8) {
            docids.push(i as u16);
            docids.push((i + 3) as u16);
        }

        assert!(docids.len() >= 4096);
        let positions: Vec<Vec<u16>> = docids.iter().map(|_| vec![0]).collect();

        let (data, ctype, _, _) = compress_postinglist(&docids, &positions);
        assert_eq!(ctype, CompressionType::Bitmap);
        let bitmap = &data[..BITMAP_SIZE];

        let decoded = decompress_docids(bitmap, ctype, docids.len() as u16);
        let mut sorted_decoded = decoded;
        sorted_decoded.sort();
        let mut sorted_original = docids.clone();
        sorted_original.sort();
        assert_eq!(sorted_decoded, sorted_original);
    }

    #[test]
    fn test_bitmap_all_bits_set() {
        // Test that setting all bits and decoding returns all 65536 docids.
        let bitmap = vec![0xFFu8; BITMAP_SIZE];
        let decoded = decompress_docids(&bitmap, CompressionType::Bitmap, u16::MAX);
        assert_eq!(decoded.len(), 65536);
    }

    #[test]
    fn test_bitmap_single_bit() {
        // Test bitmap encoding/decoding for a single bit
        let mut bitmap = vec![0u8; BITMAP_SIZE];
        bitmap[5] |= 1 << 2; // docid 42: byte 5, bit 2

        let decoded = decompress_docids(&bitmap, CompressionType::Bitmap, 1);
        assert_eq!(decoded, vec![42u16]);
    }

    #[test]
    fn test_bitmap_compress_decompress_via_api() {
        // Force bitmap by providing >= 4096 docids with many gaps
        let mut docids: Vec<u16> = Vec::new();
        for i in (0..65536).step_by(16) {
            docids.push(i as u16);
            if (i + 7) < 65536 {
                docids.push((i + 7) as u16);
            }
        }
        assert!(docids.len() >= 4096);
        docids.sort();
        docids.dedup();

        let _positions: Vec<Vec<u16>> = docids.iter().map(|_| vec![0]).collect();

        let (data, ctype, _, _) = compress_postinglist(&docids, &_positions);
        assert_eq!(ctype, CompressionType::Bitmap);

        let bitmap = &data[..BITMAP_SIZE];
        let decoded = decompress_docids(bitmap, ctype, docids.len() as u16);
        assert_eq!(decoded, docids);
    }

    // ---- RLE compression roundtrip ----

    #[test]
    fn test_rle_compress_decompress_consecutive_docids() {
        // Perfectly consecutive docids => 1 run
        let docids: Vec<u16> = (100..200).collect();
        let positions: Vec<Vec<u16>> = docids.iter().map(|_| vec![0]).collect();

        let (data, ctype, _, _) = compress_postinglist(&docids, &positions);
        assert_eq!(ctype, CompressionType::Rle);

        let decoded = decompress_docids(&data, ctype, docids.len() as u16);
        assert_eq!(decoded, docids);
    }

    #[test]
    fn test_rle_multiple_runs() {
        // Docids with a few consecutive runs
        let docids = vec![1, 2, 3, 4, 5, 100, 101, 102, 500, 501, 502, 503, 504];
        let positions: Vec<Vec<u16>> = docids.iter().map(|_| vec![0]).collect();

        let (data, ctype, _, _) = compress_postinglist(&docids, &positions);
        assert_eq!(ctype, CompressionType::Rle);

        // Verify RLE header: 3 runs ([1..=5], [100..=102], [500..=504])
        assert_eq!(u16::from_le_bytes([data[0], data[1]]), 3);

        let decoded = decompress_docids(&data, ctype, docids.len() as u16);
        assert_eq!(decoded, docids);
    }

    #[test]
    fn test_rle_long_run() {
        let docids: Vec<u16> = (0..50000).collect();
        let positions: Vec<Vec<u16>> = docids.iter().map(|_| vec![0]).collect();

        let (data, ctype, _, _) = compress_postinglist(&docids, &positions);
        assert_eq!(ctype, CompressionType::Rle);

        // Should have exactly 1 run
        assert_eq!(u16::from_le_bytes([data[0], data[1]]), 1);
        // start=0, len=50000
        assert_eq!(u16::from_le_bytes([data[2], data[3]]), 0);
        assert_eq!(u16::from_le_bytes([data[4], data[5]]), 50000);

        let decoded = decompress_docids(&data, ctype, docids.len() as u16);
        assert_eq!(decoded.len(), 50000);
        assert_eq!(decoded[0], 0);
        assert_eq!(decoded[49999], 49999);
    }

    // ---- Auto-selection ----

    #[test]
    fn test_auto_select_array_few_docids() {
        // Few docids with gaps => Array
        let docids = vec![1, 10, 20, 30, 40];
        let positions: Vec<Vec<u16>> = docids.iter().map(|_| vec![0]).collect();

        let (_, ctype, _, _) = compress_postinglist(&docids, &positions);
        assert_eq!(ctype, CompressionType::Array);
    }

    #[test]
    fn test_auto_select_rle_many_consecutive() {
        // Many consecutive docids < 4096 => RLE
        let docids: Vec<u16> = (0..1000).collect();
        let positions: Vec<Vec<u16>> = docids.iter().map(|_| vec![0]).collect();

        let (_, ctype, _, _) = compress_postinglist(&docids, &positions);
        assert_eq!(ctype, CompressionType::Rle);
    }

    #[test]
    fn test_auto_select_bitmap_many_nonconsecutive() {
        // Many docids >= 4096 with low run count but high threshold => Bitmap
        // (because count_runs will be high for non-consecutive spread)
        let mut docids: Vec<u16> = Vec::new();
        for i in (0..65536).step_by(4) {
            docids.push(i as u16);
        }
        let positions: Vec<Vec<u16>> = docids.iter().map(|_| vec![0]).collect();

        let (_, ctype, _, _) = compress_postinglist(&docids, &positions);
        assert_eq!(ctype, CompressionType::Bitmap);
    }

    #[test]
    fn test_auto_select_rle_over_array_small_runs() {
        // Docids with a few runs that is under threshold => RLE
        // posting_count=20, threshold = min(10, 10) = 10
        // If runs <= 10, RLE is chosen
        let docids = vec![
            0, 1, 2, 3, 4, 10, 11, 12, 20, 21, 22, 30, 31, 40, 41, 50, 51, 60, 61, 70,
        ]; // 10 runs
        let positions: Vec<Vec<u16>> = docids.iter().map(|_| vec![0]).collect();

        let (_, ctype, _, _) = compress_postinglist(&docids, &positions);
        assert_eq!(ctype, CompressionType::Rle);
    }

    // ---- VInt position compression roundtrip ----

    #[test]
    fn test_vint_small_deltas() {
        // All deltas < 128 => 1 byte each
        let positions = vec![vec![0, 5, 10, 127], vec![0, 1, 2]];

        let data = compress_positions(&positions, positions.len() as u16);

        // Decompress each position list
        let pos0 = decompress_positions(&data, 4, 4);
        assert_eq!(pos0, vec![0, 5, 10, 127]);

        let second_list_offset = 4 + 4;
        let pos1_data = decompress_positions(&data, second_list_offset, 3);
        assert_eq!(pos1_data, vec![0, 1, 2]);
    }

    #[test]
    fn test_vint_medium_deltas() {
        // Delta = 200 => 2 bytes
        let positions = vec![vec![0, 200, 5000]];
        let data = compress_positions(&positions, 1);

        // 2-byte pointer (pivot=1, index 0 < 1)
        // Delta 0: 0x80 (1 byte)
        // Delta 200: 200 >> 7 = 1, 200 & 0x7F = 72 => [0x01, 0xC8]
        // Delta 5000: 5000 >> 7 = 39, 5000 & 0x7F = 8 => [0x27, 0x88]
        let decoded = decompress_positions(&data, 2, 3);
        assert_eq!(decoded, vec![0, 200, 5000]);
    }

    #[test]
    fn test_vint_large_deltas() {
        // Delta >= 16384 => 3 bytes
        let positions = vec![vec![0, 20000, 40000]];
        let data = compress_positions(&positions, 1);

        let decoded = decompress_positions(&data, 2, 3);
        assert_eq!(decoded, vec![0, 20000, 40000]);
    }

    #[test]
    fn test_vint_mixed_delta_sizes() {
        // Mix of 1-byte, 2-byte, and 3-byte deltas
        let positions = vec![vec![0, 50, 200, 20000, 60000]];
        let data = compress_positions(&positions, 1);

        let decoded = decompress_positions(&data, 2, 5);
        assert_eq!(decoded, vec![0, 50, 200, 20000, 60000]);
    }

    #[test]
    fn test_vint_empty_positions() {
        let positions: Vec<Vec<u16>> = vec![vec![], vec![0, 1]];
        let data = compress_positions(&positions, 2);

        let decoded0 = decompress_positions(&data, 4, 0);
        assert!(decoded0.is_empty());

        // pos0 has 0 bytes, so pos1 starts at offset 4
        let decoded1 = decompress_positions(&data, 4, 2);
        assert_eq!(decoded1, vec![0, 1]);
    }

    #[test]
    fn test_vint_pointer_pivot_2byte_vs_3byte() {
        // First entry uses 2-byte pointer, second uses 3-byte
        let positions = vec![vec![0, 1], vec![0, 2]];
        let pivot = 1u16;
        let data = compress_positions(&positions, pivot);

        // First pointer: 2 bytes at offset 0
        // Second pointer: 3 bytes at offset 2
        // So VInt data starts at offset 5
        assert_eq!(data.len(), 5 + 2 + 2); // 2+3 pointers + 1 byte each * 2 positions

        let decoded0 = decompress_positions(&data, 5, 2);
        assert_eq!(decoded0, vec![0, 1]);

        let decoded1 = decompress_positions(&data, 7, 2);
        assert_eq!(decoded1, vec![0, 2]);
    }

    // ---- Full compress_postinglist + decompress roundtrip ----

    #[test]
    fn test_full_roundtrip_array_with_positions() {
        let docids = vec![3, 7, 15, 31, 63];
        let positions = vec![
            vec![0, 1, 2, 3],
            vec![0, 5],
            vec![0, 10, 20],
            vec![0],
            vec![0, 1, 2, 3, 4, 5],
        ];

        let (data, ctype, pivot, _) = compress_postinglist(&docids, &positions);
        assert_eq!(ctype, CompressionType::Array);
        assert_eq!(pivot, 5);

        let decoded_docids = decompress_docids(&data, ctype, docids.len() as u16);
        assert_eq!(decoded_docids, docids);

        // Position data starts after the docid array
        let pos_offset = docids.len() * 2;
        // Pointer block: 5 entries * 2 bytes = 10 bytes
        let pointer_block_size = docids.len() * 2;
        let vint_start = pos_offset + pointer_block_size;

        let mut current_offset = vint_start;
        for (i, expected_pos) in positions.iter().enumerate() {
            let decoded = decompress_positions(&data, current_offset, expected_pos.len() as u16);
            assert_eq!(decoded, *expected_pos, "position list {} mismatch", i);
            for &pos in expected_pos {
                current_offset += vint_delta_bytes(pos);
            }
        }
    }

    #[test]
    fn test_full_roundtrip_rle_with_positions() {
        let docids: Vec<u16> = (100..150).collect();
        let positions: Vec<Vec<u16>> = docids
            .iter()
            .enumerate()
            .map(|(i, _)| vec![0, (i as u16) * 3, (i as u16) * 5])
            .collect();

        let (data, ctype, pivot, _) = compress_postinglist(&docids, &positions);
        assert_eq!(ctype, CompressionType::Rle);
        assert_eq!(pivot, 50);

        let decoded_docids = decompress_docids(&data, ctype, docids.len() as u16);
        assert_eq!(decoded_docids, docids);

        // RLE data: 2 bytes header + 1 run * 4 bytes = 6 bytes
        let rle_size = 6;
        let pointer_block_size = docids.len() * 2; // 2-byte pointers (all < pivot)
        let vint_start = rle_size + pointer_block_size;

        let mut current_offset = vint_start;
        for (i, expected_pos) in positions.iter().enumerate() {
            let decoded = decompress_positions(&data, current_offset, expected_pos.len() as u16);
            assert_eq!(decoded, *expected_pos, "position list {} mismatch", i);
            for &pos in expected_pos {
                current_offset += vint_delta_bytes(pos);
            }
        }
    }

    #[test]
    fn test_full_roundtrip_bitmap_with_positions() {
        let docids: Vec<u16> = (0..65536).step_by(16).map(|i| i as u16).collect();
        assert!(docids.len() >= 4096);
        let positions: Vec<Vec<u16>> = docids.iter().map(|_| vec![0, 100, 200]).collect();

        let (data, ctype, _, _) = compress_postinglist(&docids, &positions);
        assert_eq!(ctype, CompressionType::Bitmap);

        let decoded_docids = decompress_docids(&data, ctype, docids.len() as u16);
        let mut sorted_d = decoded_docids;
        sorted_d.sort();
        let mut sorted_o = docids.clone();
        sorted_o.sort();
        assert_eq!(sorted_d, sorted_o);

        // Bitmap: 8192 bytes
        // Position pointers: 4096 entries * 3 bytes each (index >= pivot=4096)
        // Actually posting_count = 4096, pivot = 4096, so all indices are < pivot? No.
        // Indices 0..4095, pivot = 4096, so all use 2-byte pointers.
        let pointer_block_size = docids.len() * 2;
        let vint_start = BITMAP_SIZE + pointer_block_size;

        let mut current_offset = vint_start;
        for expected_pos in positions.iter().take(10) {
            let decoded = decompress_positions(&data, current_offset, expected_pos.len() as u16);
            assert_eq!(decoded, *expected_pos);
            for &pos in expected_pos {
                current_offset += vint_delta_bytes(pos);
            }
        }
    }

    #[test]
    fn test_compress_empty_docids() {
        let docids: Vec<u16> = vec![];
        let positions: Vec<Vec<u16>> = vec![];

        let (data, ctype, pivot, score) = compress_postinglist(&docids, &positions);
        assert_eq!(pivot, 0);
        assert_eq!(score, 0.0);
        // Empty RLE: just 2 bytes for runs_count = 0
        // No position data
        assert!(ctype == CompressionType::Rle || ctype == CompressionType::Array);

        let decoded = decompress_docids(&data, ctype, 0);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_vint_decode_all_byte_sizes() {
        let mut data: Vec<u8> = Vec::new();

        // 1-byte: 42
        data.push(42 | STOP_BIT);

        // 2-byte: 200 = (1 << 7) | 72
        data.push(1);
        data.push(72 | STOP_BIT);

        // 3-byte: 20000 = ((1 << 7) | 28) << 7 | 32
        // 20000 >> 14 = 1, (20000 >> 7) & 0x7F = 28, 20000 & 0x7F = 32
        data.push((20000 >> 14) as u8);
        data.push(((20000 >> 7) & 0x7F) as u8);
        data.push(((20000 & 0x7F) as u8) | STOP_BIT);

        let decoded = decompress_positions(&data, 0, 3);
        assert_eq!(decoded, vec![42, 200, 20000]);
    }

    #[test]
    fn test_rle_roundtrip_preserves_order() {
        let docids = vec![5, 6, 7, 8, 100, 101, 102, 103, 104, 200];
        let positions: Vec<Vec<u16>> = docids.iter().map(|_| vec![0]).collect();

        let (data, ctype, _, _) = compress_postinglist(&docids, &positions);
        assert_eq!(ctype, CompressionType::Rle);

        let decoded = decompress_docids(&data, ctype, docids.len() as u16);
        assert_eq!(decoded, docids);
    }

    #[test]
    fn test_bitmap_decompress_single_docid() {
        let mut bitmap = vec![0u8; BITMAP_SIZE];
        bitmap[0] |= 1; // docid 0
        let decoded = decompress_docids(&bitmap, CompressionType::Bitmap, 1);
        assert_eq!(decoded, vec![0u16]);
    }

    #[test]
    fn test_compress_postinglist_data_layout() {
        // [1,2,3] = 1 run, threshold = min(1,1) = 1 => RLE
        let docids = vec![1, 2, 3];
        let positions = vec![vec![0, 1], vec![0], vec![0, 2]];

        let (data, ctype, _, _) = compress_postinglist(&docids, &positions);
        assert_eq!(ctype, CompressionType::Rle);
        assert_eq!(data.len(), 17); // 6 RLE + 6 pointers + 5 vint

        let decoded_docids = decompress_docids(&data, ctype, 3);
        assert_eq!(decoded_docids, docids);

        let decoded_pos0 = decompress_positions(&data, 12, 2);
        assert_eq!(decoded_pos0, vec![0, 1]);

        let decoded_pos1 = decompress_positions(&data, 14, 1);
        assert_eq!(decoded_pos1, vec![0]);

        let decoded_pos2 = decompress_positions(&data, 15, 2);
        assert_eq!(decoded_pos2, vec![0, 2]);
    }
}
