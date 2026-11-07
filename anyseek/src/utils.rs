use base64::Engine;
use base64::engine::general_purpose;

pub(crate) fn write_u32(value: u32, vec8: &mut [u8], pos: usize) {
    vec8[pos..(pos + 4)].copy_from_slice(&value.to_le_bytes());
}

pub(crate) fn write_i64(value: i64, vec8: &mut [u8], pos: usize) {
    vec8[pos..(pos + 8)].copy_from_slice(&value.to_le_bytes());
}

pub(crate) fn write_f64(value: f64, vec8: &mut [u8], pos: usize) {
    vec8[pos..(pos + 8)].copy_from_slice(&value.to_le_bytes());
}

#[inline]
pub(crate) fn read_u32(vec8: &[u8], pos: usize) -> u32 {
    u32::from_le_bytes(vec8[pos..pos + 4].try_into().unwrap())
}

#[inline]
pub(crate) fn read_i64(vec8: &[u8], pos: usize) -> i64 {
    i64::from_le_bytes(vec8[pos..pos + 8].try_into().unwrap())
}

#[inline]
pub(crate) fn read_f64(vec8: &[u8], pos: usize) -> f64 {
    f64::from_le_bytes(vec8[pos..pos + 8].try_into().unwrap())
}

/// Encodes a byte slice into a base64 string.
pub fn encode_bytes_to_base64_string(input: &[u8]) -> String {
    general_purpose::STANDARD.encode(input)
}

/// Decodes a base64 string into a byte vector.
pub fn decode_bytes_from_base64_string(input: &str) -> Result<Vec<u8>, base64::DecodeError> {
    general_purpose::STANDARD.decode(input)
}
