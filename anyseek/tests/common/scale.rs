//! Explicit CI fixture scale; invalid values fail instead of silently reducing coverage.
#![allow(dead_code)]
pub fn full() -> bool {
    match std::env::var("ANYSEEK_TEST_SCALE") {
        Ok(value) if value == "full" => true,
        Ok(value) if value == "small" => false,
        Err(std::env::VarError::NotPresent) => false,
        value => panic!("invalid ANYSEEK_TEST_SCALE: {value:?}; expected small or full"),
    }
}
pub fn choose(small: usize, full_size: usize) -> usize {
    if full() { full_size } else { small }
}
