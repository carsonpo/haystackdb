use crate::constants::QUANTIZED_VECTOR_SIZE as ARRAY_SIZE;

// #[cfg(not(target_arch = "aarch64"))]
pub fn hamming_distance(a: &[u8; ARRAY_SIZE], b: &[u8; ARRAY_SIZE]) -> u16 {
    a.iter()
        .zip(b.iter())
        .fold(0, |acc, (&x, &y)| acc + (x ^ y).count_ones() as u16)
}
