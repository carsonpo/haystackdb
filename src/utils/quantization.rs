use crate::constants::{QUANTIZED_VECTOR_SIZE, VECTOR_SIZE};

pub fn quantize(vec: &[f32; VECTOR_SIZE]) -> [u8; QUANTIZED_VECTOR_SIZE] {
    let mut result = [0; QUANTIZED_VECTOR_SIZE];
    for i in 0..QUANTIZED_VECTOR_SIZE {
        for j in 0..8 {
            result[i] |= ((vec[i * 8 + j] >= 0.0) as u8) << j;
        }
    }
    result
}

pub fn dequantize(vec: &[u8; QUANTIZED_VECTOR_SIZE]) -> [f32; VECTOR_SIZE] {
    let mut result = [0.0; VECTOR_SIZE];
    for i in 0..QUANTIZED_VECTOR_SIZE {
        for j in 0..8 {
            result[i * 8 + j] = if (vec[i] & (1 << j)) > 0 { 1.0 } else { -1.0 };
        }
    }
    result
}
