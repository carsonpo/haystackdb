extern crate haystackdb;

use haystackdb::constants::{QUANTIZED_VECTOR_SIZE, VECTOR_SIZE};
use haystackdb::math::gemm;
use haystackdb::math::gemv;
use haystackdb::math::hamming_distance::hamming_distance;

#[cfg(test)]
mod math_tests {

    use super::*;

    // Test the basic functionality of the hamming_distance function
    #[test]
    fn test_hamming_distance() {
        let a: [u8; QUANTIZED_VECTOR_SIZE] = [0xFF; QUANTIZED_VECTOR_SIZE]; // All bits set
        let b: [u8; QUANTIZED_VECTOR_SIZE] = [0x00; QUANTIZED_VECTOR_SIZE]; // All bits unset
        assert_eq!(hamming_distance(&a, &b), QUANTIZED_VECTOR_SIZE as u16 * 8);
    }

    // Test the zero case for hamming_distance
    #[test]
    fn test_zero_hamming_distance() {
        let a: [u8; QUANTIZED_VECTOR_SIZE] = [0xFF; QUANTIZED_VECTOR_SIZE]; // All bits set
        let b: [u8; QUANTIZED_VECTOR_SIZE] = [0xFF; QUANTIZED_VECTOR_SIZE]; // All bits set
        assert_eq!(hamming_distance(&a, &b), 0);
    }

    // Helper function to create identity matrix for testing GEMV and GEMM
    fn identity_matrix(size: usize) -> Vec<[f32; VECTOR_SIZE]> {
        let mut matrix = vec![[0.0; VECTOR_SIZE]; size];
        for i in 0..size {
            matrix[i][i] = 1.0;
        }
        matrix
    }

    // Helper function to create zero matrix for testing GEMV and GEMM
    fn zero_matrix(size: usize) -> Vec<[f32; VECTOR_SIZE]> {
        vec![[0.0; VECTOR_SIZE]; size]
    }

    // Helper function to create a matrix of ones for testing GEMV and GEMM
    fn one_matrix(size: usize) -> Vec<[f32; VECTOR_SIZE]> {
        vec![[1.0; VECTOR_SIZE]; size]
    }

    // Test GEMV with identity matrix
    #[test]
    fn test_gemv_identity() {
        let matrix = identity_matrix(VECTOR_SIZE);
        let vector = [1.0; VECTOR_SIZE];
        let result = gemv::gemv(&matrix, &vector);
        assert_eq!(result, [1.0; VECTOR_SIZE]);
    }

    // Test GEMM with identity matrix
    #[test]
    fn test_gemm_identity() {
        let matrix_a = identity_matrix(VECTOR_SIZE);
        let matrix_b = identity_matrix(VECTOR_SIZE);
        let mut result_matrix = zero_matrix(VECTOR_SIZE);
        gemm::gemm(&matrix_a, &matrix_b, &mut result_matrix);
        assert_eq!(result_matrix, identity_matrix(VECTOR_SIZE));
    }

    // Test GEMV with zero matrix
    #[test]
    fn test_gemv_zero() {
        let matrix = zero_matrix(VECTOR_SIZE);
        let vector = [1.0; VECTOR_SIZE];
        let result = gemv::gemv(&matrix, &vector);
        assert_eq!(result, [0.0; VECTOR_SIZE]);
    }

    // Test GEMM with zero matrix
    #[test]
    fn test_gemm_zero() {
        let matrix_a = zero_matrix(VECTOR_SIZE);
        let matrix_b = zero_matrix(VECTOR_SIZE);
        let mut result_matrix = zero_matrix(VECTOR_SIZE);
        gemm::gemm(&matrix_a, &matrix_b, &mut result_matrix);
        assert_eq!(result_matrix, zero_matrix(VECTOR_SIZE));
    }

    // Test GEMV with one matrix
    #[test]
    fn test_gemv_ones() {
        let matrix = one_matrix(VECTOR_SIZE);
        let vector = [1.0; VECTOR_SIZE];
        let result = gemv::gemv(&matrix, &vector);
        let expected_result: [f32; VECTOR_SIZE] = [VECTOR_SIZE as f32; VECTOR_SIZE];
        assert_eq!(result, expected_result);
    }

    // Test GEMM with one matrix
    #[test]
    fn test_gemm_ones() {
        let matrix_a = one_matrix(VECTOR_SIZE);
        let matrix_b = one_matrix(VECTOR_SIZE);
        let mut result_matrix = zero_matrix(VECTOR_SIZE);
        gemm::gemm(&matrix_a, &matrix_b, &mut result_matrix);
        let expected_result: Vec<[f32; VECTOR_SIZE]> =
            vec![[VECTOR_SIZE as f32; VECTOR_SIZE]; VECTOR_SIZE];
        assert_eq!(result_matrix, expected_result);
    }
}
