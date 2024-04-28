use crate::constants::VECTOR_SIZE;

pub fn gemv(matrix: &Vec<[f32; VECTOR_SIZE]>, vector: &[f32; VECTOR_SIZE]) -> Vec<f32> {
    let mut result = vec![0f32; VECTOR_SIZE];
    for (i, row) in matrix.iter().enumerate() {
        let mut sum = 0f32;
        for j in 0..VECTOR_SIZE {
            sum += row[j] * vector[j];
        }
        result[i] = sum;
    }
    result
}
