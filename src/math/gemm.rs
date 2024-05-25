use crate::constants::VECTOR_SIZE;

pub fn gemm(
    a: &Vec<[f32; VECTOR_SIZE]>,
    b: &Vec<[f32; VECTOR_SIZE]>,
    result: &mut Vec<[f32; VECTOR_SIZE]>,
) {
    for i in 0..a.len() {
        for j in 0..VECTOR_SIZE {
            let mut sum = 0.0_f32;
            for k in 0..VECTOR_SIZE {
                sum += a[i][k] * b[k][j];
            }
            result[i][j] = sum;
        }
    }
}
