extern crate haystackdb;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use haystackdb::constants::VECTOR_SIZE;
use haystackdb::math::gemm;

fn criterion_benchmark(c: &mut Criterion) {
    let matrix_a = vec![[1.0f32; VECTOR_SIZE]; VECTOR_SIZE]; // Example matrix data for GEMV and GEMM
    let matrix_b = vec![[2.0f32; VECTOR_SIZE]; VECTOR_SIZE]; // Example matrix data for GEMV and GEMM
    let mut result_matrix = vec![[0f32; VECTOR_SIZE]; VECTOR_SIZE]; // Placeholder for GEMM output

    c.bench_function("gemm", |bencher| {
        bencher.iter(|| {
            gemm::gemm(
                black_box(&matrix_a),
                black_box(&matrix_b),
                black_box(&mut result_matrix),
            )
        })
    });
}

fn custom_criterion() -> Criterion {
    Criterion::default()
        .warm_up_time(std::time::Duration::from_secs(2))
        .measurement_time(std::time::Duration::from_secs(5)) // Increasing target time to 11 seconds
}

criterion_group! {
    name = benches;
    config = custom_criterion();
    targets = criterion_benchmark
}
criterion_main!(benches);
