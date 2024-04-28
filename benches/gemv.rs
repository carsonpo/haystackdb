extern crate haystackdb;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use haystackdb::constants::VECTOR_SIZE;
use haystackdb::math::gemv;

fn criterion_benchmark(c: &mut Criterion) {
    let matrix_a = vec![[1.0f32; VECTOR_SIZE]; VECTOR_SIZE]; // Example matrix data for GEMV and GEMM
    let vector = [1.0f32; VECTOR_SIZE]; // Example vector data for GEMV

    c.bench_function("gemv", |bencher| {
        bencher.iter(|| gemv::gemv(black_box(&matrix_a), black_box(&vector)))
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
