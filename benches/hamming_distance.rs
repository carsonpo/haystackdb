extern crate haystackdb;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use haystackdb::constants::QUANTIZED_VECTOR_SIZE;
use haystackdb::math::hamming_distance::hamming_distance;

fn criterion_benchmark(c: &mut Criterion) {
    let a = [0u8; QUANTIZED_VECTOR_SIZE]; // Example data for hamming_distance
    let b = [255u8; QUANTIZED_VECTOR_SIZE]; // Example data for hamming_distance

    c.bench_function("hamming_distance", |bencher| {
        bencher.iter(|| hamming_distance(black_box(&a), black_box(&b)))
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
