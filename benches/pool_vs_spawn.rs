use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::hash::Hasher;
use std::thread;

use schedulite_rs::{PoolBuilder, SchedulerMode};

fn hash_work(seed: u64, rounds: u32) -> u64 {
    let mut acc = seed;
    for i in 0..rounds {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        std::hash::Hash::hash(&acc, &mut hasher);
        std::hash::Hash::hash(&i, &mut hasher);
        acc = hasher.finish();
    }
    acc
}

fn bench_group(c: &mut Criterion, name: &str, workers: usize, tasks: usize) {
    let mut group = c.benchmark_group(name);
    group.sample_size(10);

    group.bench_function("raw_spawn", |b| {
        b.iter(|| {
            let handles: Vec<_> = (0..tasks)
                .map(|i| {
                    thread::spawn(move || {
                        black_box(hash_work(i as u64, 10));
                    })
                })
                .collect();
            for h in handles {
                h.join().unwrap();
            }
        });
    });

    group.bench_function("fifo", |b| {
        b.iter(|| {
            let pool = PoolBuilder::new().workers(workers).build();
            for i in 0..tasks {
                pool.submit(move || {
                    black_box(hash_work(i as u64, 10));
                })
                .unwrap();
            }
            drop(pool);
        });
    });

    group.bench_function("steal", |b| {
        b.iter(|| {
            let pool = PoolBuilder::new()
                .workers(workers)
                .mode(SchedulerMode::Steal)
                .build();
            for i in 0..tasks {
                pool.submit(move || {
                    black_box(hash_work(i as u64, 10));
                })
                .unwrap();
            }
            drop(pool);
        });
    });

    group.finish();
}

fn benchmarks(c: &mut Criterion) {
    bench_group(c, "small_4w_100t", 4, 100);
    bench_group(c, "medium_4w_1000t", 4, 1000);
    bench_group(c, "large_8w_2000t", 8, 2000);
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
