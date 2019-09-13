#[macro_use]
extern crate criterion;
extern crate kvs;

use std::path::Path;
use rand::prelude::*;
use rand::{Rng, SeedableRng};
use criterion::{BatchSize, BenchmarkId, Criterion, black_box};
use tempfile::TempDir;

use kvs::{KvStore, SledKvsEngine, KvsEngine};

static SET_ITERATION_COUNT: usize = 100;
static GET_ITERATION_COUNT: usize = 100;
// static MAX_KEY_SIZE: usize = 100000;
// static MAX_VALUE_SIZE: usize = 100000;
static MAX_KEY_SIZE: usize = 1000;
static MAX_VALUE_SIZE: usize = 1000;

pub fn kvs_set_benchmark(c: &mut Criterion) {
    let seed = [0; 32];
    let mut rng: StdRng = SeedableRng::from_seed(seed);
    let values: Vec<(String, String)> = (0..SET_ITERATION_COUNT).map(|_| {
        let key_size = rng.gen_range(0, MAX_KEY_SIZE);
        let value_size = rng.gen_range(0, MAX_VALUE_SIZE);
        let mut key = vec![0u8; key_size];
        let mut value = vec![0u8; value_size];
        rng.fill_bytes(&mut key);
        rng.fill_bytes(&mut value);
        (format!("k_{}", String::from_utf8_lossy(&key)), format!("v_{}", String::from_utf8_lossy(&value)))
    }).collect(); 

    let mut group = c.benchmark_group("set");
    let set_value = |(mut store, _temp_dir): (Box<dyn KvsEngine>, TempDir)| {
        for (k, v) in &values {
            store.set(black_box(k.to_owned()), black_box(v.to_owned())).expect("KvStore set failed");
        }
    };

    group.bench_function("kv set", |b| b.iter_batched(
        || {
            let temp_dir = TempDir::new().expect("unable to create temporary working directory");
            let kv_store = Box::new(KvStore::open(temp_dir.path()).expect("can't open KvStore"));
            // Don't drop temp_dir so that it doesn't delete the dir
            (kv_store, temp_dir)
        }, set_value,
        BatchSize::SmallInput
    ));

    group.bench_function("sled set", |b| b.iter_batched(
        || {
            let temp_dir = TempDir::new().expect("unable to create temporary working directory");
            let sled_store = Box::new(SledKvsEngine::open(temp_dir.path()).expect("can't open sled db"));
            // Don't drop temp_dir so that it doesn't delete the dir
            (sled_store, temp_dir)
        }, set_value,
        BatchSize::SmallInput
    ));

    group.finish();
}

pub fn kvs_get_benchmark(c: &mut Criterion) {
    let seed = [0; 32];
    let mut rng: StdRng = SeedableRng::from_seed(seed);
    let values: Vec<(String, String)> = (0..GET_ITERATION_COUNT).map(|_| {
        let key_size = rng.gen_range(0, MAX_KEY_SIZE);
        let value_size = rng.gen_range(0, MAX_VALUE_SIZE);
        let mut key = vec![0u8; key_size];
        let mut value = vec![0u8; value_size];
        rng.fill_bytes(&mut key);
        rng.fill_bytes(&mut value);
        (format!("k_{}", String::from_utf8_lossy(&key)), format!("v_{}", String::from_utf8_lossy(&value)))
    }).collect(); 

    let mut group = c.benchmark_group("get");
    let set_value = |mut store: Box<dyn KvsEngine>| {
        for (k, v) in &values {
            store.set(black_box(k.to_owned()), black_box(v.to_owned())).expect("KvStore set failed");
        }
        store
    };

    let get_value = |(mut store, _temp_dir): (Box<dyn KvsEngine>, TempDir)| {
        for (k, v) in &values {
            store.get(black_box(k.to_owned())).expect("failed to fetch key");
        }
    };

    group.bench_function("kv get", |b| b.iter_batched(
        || {
            let temp_dir = TempDir::new().expect("unable to create temporary working directory");
            let kv_store = Box::new(KvStore::open(temp_dir.path()).expect("can't open KvStore"));
            
            let kv_store = set_value(kv_store);
            // Don't drop temp_dir so that it doesn't delete the dir
            (kv_store, temp_dir)
        }, get_value,
        BatchSize::SmallInput
    ));

    group.bench_function("sled get", |b| b.iter_batched(
        || {
            let temp_dir = TempDir::new().expect("unable to create temporary working directory");
            let sled_store = Box::new(SledKvsEngine::open(temp_dir.path()).expect("can't open sled db"));
            // Don't drop temp_dir so that it doesn't delete the dir
            (sled_store, temp_dir)
        }, get_value,
        BatchSize::SmallInput
    ));

    group.finish();
}

criterion_group!(benches, kvs_set_benchmark, kvs_get_benchmark);
criterion_main!(benches);
