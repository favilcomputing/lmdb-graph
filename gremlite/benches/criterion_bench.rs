use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use lmdb_graph::{graph::Vertex, heed::Graph};
use std::time::Duration;

pub fn bench_vertices(c: &mut Criterion) {
    let f = tempfile::TempDir::new().unwrap();
    let graph = Graph::<String, String, ()>::new(f.path()).unwrap();
    c.bench_function("graph add 1 vertices", |b| {
        b.iter_batched(
            || (0..1).map(|i| Vertex::new(format!("Vertex {}", i)).unwrap()),
            |data| {
                let mut txn = graph.write_txn().unwrap();
                for vertex in data {
                    graph.put_vertex(&mut txn, &vertex).unwrap();
                }
                txn.commit().unwrap();
            },
            BatchSize::SmallInput,
        )
    });
    let mut txn = graph.write_txn().unwrap();
    graph.clear(&mut txn).unwrap();
    txn.commit().unwrap();
    c.bench_function("graph add 10 vertices", |b| {
        b.iter_batched(
            || (0..10).map(|i| Vertex::new(format!("Vertex {}", i)).unwrap()),
            |data| {
                let mut txn = graph.write_txn().unwrap();
                for vertex in data {
                    graph.put_vertex(&mut txn, &vertex).unwrap();
                }
                txn.commit().unwrap();
            },
            BatchSize::SmallInput,
        )
    });
    let mut txn = graph.write_txn().unwrap();
    graph.clear(&mut txn).unwrap();
    txn.commit().unwrap();
    c.bench_function("graph add 100 vertices", |b| {
        b.iter_batched(
            || (0..100).map(|i| Vertex::new(format!("Vertex {}", i)).unwrap()),
            |data| {
                let mut txn = graph.write_txn().unwrap();
                for vertex in data {
                    graph.put_vertex(&mut txn, &vertex).unwrap();
                }
                txn.commit().unwrap();
            },
            BatchSize::LargeInput,
        )
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(100).measurement_time(Duration::from_secs(30));
    targets = bench_vertices
}
criterion_main!(benches);
