use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use lmdb_graph::{graph::Node, heed::Graph};
use std::time::Duration;

pub fn bench_nodes(c: &mut Criterion) {
    let f = tempfile::TempDir::new().unwrap();
    let graph = Graph::<String, String>::new(f.path()).unwrap();
    c.bench_function("graph add 1 nodes", |b| {
        b.iter_batched(
            || (0..1).map(|i| Node::new(format!("Node {}", i)).unwrap()),
            |data| {
                let mut txn = graph.write_txn().unwrap();
                for node in data {
                    graph.put_node(&mut txn, &node).unwrap();
                }
                txn.commit().unwrap();
            },
            BatchSize::SmallInput,
        )
    });
    let mut txn = graph.write_txn().unwrap();
    graph.clear(&mut txn).unwrap();
    txn.commit().unwrap();
    c.bench_function("graph add 10 nodes", |b| {
        b.iter_batched(
            || (0..10).map(|i| Node::new(format!("Node {}", i)).unwrap()),
            |data| {
                let mut txn = graph.write_txn().unwrap();
                for node in data {
                    graph.put_node(&mut txn, &node).unwrap();
                }
                txn.commit().unwrap();
            },
            BatchSize::SmallInput,
        )
    });
    let mut txn = graph.write_txn().unwrap();
    graph.clear(&mut txn).unwrap();
    txn.commit().unwrap();
    c.bench_function("graph add 100 nodes", |b| {
        b.iter_batched(
            || (0..100).map(|i| Node::new(format!("Node {}", i)).unwrap()),
            |data| {
                let mut txn = graph.write_txn().unwrap();
                for node in data {
                    graph.put_node(&mut txn, &node).unwrap();
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
    targets = bench_nodes
}
criterion_main!(benches);
