use lmdb_graph::{error::Result, graph::*, lmdb::graph::LmdbGraph};
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Debug, Serialize, Deserialize, Clone)]
enum Type {
    Name,
}

#[allow(unused_mut)]
fn main() -> Result<()> {
    env_logger::init();

    log::info!("Setting up graph");
    fs::create_dir_all("test.db")?;
    let mut graph = unsafe { LmdbGraph::new("test.db") }?;
    let mut txn: lmdb_graph::lmdb::graph::LmdbWriteTransaction = graph.write_transaction()?;
    let n = txn.get_node_by_value(&Node::new(Type::Name, "Phineas".to_string())?)?;
    if n.is_some() {
        log::info!("Phineas found, clearing database");
        txn.clear()?;
    }
    txn.put_node(Node::new(Type::Name, "Phineas".to_string())?)?;
    txn.put_node(Node::new(Type::Name, "Ferb".to_string())?)?;
    txn.put_node(Node::new(Type::Name, "Candace".to_string())?)?;
    txn.put_node(Node::new(Type::Name, "Isabella".to_string())?)?;
    txn.commit()?;

    let txn = graph.read_transaction()?;
    let isabella = txn.get_node_by_value(&Node::new(Type::Name, "Isabella".to_string())?)?;
    log::info!("Found node: {:?}", isabella);

    Ok(())
}
