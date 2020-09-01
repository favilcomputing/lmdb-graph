use lmdb_graph::{
    error::Result,
    graph::{Edge, Graph, Node, ReadTransaction, WriteTransaction},
    lmdb::{LmdbGraph, LmdbWriteTransaction},
};
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Debug, Serialize, Deserialize, Clone)]
enum NodeType {
    Name,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
enum EdgeType {
    Sibling,
}

#[allow(unused_mut)]
fn main() -> Result<()> {
    env_logger::init();

    log::info!("Setting up graph");
    fs::create_dir_all("test.db")?;
    let mut graph = unsafe { LmdbGraph::new("test.db") }?;
    let mut txn: LmdbWriteTransaction = graph.write_transaction()?;
    let n = txn.get_node_by_value(&Node::new(NodeType::Name, "Phineas".to_string())?)?;
    if n.is_some() {
        log::info!("Phineas found, clearing database");
        txn.clear()?;
    }
    let phineas = txn.put_node(Node::new(NodeType::Name, "Phineas".to_string())?)?;
    let ferb = txn.put_node(Node::new(NodeType::Name, "Ferb".to_string())?)?;
    txn.put_node(Node::new(NodeType::Name, "Candace".to_string())?)?;
    txn.put_node(Node::new(NodeType::Name, "Isabella".to_string())?)?;

    let edge = Edge::new(
        phineas.get_id().unwrap(),
        ferb.get_id().unwrap(),
        EdgeType::Sibling,
        (),
    )?;

    txn.put_edge(edge)?;

    txn.commit()?;

    let txn = graph.read_transaction()?;
    let isabella = txn.get_node_by_value(&Node::new(NodeType::Name, "Isabella".to_string())?)?;
    log::info!("Found node: {:?}", isabella);

    Ok(())
}
