#[allow(unused_imports)]
use lmdb_graph::{
    error::Result,
    graph::{Edge, Node},
    heed::Graph,
};
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Debug, Serialize, Deserialize, Clone)]
enum NodeType {
    Name(String),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
enum EdgeType {
    Sibling,
}

#[allow(unused_mut)]
fn main() -> Result<()> {
    env_logger::init();

    log::info!("Setting up graph");
    fs::create_dir_all("test.mdb")?;
    let graph = Graph::new("test.db")?;
    let mut txn = graph.write_txn()?;
    let n = graph.get_node_by_value(&txn, &NodeType::Name("Phineas".to_string()))?;
    if n.is_some() {
        log::info!("Phineas found, clearing database");
        graph.clear(&mut txn)?;
    }
    let phineas = graph.put_node(&mut txn, &Node::new(NodeType::Name("Phineas".to_string()))?)?;
    let ferb = graph.put_node(&mut txn, &Node::new(NodeType::Name("Ferb".to_string()))?)?;
    graph.put_node(&mut txn, &Node::new(NodeType::Name("Candace".to_string()))?)?;
    graph.put_node(
        &mut txn,
        &Node::new(NodeType::Name("Isabella".to_string()))?,
    )?;

    let edge = Edge::new(
        phineas.get_id().unwrap(),
        ferb.get_id().unwrap(),
        EdgeType::Sibling,
    )?;

    graph.put_edge(&mut txn, &edge)?;

    txn.commit()?;

    let txn = graph.read_txn()?;
    let isabella = graph.get_node_by_value(&txn, &NodeType::Name("Isabella".to_string()))?;
    log::info!("Found node: {:?}", isabella);

    Ok(())
}
