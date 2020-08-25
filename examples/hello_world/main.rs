use lmdb_graph::{error::Result, graph::*, lmdb::graph::LmdbGraph};

#[allow(unused_mut)]
fn main() -> Result<()> {
    env_logger::init();

    log::info!("Setting up graph");
    let mut graph = unsafe { LmdbGraph::new("test.db") }?;
    let mut txn: lmdb_graph::lmdb::graph::LmdbWriteTransaction = graph.write_transaction()?;
    txn.put_node(Node::new("name", "Phineas".to_string())?)?;
    txn.put_node(Node::new("name", "Ferb".to_string())?)?;
    txn.put_node(Node::new("name", "Candace".to_string())?)?;
    txn.put_node(Node::new("name", "Isabella".to_string())?)?;
    txn.commit()?;

    let txn = graph.read_transaction()?;
    let isabella = txn.get_node_by_value(&Node::new("name","Isabella".to_string())?)?;
    log::info!("Found node: {:?}", isabella);

    Ok(())
}
