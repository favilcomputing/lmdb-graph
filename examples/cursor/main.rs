use lmdb_graph::{error::Result, graph::Node, heed::Graph};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::{
    fmt::Debug,
    fs,
    path::Path,
};

#[derive(Serialize, Deserialize, Clone, Debug)]
enum NodeType {
    KV(String, String),
}

fn main() -> Result<()> {
    log::info!("Setting up environment");
    fs::create_dir_all(Path::new("zerocopy.mdb"))?;

    log::info!("Creating database");
    let graph = Graph::new(Path::new("zerocopy.mdb"))?;
    {
        let mut txn = graph.write_txn()?;
        graph.clear(&mut txn)?;
        graph.put_node(
            &mut txn,
            Node::new(NodeType::KV(
                "Phineas".to_string(),
                thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(30)
                    .collect::<String>(),
            ))?,
        )?;
        graph.put_node(
            &mut txn,
            Node::new(NodeType::KV(
                "Ferb".to_string(),
                thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(30)
                    .collect::<String>(),
            ))?,
        )?;
        graph.put_node(
            &mut txn,
            Node::new(NodeType::KV(
                "Candace".to_string(),
                thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(30)
                    .collect::<String>(),
            ))?,
        )?;
        graph.put_node(
            &mut txn,
            Node::new(NodeType::KV("Isabella".to_string(), "🍔∈🌏".to_string()))?,
        )?;
        txn.commit()?;
    }
    {
        let txn = graph.read_txn()?;
        {
            for ret in graph.nodes(&txn)? {
                println!("{:?}", ret);
            }
        }
    }

    Ok(())
}
