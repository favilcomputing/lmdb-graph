use lmdb_graph::{error::Result, graph::{Edge, Node}, heed::Graph};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, fs, path::Path};

#[derive(Serialize, Deserialize, Clone, Debug)]
enum NodeType {
    KV(String, String),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
enum EdgeType {
    Brother,
}

fn main() -> Result<()> {
    env_logger::init();

    log::info!("Setting up environment");
    fs::create_dir_all(Path::new("test.mdb"))?;

    log::info!("Creating database");
    let graph: Graph<NodeType, EdgeType> = Graph::new(Path::new("test.mdb"))?;
    {
        let mut txn = graph.write_txn()?;
        graph.clear(&mut txn)?;
        let phineas = graph.put_node(
            &mut txn,
            &Node::new(NodeType::KV(
                "Phineas".to_string(),
                thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(30)
                    .collect::<String>(),
            ))?,
        )?;
        let ferb = graph.put_node(
            &mut txn,
            &Node::new(NodeType::KV(
                "Ferb".to_string(),
                thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(30)
                    .collect::<String>(),
            ))?,
        )?;
        graph.put_node(
            &mut txn,
            &Node::new(NodeType::KV(
                "Candace".to_string(),
                thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(30)
                    .collect::<String>(),
            ))?,
        )?;
        graph.put_node(
            &mut txn,
            &Node::new(NodeType::KV("Isabella".to_string(), "🍔∈🌏".to_string()))?,
        )?;
        graph.put_edge(
            &mut txn,
            &Edge::new(&phineas, &ferb, EdgeType::Brother)?,
        )?;
        graph.put_edge(
            &mut txn,
            &Edge::new(&ferb, &phineas, EdgeType::Brother)?,
        )?;
        txn.commit()?;
    }
    {
        let txn = graph.read_txn()?;
        {
            for node in graph.nodes(&txn)? {
                log::info!("{:?}", node);
            }
            for edge in graph.edges(&txn)? {
                log::info!("Edge: {:?}", edge);
            }
        }
    }

    Ok(())
}
