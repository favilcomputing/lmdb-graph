use gremlite::{
    error::Result,
    graph::{Edge, PValue, Vertex},
    heed::Graph,
};
#[allow(unused_imports)]
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, fs, path::Path};
use strum_macros::EnumString;

#[derive(Serialize, Deserialize, Clone, Debug, EnumString, Eq, PartialEq, Hash)]
enum VertexType {
    Person,
}

#[derive(Serialize, Deserialize, Clone, Debug, EnumString, Eq, PartialEq, Hash)]
enum EdgeType {
    Brother,
}

#[derive(Serialize, Deserialize, Clone, Debug, EnumString, Eq, PartialEq, Hash)]
enum ParameterType {
    Name,
}

fn main() -> Result<()> {
    env_logger::init();

    log::info!("Setting up environment");
    fs::create_dir_all(Path::new("cursor.mdb"))?;

    log::info!("Creating database");
    let graph: Graph<VertexType, EdgeType, ParameterType> = Graph::new(Path::new("cursor.mdb"))?;
    {
        let mut txn = graph.write_txn()?;
        graph.clear(&mut txn)?;
        let phineas = graph.put_vertex(
            &mut txn,
            &Vertex::new(VertexType::Person)
                .set_param(ParameterType::Name, PValue::String("Phineas".into())),
        )?;
        tracing::trace!("phineas");
        let ferb = graph.put_vertex(
            &mut txn,
            &Vertex::new(VertexType::Person)
                .set_param(ParameterType::Name, PValue::String("Ferb".into())),
        )?;
        tracing::trace!("ferb");
        graph.put_vertex(&mut txn, &Vertex::new(VertexType::Person))?;
        tracing::trace!("candace");
        graph.put_vertex(&mut txn, &Vertex::new(VertexType::Person))?;
        tracing::trace!("isabella");
        graph.put_edge(&mut txn, &Edge::new(&phineas, &ferb, EdgeType::Brother)?)?;
        graph.put_edge(&mut txn, &Edge::new(&ferb, &phineas, EdgeType::Brother)?)?;
        txn.commit()?;
        tracing::trace!("put edges");
    }
    {
        let txn = graph.read_txn()?;
        {
            for vertex in graph.vertices(&txn)? {
                log::info!("{:?}", vertex);
            }
            tracing::trace!("vertices");
            for edge in graph.edges(&txn)? {
                log::info!("Edge: {:?}", edge);
            }
            tracing::trace!("edges");
        }
    }

    Ok(())
}
