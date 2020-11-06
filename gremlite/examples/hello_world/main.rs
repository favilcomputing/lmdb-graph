#[allow(unused_imports)]
use gremlite::{
    error::Result,
    graph::{Edge, PValue, Vertex, Writable},
    gremlin::TraversalSource,
    heed::Graph,
};
use serde::{Deserialize, Serialize};
use std::fs;
use strum_macros::EnumString;

#[derive(Serialize, Deserialize, Clone, Debug, EnumString, Eq, PartialEq, Hash)]
enum VertexType {
    Name(String),
}

impl Writable for VertexType {}

#[derive(Serialize, Deserialize, Clone, Debug, EnumString, Eq, PartialEq, Hash)]
enum EdgeType {
    Sibling,
}

impl Writable for EdgeType {}

#[allow(unused_mut)]
fn main() -> Result<()> {
    env_logger::init();

    log::info!("Creating directory");
    fs::create_dir_all("test.mdb")?;
    log::info!("Setting up graph");
    let graph = Graph::<_, _, String>::new("test.mdb")?;
    let mut txn = graph.write_txn()?;
    let n = graph.get_vertex_by_label(&txn, &VertexType::Name("Phineas".to_string()))?;
    if n.is_some() {
        log::info!("Phineas found, clearing database");
        graph.clear(&mut txn)?;
    }
    let phineas = graph.put_vertex(
        &mut txn,
        &Vertex::new(VertexType::Name("Phineas".to_string())),
    )?;
    let ferb = graph.put_vertex(&mut txn, &Vertex::new(VertexType::Name("Ferb".to_string())))?;
    graph.put_vertex(
        &mut txn,
        &Vertex::new(VertexType::Name("Candace".to_string())),
    )?;
    graph.put_vertex(
        &mut txn,
        &Vertex::new(VertexType::Name("Isabella".to_string())),
    )?;

    let edge = Edge::new(&phineas, &ferb, EdgeType::Sibling)?;

    graph.put_edge(&mut txn, &edge)?;

    txn.commit()?;

    let (vs, es) = graph.write_traversal(|g, mut txn| {
        let vs = g.v(()).to_list(&mut txn)?;
        let es = g.e(()).to_list(&mut txn)?;
        Ok((vs, es))
    })?;
    for v in vs {
        log::info!("Found vertex: {:#?}", v);
    }
    for e in es {
        log::info!("Found edge: {:#?}", e);
    }

    Ok(())
}
