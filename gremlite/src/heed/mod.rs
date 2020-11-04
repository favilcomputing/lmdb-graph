pub mod edge;
pub mod vertex;

use heed::{BytesDecode, BytesEncode, Database, Env, EnvOpenOptions, RoTxn, RwTxn};
use parking_lot::Mutex;
use postcard::{from_bytes, to_stdvec};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{borrow::Cow, fmt::Debug, path::Path, time::Duration};
use tracing::instrument;

use crate::{
    error::{Error, Result},
    graph::{parameter::PValue, Edge, Id, Vertex, Writable},
    gremlin::{terminator::TraversalTerminator, GraphTraversalSource},
};

use ulid::Generator;

#[derive(Serialize, Deserialize)]
pub struct LabelId<Label>(
    #[serde(bound(deserialize = "Label: DeserializeOwned"))] Label,
    Id,
)
where
    Label: Writable;

impl<'a, Parameter> BytesEncode<'a> for LabelId<Parameter>
where
    Parameter: 'a + Writable,
{
    type EItem = Self;
    fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        to_stdvec(item).map(Cow::Owned).ok()
    }
}

impl<'a, Parameter> BytesDecode<'a> for LabelId<Parameter>
where
    Parameter: 'a + Writable,
{
    type DItem = Self;
    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        from_bytes(bytes).ok()
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct IdParam<Parameter>(
    Id,
    #[serde(bound(deserialize = "Parameter: DeserializeOwned"))] Parameter,
)
where
    Parameter: Writable;

impl<'a, Parameter> BytesEncode<'a> for IdParam<Parameter>
where
    Parameter: 'a + Writable,
{
    type EItem = Self;
    fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        to_stdvec(item).map(Cow::Owned).ok()
    }
}

impl<'a, Parameter> BytesDecode<'a> for IdParam<Parameter>
where
    Parameter: 'a + Writable,
{
    type DItem = Self;
    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        from_bytes(bytes).ok()
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct ParamId<P>(#[serde(bound(deserialize = "P: DeserializeOwned"))] P, Id)
where
    P: Writable;

impl<'a, V> BytesEncode<'a> for ParamId<V>
where
    V: 'a + Writable,
{
    type EItem = Self;
    fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        to_stdvec(item).map(Cow::Owned).ok()
    }
}

impl<'a, V> BytesDecode<'a> for ParamId<V>
where
    V: 'a + Writable,
{
    type DItem = Self;
    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        from_bytes(bytes).ok()
    }
}

pub struct Graph<V = String, E = String, P = String>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable,
{
    env: Env,
    generator: Mutex<Generator>,

    pub(crate) vertex_db: Database<Id, Vertex<V, E, P>>,
    pub(crate) vertex_idx_db: Database<LabelId<V>, Id>,

    pub(crate) edge_db: Database<Id, Edge<V, E, P>>,
    pub(crate) edge_idx_db: Database<LabelId<E>, Id>,

    pub(crate) parameters_db: Database<IdParam<P>, PValue<V, E, P>>,
    pub(crate) parameters_idx_db: Database<ParamId<P>, Id>,
    // TODO: Create a collection of databases that can be used as indices
}

impl<V, E, P> Debug for Graph<V, E, P>
where
    V: Writable,
    E: Writable,
    P: Writable,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HeedGraph")
    }
}

impl<V, E, P> Graph<V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable,
{
    #[instrument]
    pub fn new<T: AsRef<Path> + Debug>(path: T) -> Result<Self> {
        let env = EnvOpenOptions::new()
            .max_dbs(200)
            .map_size(2 << 40)
            .open(path)?;
        let generator = Mutex::new(Generator::new());
        let vertex_db = env.create_database(Some("vertices:v1"))?;
        let vertex_idx_db = env.create_database(Some("vertices_idx:v1"))?;
        let edge_db = env.create_database(Some("edges:v1"))?;
        let edge_idx_db = env.create_database(Some("edges_idx:v1"))?;

        let parameters_db = env.create_database(Some("parameters:v1"))?;
        let parameters_idx_db = env.create_database(Some("parameters_idx:v1"))?;
        Ok(Self {
            env,
            generator,

            vertex_db,
            vertex_idx_db,
            edge_db,
            edge_idx_db,

            parameters_db,
            parameters_idx_db,
        })
    }

    #[inline]
    pub fn write_txn(&self) -> Result<RwTxn> {
        self.write_txn_wait(Duration::from_secs(30))
    }

    #[instrument]
    pub fn write_txn_wait(&self, _d: Duration) -> Result<RwTxn> {
        // TODO: Need to add timeout to heed
        let txn = self.env.write_txn();
        if let Err(heed::Error::Mdb(heed::MdbError::Busy)) = txn {
            return Err(Error::Busy);
        }
        Ok(txn?)
    }

    #[instrument]
    pub fn read_txn(&self) -> Result<RoTxn> {
        let txn = self.env.read_txn()?;
        Ok(txn)
    }

    pub fn clear(&self, txn: &mut RwTxn) -> Result<()> {
        self.vertex_db.clear(txn)?;
        self.vertex_idx_db.clear(txn)?;
        self.edge_db.clear(txn)?;
        self.edge_idx_db.clear(txn)?;
        Ok(())
    }

    pub fn traversal<'graph>(&'graph self) -> GraphTraversalSource<'graph, V, E, P> {
        GraphTraversalSource::new(&self)
    }

    pub(crate) fn terminator(&self) -> TraversalTerminator<V, E, P> {
        TraversalTerminator::new(self)
    }
}

#[cfg(test)]
mod tests {
    use rstest::{fixture, rstest};
    use tempfile::TempDir;

    use super::*;
    use crate::{
        error::Error,
        graph::{
            parameter::{FromPValue, ToPValue},
            Type, Vertex,
        },
        gremlin::TraversalSource,
    };
    use parking::Parker;
    use std::{sync::Arc, thread::JoinHandle};

    #[fixture]
    fn tmpdir() -> TempDir {
        TempDir::new().unwrap()
    }

    #[fixture]
    fn graph(tmpdir: TempDir) -> Graph<String, String, ()> {
        Graph::new(tmpdir.path()).unwrap()
    }

    #[fixture]
    fn graphs(tmpdir: TempDir) -> (Graph<String, String, ()>, Graph<String, String, ()>) {
        (
            Graph::new(tmpdir.path()).unwrap(),
            Graph::new(tmpdir.path()).unwrap(),
        )
    }

    #[rstest]
    fn test_mult_trans(graph: Graph<String, String, ()>) -> Result<()> {
        let _w1 = graph.write_txn()?;
        let w2 = graph.write_txn_wait(Duration::from_secs(0));
        match w2 {
            Err(Error::TimedOut(d)) => assert_eq!(d, Duration::from_secs(0)),
            Err(Error::Busy) => {}
            _ => panic!("Not correct error"),
        }
        Ok(())
    }

    #[rstest]
    fn test_mult_trans_threads(graph: Graph<String, String, ()>) -> Result<()> {
        let p1 = Parker::new();
        let u1 = p1.unparker();

        let graph = Arc::new(graph);
        let g1 = graph.clone();
        let t1: JoinHandle<Result<()>> = std::thread::spawn(move || {
            let txn = g1.write_txn_wait(Duration::from_secs(0));
            assert!(txn.is_ok());
            let mut txn = txn?;
            g1.put_vertex(&mut txn, &Vertex::new("Test".into()))?;
            u1.unpark();

            // Can't park here, because this will deadlock due to
            // mutex inside heed library
            txn.commit()?;
            Ok(())
        });

        p1.park();
        let w2 = graph.write_txn_wait(Duration::from_secs(0));
        assert!(w2.is_ok());
        t1.join().unwrap().unwrap();
        Ok(())
    }

    #[rstest]
    fn test_mult_graph(
        graphs: (Graph<String, String, ()>, Graph<String, String, ()>),
    ) -> Result<()> {
        let graph = graphs.0;
        let graph2 = graphs.1;

        let mut w1 = graph.write_txn().unwrap();

        let w2 = graph2.write_txn_wait(Duration::from_secs(0));
        assert!(w2.is_err());
        match w2 {
            Err(Error::TimedOut(d)) => assert_eq!(d, Duration::from_secs(0)),
            Err(Error::Busy) => {}
            _ => panic!("Not correct error"),
        }

        let v1: Vertex<String, String, ()> = graph
            .put_vertex(&mut w1, &Vertex::new("n1".into()))
            .unwrap();
        w1.commit().unwrap();

        let mut w2 = graph2.write_txn_wait(Duration::from_secs(0)).unwrap();
        let v2: Vertex<String, String, ()> = graph2
            .put_vertex(&mut w2, &Vertex::new("n2".into()))
            .unwrap();
        w2.commit().unwrap();

        let txn = graph.read_txn().unwrap();
        let vertices: Vec<Vertex<String, String, ()>> = graph
            .vertices(&txn)
            .unwrap()
            .map(Vertex::from_pvalue)
            .map(Result::unwrap)
            .collect();
        assert_eq!(vertices.len(), 2);
        let vertex_values: Vec<String> = vertices.iter().map(|n| n.get_label()).collect();
        let vertex_ids: Vec<Option<Id>> = vertices.iter().map(|n| n.get_id()).collect();
        assert_eq!(vertex_values, vec!["n1", "n2"]);
        assert_eq!(vertex_ids, vec![v1.get_id(), v2.get_id()]);

        Ok(())
    }

    #[rstest]
    fn test_mult_graph_thread(
        graphs: (Graph<String, String, ()>, Graph<String, String, ()>),
    ) -> Result<()> {
        let graph = Arc::new(graphs.0);
        let g1 = graph.clone();
        let graph2 = graphs.1;
        let p = Parker::new();
        let u = p.unparker();

        let t1 = std::thread::spawn(move || {
            let mut w1 = g1.write_txn().unwrap();
            u.unpark();

            let n1: Vertex<String, String, ()> =
                g1.put_vertex(&mut w1, &Vertex::new("n1".into())).unwrap();
            w1.commit().unwrap();
            // p2.park();
            n1
        });

        let t2 = std::thread::spawn(move || {
            p.park();
            let mut w2 = graph2.write_txn_wait(Duration::from_secs(0)).unwrap();
            // The library handles blocking for the transaction if
            // another graph is open elsewhere.

            let n2: Vertex<String, String, ()> = graph2
                .put_vertex(&mut w2, &Vertex::new("n2".into()))
                .unwrap();
            w2.commit().unwrap();
            n2
        });

        let n1 = t1.join().unwrap();
        let n2 = t2.join().unwrap();
        let txn = graph.read_txn().unwrap();
        let vertices: Vec<Vertex<String, String, ()>> = graph
            .vertices(&txn)
            .unwrap()
            .map(Vertex::from_pvalue)
            .map(Result::unwrap)
            .collect();
        assert_eq!(vertices.len(), 2);
        let vertex_values: Vec<String> = vertices.iter().map(|n| n.get_label()).collect();
        let vertex_ids: Vec<Option<Id>> = vertices.iter().map(|n| n.get_id()).collect();
        assert_eq!(vertex_values, vec!["n1", "n2"]);
        assert_eq!(vertex_ids, vec![n1.get_id(), n2.get_id()]);

        Ok(())
    }

    #[rstest]
    fn test_traversal(graph: Graph<String, String, ()>) -> Result<()> {
        let vertex = Vertex::new("test".to_string());
        let mut txn = graph.write_txn().unwrap();
        let returned = graph.put_vertex(&mut txn, &vertex.clone()).unwrap();
        txn.commit()?;

        let vs = {
            let g = graph.traversal();
            let mut txn = graph.write_txn()?;
            g.v(()).to_list(&mut txn).unwrap()
        };
        assert_eq!(vs.len(), 1);
        assert_eq!(vs[0], returned.to_pvalue());

        let vs = {
            let g = graph.traversal();
            let mut txn = graph.write_txn()?;
            g.v(returned.id.unwrap()).to_list(&mut txn).unwrap()
        };
        assert_eq!(vs.len(), 1);
        assert_eq!(vs[0], returned.to_pvalue());

        let vs = {
            let g = graph.traversal();
            let mut txn = graph.write_txn()?;
            g.v(Id::nil(Type::Vertex)).to_list(&mut txn).unwrap()
        };
        assert_eq!(vs.len(), 0);

        Ok(())
    }
}
