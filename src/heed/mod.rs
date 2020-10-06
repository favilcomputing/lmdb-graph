pub mod edge;
pub mod node;

use heed::{types::OwnedSlice, Database, Env, EnvOpenOptions, RoTxn, RwTxn};
use parking_lot::Mutex;
use serde::{de::DeserializeOwned, Serialize};
use std::{fmt::Debug, ops::Deref, path::Path, ptr::null_mut, time::Duration};

use crate::{
    error::{Error, Result},
    graph::{Edge, LogId, Node},
};

use ulid::Generator;

pub struct Graph<NodeT, EdgeT> {
    env: Env,
    generator: Mutex<Generator>,

    pub(crate) node_db: Database<LogId, Node<NodeT>>,
    pub(crate) node_idx_db: Database<OwnedSlice<u8>, LogId>,

    pub(crate) edge_db: Database<LogId, Edge<EdgeT>>,
    pub(crate) edge_idx_db: Database<OwnedSlice<u8>, LogId>,
}

impl<NodeT, EdgeT> Debug for Graph<NodeT, EdgeT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HeedGraph")
    }
}

impl<NodeT: 'static, EdgeT: 'static> Graph<NodeT, EdgeT>
where
    NodeT: Clone + Serialize + DeserializeOwned,
    EdgeT: Clone + Serialize + DeserializeOwned,
{
    pub fn new<T: AsRef<Path>>(path: T) -> Result<Self> {
        let env = EnvOpenOptions::new()
            .max_dbs(200)
            .map_size(2 << 40)
            .open(path)?;
        let generator = Mutex::new(Generator::new());
        let node_db = env.create_database(Some("nodes:v1"))?;
        let node_idx_db = env.create_database(Some("nodes_idx:v1"))?;
        let edge_db = env.create_database(Some("edges:v1"))?;
        let edge_idx_db = env.create_database(Some("edges_idx:v1"))?;
        Ok(Self {
            env,
            generator,

            node_db,
            node_idx_db,
            edge_db,
            edge_idx_db,
        })
    }

    #[inline]
    pub fn write_txn(&self) -> Result<WriteTxn> {
        self.write_txn_wait(Duration::from_secs(30))
    }

    pub fn write_txn_wait(&self, _d: Duration) -> Result<WriteTxn> {
        // TODO: Need to add timeout to heed
        let txn = self.env.write_txn();
        if let Err(heed::Error::Mdb(heed::MdbError::Busy)) = txn {
            return Err(Error::Busy);
        }
        Ok(WriteTxn(txn?))
    }

    pub fn read_txn(&self) -> Result<ReadTxn> {
        let txn = self.env.read_txn()?;
        Ok(ReadTxn(txn))
    }

    pub fn clear(&self, txn: &mut WriteTxn) -> Result<()> {
        self.node_db.clear(&mut txn.0)?;
        self.node_idx_db.clear(&mut txn.0)?;
        self.edge_db.clear(&mut txn.0)?;
        self.edge_idx_db.clear(&mut txn.0)?;
        Ok(())
    }
}

pub struct WriteTxn<'graph>(RwTxn<'graph>);

impl Debug for WriteTxn<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Txn")
    }
}

impl<'graph> WriteTxn<'graph> {
    pub fn commit(self) -> Result<()> {
        Ok(self.0.commit()?)
    }
}

impl Deref for WriteTxn<'_> {
    type Target = RoTxn<()>;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

pub struct ReadTxn(RoTxn);

impl ReadTxn {}

impl Deref for ReadTxn {
    type Target = RoTxn<()>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use rstest::{fixture, rstest};
    use tempfile::TempDir;

    use super::*;
    use crate::{error::Error, graph::Node};
    use parking::Parker;
    use std::{sync::Arc, thread::JoinHandle};

    #[fixture]
    fn tmpdir() -> TempDir {
        TempDir::new().unwrap()
    }

    #[fixture]
    fn graph(tmpdir: TempDir) -> Graph<String, String> {
        Graph::new(tmpdir.path()).unwrap()
    }

    #[fixture]
    fn graphs(tmpdir: TempDir) -> (Graph<String, String>, Graph<String, String>) {
        (
            Graph::new(tmpdir.path()).unwrap(),
            Graph::new(tmpdir.path()).unwrap(),
        )
    }

    #[rstest]
    fn test_mult_trans(graph: Graph<String, String>) -> Result<()> {
        let _w1 = graph.write_txn()?;
        let w2 = graph.write_txn_wait(Duration::from_secs(0));
        match w2 {
            Err(Error::TimedOut(d)) => assert_eq!(d, Duration::from_secs(0)),
            Err(Error::Busy) => {}
            _ => panic!("Not correct error {:?}", w2),
        }
        Ok(())
    }

    #[rstest]
    fn test_mult_trans_threads(graph: Graph<String, String>) -> Result<()> {
        let p1 = Parker::new();
        let u1 = p1.unparker();

        let graph = Arc::new(graph);
        let g1 = graph.clone();
        let t1: JoinHandle<Result<()>> = std::thread::spawn(move || {
            let txn = g1.write_txn_wait(Duration::from_secs(0));
            assert!(txn.is_ok());
            let mut txn = txn?;
            g1.put_node(&mut txn, &Node::new("Test".into())?)?;
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
    fn test_mult_graph(graphs: (Graph<String, String>, Graph<String, String>)) -> Result<()> {
        let graph = graphs.0;
        let graph2 = graphs.1;

        let mut w1 = graph.write_txn().unwrap();

        let w2 = graph2.write_txn_wait(Duration::from_secs(0));
        assert!(w2.is_err());
        match w2 {
            Err(Error::TimedOut(d)) => assert_eq!(d, Duration::from_secs(0)),
            Err(Error::Busy) => {}
            _ => panic!("Not correct error {:?}", w2),
        }

        let n1: Node<String> = graph
            .put_node(&mut w1, &Node::new("n1".into()).unwrap())
            .unwrap();
        w1.commit().unwrap();

        let mut w2: WriteTxn = graph2.write_txn_wait(Duration::from_secs(0)).unwrap();
        let n2: Node<String> = graph2
            .put_node(&mut w2, &Node::new("n2".into()).unwrap())
            .unwrap();
        w2.commit().unwrap();

        let txn = graph.read_txn().unwrap();
        let nodes: Vec<Node<String>> = graph.nodes(&txn).unwrap().collect();
        assert_eq!(nodes.len(), 2);
        let node_values: Vec<String> = nodes.iter().map(|n| n.get_value()).collect();
        let node_ids: Vec<Option<LogId>> = nodes.iter().map(|n| n.get_id()).collect();
        assert_eq!(node_values, vec!["n1", "n2"]);
        assert_eq!(node_ids, vec![n1.get_id(), n2.get_id()]);

        Ok(())
    }

    #[rstest]
    fn test_mult_graph_thread(
        graphs: (Graph<String, String>, Graph<String, String>),
    ) -> Result<()> {
        let graph = Arc::new(graphs.0);
        let g1 = graph.clone();
        let graph2 = graphs.1;
        let p = Parker::new();
        let u = p.unparker();

        let t1 = std::thread::spawn(move || {
            let mut w1 = g1.write_txn().unwrap();
            u.unpark();

            let n1: Node<String> = g1
                .put_node(&mut w1, &Node::new("n1".into()).unwrap())
                .unwrap();
            w1.commit().unwrap();
            // p2.park();
            n1
        });

        let t2 = std::thread::spawn(move || {
            p.park();
            let mut w2 = graph2.write_txn_wait(Duration::from_secs(0)).unwrap();
            // The library handles blocking for the transaction if
            // another graph is open elsewhere.

            let n2: Node<String> = graph2
                .put_node(&mut w2, &Node::new("n2".into()).unwrap())
                .unwrap();
            w2.commit().unwrap();
            n2
        });

        let n1 = t1.join().unwrap();
        let n2 = t2.join().unwrap();
        let txn = graph.read_txn().unwrap();
        let nodes: Vec<Node<String>> = graph.nodes(&txn).unwrap().collect();
        assert_eq!(nodes.len(), 2);
        let node_values: Vec<String> = nodes.iter().map(|n| n.get_value()).collect();
        let node_ids: Vec<Option<LogId>> = nodes.iter().map(|n| n.get_id()).collect();
        assert_eq!(node_values, vec!["n1", "n2"]);
        assert_eq!(node_ids, vec![n1.get_id(), n2.get_id()]);

        Ok(())
    }
}
