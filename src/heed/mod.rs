pub mod node;

use heed::{types::OwnedSlice, Database, Env, EnvOpenOptions, RoTxn, RwTxn};
use serde::{de::DeserializeOwned, Serialize};
use std::{fmt::Debug, ops::Deref, path::Path, sync::Mutex};

use self::node::NodeIter;
use crate::{
    error::Result,
    graph::{Edge, LogId, Node, ToDB},
};
use node::NodeRange;
use ulid::Generator;

pub struct Graph<T> {
    pub(crate) env: Env,
    generator: Mutex<Generator>,

    pub(crate) node_db: Database<LogId, Node<T>>,
    pub(crate) node_idx_db: Database<OwnedSlice<u8>, LogId>,

    pub(crate) edge_db: Database<LogId, Edge<T>>,
    pub(crate) edge_idx_db: Database<OwnedSlice<u8>, LogId>,
}

impl<T> Debug for Graph<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HeedGraph")
    }
}

impl<Value: 'static> Graph<Value>
where
    Value: Clone + Serialize + DeserializeOwned,
{
    pub fn new<T: AsRef<Path>>(path: T) -> Result<Self> {
        let env = EnvOpenOptions::new().max_dbs(200).open(path)?;
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

    pub fn write_txn(&self) -> Result<WriteTxn> {
        let txn = self.env.write_txn()?;
        Ok(WriteTxn(txn))
    }

    pub fn read_txn(&self) -> Result<ReadTxn> {
        let txn = self.env.read_txn()?;
        Ok(ReadTxn(txn))
    }

    pub fn put_node(&self, txn: &mut WriteTxn, n: Node<Value>) -> Result<Node<Value>> {
        let n = if n.id.is_some() {
            let node: Option<Node<Value>> = self.node_db.get(&txn, &n.id.unwrap())?;
            if let Some(node) = node {
                self.node_idx_db.delete(&mut txn.0, &node.rev_to_db()?)?;
            }
            n
        } else {
            let id = LogId::new(&mut self.generator.lock().unwrap())?;
            Node { id: Some(id), ..n }
        };
        self.node_db.put(&mut txn.0, n.id.as_ref().unwrap(), &n)?;
        let rev = &n.rev_to_db()?;
        self.node_idx_db
            .put(&mut txn.0, rev, n.id.as_ref().unwrap())?;

        Ok(n)
    }

    pub fn get_node_by_id<Txn>(&self, txn: &Txn, id: &LogId) -> Result<Option<Node<Value>>>
    where
        Txn: Deref<Target = RoTxn>,
    {
        let node = self.node_db.get(&txn, &id)?;
        Ok(node)
    }

    pub fn get_nodes_by_value<'txn, Txn: 'txn>(
        &self,
        txn: &'txn Txn,
        value: &Value,
    ) -> Result<NodeRange<'txn, Value>>
    where
        Txn: Deref<Target = RoTxn>,
        Value: Clone,
    {
        let prefix = Node::new(value.clone())?.value_to_db()?;
        let iter = self.node_idx_db.prefix_iter(&txn, &prefix)?;
        Ok(NodeRange::new(iter))
    }

    pub fn get_node_by_value<'txn, Txn: 'txn>(
        &self,
        txn: &'txn Txn,
        value: &Value,
    ) -> Result<Option<Node<Value>>>
    where
        Txn: Deref<Target = RoTxn>,
        Value: Clone,
    {
        Ok(self.get_nodes_by_value(txn, value)?.next())
    }

    pub fn node_count<Txn>(&self, txn: &Txn) -> Result<usize>
    where
        Txn: Deref<Target = RoTxn>,
    {
        assert_eq!(self.node_db.len(&txn)?, self.node_idx_db.len(&txn)?);
        Ok(self.node_db.len(&txn)?)
    }

    pub fn nodes<'txn, Txn>(&self, txn: &'txn Txn) -> Result<NodeIter<'txn, Value>>
    where
        Txn: Deref<Target = RoTxn>,
    {
        Ok(NodeIter {
            iter: self.node_db.iter(&txn)?,
        })
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

    use super::*;
    use tempdir::TempDir;

    #[allow(dead_code)]
    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[fixture]
    fn tmpdir() -> TempDir {
        TempDir::new("test").unwrap()
    }
    #[fixture]
    fn graph(tmpdir: TempDir) -> Graph<String> {
        Graph::new(tmpdir.path()).unwrap()
    }

    #[rstest]
    fn test_put(graph: Graph<String>) -> Result<()> {
        let node = Node::new("test".to_string()).unwrap();
        let mut txn = graph.write_txn().unwrap();
        let returned = graph.put_node(&mut txn, node.clone()).unwrap();
        txn.commit()?;
        assert_eq!(node.id, None);
        assert_ne!(returned.id, None);
        assert_eq!(returned.get_value(), node.get_value());

        Ok(())
    }

    #[rstest]
    fn test_get(graph: Graph<String>) -> Result<()> {
        let node = Node::new("test".to_string())?;

        let mut txn = graph.write_txn()?;
        let returned = graph.put_node(&mut txn, node.clone())?;
        txn.commit()?;

        let txn = graph.read_txn()?;
        let fetched = graph.get_node_by_id::<_>(&txn, &returned.id.unwrap())?;
        assert!(fetched.is_some());
        let fetched = fetched.unwrap();
        assert_eq!(fetched.id, returned.id);
        assert_eq!(node.value, fetched.value);

        let none = graph.get_node_by_id::<_>(&txn, &LogId::nil())?;
        assert!(none.is_none());

        Ok(())
    }

    #[rstest]
    fn test_get_value(graph: Graph<String>) -> Result<()> {
        let node = Node::new("test".to_string())?;

        let mut txn = graph.write_txn()?;
        let returned = graph.put_node(&mut txn, node.clone())?;
        txn.commit()?;

        let txn = graph.read_txn()?;
        let fetched = graph.get_node_by_value(&txn, &node.value)?;
        assert!(fetched.is_some());
        let fetched = fetched.unwrap();
        assert_eq!(fetched.id, returned.id);
        assert_eq!(node.value, fetched.value);

        let fetched = graph.get_node_by_value(&txn, &"test2".to_string())?;
        assert!(fetched.is_none());
        Ok(())
    }

    #[rstest]
    fn test_node_iter(graph: Graph<String>) -> Result<()> {
        let mut returned = vec![];
        let mut txn = graph.write_txn()?;

        for i in 0..10 {
            let node = Node::new(format!("test {}", i).to_string())?;
            returned.push(graph.put_node(&mut txn, node.clone())?);
        }
        txn.commit()?;

        let txn = graph.read_txn()?;
        let nodes: Vec<_> = graph.nodes(&txn)?.collect();
        assert_eq!(nodes, returned);

        Ok(())
    }

    #[rstest]
    fn test_put_existing_node(graph: Graph<String>) -> Result<()> {
        let node = Node::new("tester".to_string())?;
        let mut txn = graph.write_txn()?;

        let mut returned = graph.put_node(&mut txn, node.clone())?;
        returned.value = "testers".to_string();
        graph.put_node(&mut txn, returned.clone())?;
        txn.commit()?;

        let txn = graph.read_txn()?;

        assert_eq!(graph.node_count(&txn)?, 1);
        let n = graph.get_node_by_id(&txn, returned.id.as_ref().unwrap())?;
        assert!(n.is_some());
        assert_eq!(n.unwrap().value, returned.value);

        Ok(())
    }
}
