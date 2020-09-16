use heed::{types::OwnedSlice, RoIter, RoRange, RoTxn};
use serde::{de::DeserializeOwned, Serialize};
use std::{marker::PhantomData, ops::Deref};

use super::{Graph, WriteTxn};
use crate::{
    error::Result,
    graph::{FromDB, LogId, Node, ToDB},
};

impl<NodeT, EdgeT> Graph<NodeT, EdgeT>
where
    NodeT: Clone + Serialize + DeserializeOwned,
    EdgeT: Clone + Serialize + DeserializeOwned,
{
    pub fn put_node(&self, txn: &mut WriteTxn, n: &Node<NodeT>) -> Result<Node<NodeT>> {
        let n = if n.id.is_some() {
            let node: Option<Node<NodeT>> = self.node_db.get(&txn, &n.id.unwrap())?;
            if let Some(node) = node {
                self.node_idx_db.delete(&mut txn.0, &node.rev_to_db()?)?;
            }
            n.clone()
        } else {
            let id = LogId::new(&mut self.generator.lock().unwrap())?;
            Node {
                id: Some(id),
                ..n.clone()
            }
        };
        self.node_db.put(&mut txn.0, n.id.as_ref().unwrap(), &n)?;
        let rev = &n.rev_to_db()?;
        self.node_idx_db
            .put(&mut txn.0, rev, n.id.as_ref().unwrap())?;

        Ok(n)
    }

    pub fn get_node_by_id<Txn>(&self, txn: &Txn, id: &LogId) -> Result<Option<Node<NodeT>>>
    where
        Txn: Deref<Target = RoTxn>,
    {
        let node = self.node_db.get(&txn, &id)?;
        Ok(node)
    }

    pub fn get_nodes_by_value<'txn, Txn: 'txn>(
        &self,
        txn: &'txn Txn,
        value: &NodeT,
    ) -> Result<NodeRange<'txn, NodeT>>
    where
        Txn: Deref<Target = RoTxn>,
        NodeT: Clone,
    {
        let prefix: Vec<u8> = Node::<NodeT>::value_to_db(value)?;
        let iter = self.node_idx_db.prefix_iter(&txn, &prefix)?;
        Ok(NodeRange::new(iter))
    }

    pub fn get_node_by_value<'txn, Txn: 'txn>(
        &self,
        txn: &'txn Txn,
        value: &NodeT,
    ) -> Result<Option<Node<NodeT>>>
    where
        Txn: Deref<Target = RoTxn>,
        NodeT: Clone,
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

    pub fn nodes<'txn, Txn>(&self, txn: &'txn Txn) -> Result<NodeIter<'txn, NodeT>>
    where
        Txn: Deref<Target = RoTxn>,
    {
        Ok(NodeIter {
            iter: self.node_db.iter(&txn)?,
        })
    }
}

pub struct NodeIter<'txn, Value> {
    pub(crate) iter: RoIter<'txn, LogId, Node<Value>>,
}

impl<'txn, Value: 'txn + DeserializeOwned> Iterator for NodeIter<'txn, Value> {
    type Item = Node<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.iter.next();
        match next {
            Some(Ok(node)) => Some(node.1),
            Some(Err(_)) => None,
            None => None,
        }
    }
}

pub struct NodeRange<'txn, Value> {
    pub(crate) iter: RoRange<'txn, OwnedSlice<u8>, LogId>,
    _marker: PhantomData<Value>,
}

impl<'txn, Value> NodeRange<'txn, Value> {
    pub fn new(iter: RoRange<'txn, OwnedSlice<u8>, LogId>) -> Self {
        Self {
            iter,
            _marker: PhantomData,
        }
    }
}

impl<'txn, Value: 'txn + DeserializeOwned> Iterator for NodeRange<'txn, Value> {
    type Item = Node<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.iter.next();
        match next {
            Some(Ok(next)) => Some(Node::rev_from_db(&next.0).unwrap()),
            Some(Err(_)) => None,
            None => None,
        }
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
    fn graph(tmpdir: TempDir) -> Graph<String, String> {
        Graph::new(tmpdir.path()).unwrap()
    }

    #[rstest]
    fn test_put(graph: Graph<String, String>) -> Result<()> {
        let node = Node::new("test".to_string()).unwrap();
        let mut txn = graph.write_txn().unwrap();
        let returned = graph.put_node(&mut txn, &node.clone()).unwrap();
        txn.commit()?;
        assert_eq!(node.id, None);
        assert_ne!(returned.id, None);
        assert_eq!(returned.get_value(), node.get_value());

        Ok(())
    }

    #[rstest]
    fn test_get(graph: Graph<String, String>) -> Result<()> {
        let node = Node::new("test".to_string())?;

        let mut txn = graph.write_txn()?;
        let returned = graph.put_node(&mut txn, &node.clone())?;
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
    fn test_get_value(graph: Graph<String, String>) -> Result<()> {
        let node = Node::new("test".to_string())?;

        let mut txn = graph.write_txn()?;
        let returned = graph.put_node(&mut txn, &node.clone())?;
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
    fn test_node_iter(graph: Graph<String, String>) -> Result<()> {
        let mut returned = vec![];
        let mut txn = graph.write_txn()?;

        for i in 0..10 {
            let node = Node::new(format!("test {}", i).to_string())?;
            returned.push(graph.put_node(&mut txn, &node.clone())?);
        }
        txn.commit()?;

        let txn = graph.read_txn()?;
        let nodes: Vec<_> = graph.nodes(&txn)?.collect();
        assert_eq!(nodes, returned);

        Ok(())
    }

    #[rstest]
    fn test_put_existing_node(graph: Graph<String, String>) -> Result<()> {
        let node = Node::new("tester".to_string())?;
        let mut txn = graph.write_txn()?;

        let mut returned = graph.put_node(&mut txn, &node.clone())?;
        returned.value = "testers".to_string();
        graph.put_node(&mut txn, &returned.clone())?;
        txn.commit()?;

        let txn = graph.read_txn()?;

        assert_eq!(graph.node_count(&txn)?, 1);
        let n = graph.get_node_by_id(&txn, returned.id.as_ref().unwrap())?;
        assert!(n.is_some());
        assert_eq!(n.unwrap().value, returned.value);

        Ok(())
    }
}
