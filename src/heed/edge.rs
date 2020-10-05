use heed::{types::OwnedSlice, RoIter, RoRange, RoTxn};
use serde::{de::DeserializeOwned, Serialize};
use std::{fmt::Debug, marker::PhantomData, ops::Deref};

use super::WriteTxn;
use crate::{
    error::Result,
    graph::{Edge, FromDB, LogId, ToDB},
    heed::Graph,
};

impl<NodeT, EdgeT> Graph<NodeT, EdgeT>
where
    NodeT: Clone + Serialize + DeserializeOwned,
    EdgeT: Clone + Serialize + DeserializeOwned + Debug,
{
    pub fn put_edge(&self, txn: &mut WriteTxn, edge: &Edge<EdgeT>) -> Result<Edge<EdgeT>> {
        let e = if edge.id.is_some() {
            let e: Option<_> = self.edge_db.get(&txn, edge.id.as_ref().unwrap())?;
            if let Some(e) = e {
                self.edge_idx_db.delete(&mut txn.0, &e.rev_to_db()?)?;
            }
            edge.clone()
        } else {
            let id = LogId::new(&mut self.generator.lock().unwrap())?;
            Edge {
                id: Some(id),
                ..edge.clone()
            }
        };
        self.edge_db.put(&mut txn.0, e.id.as_ref().unwrap(), &e)?;
        let rev = &e.rev_to_db()?;
        self.edge_idx_db
            .put(&mut txn.0, rev, e.id.as_ref().unwrap())?;
        // TODO: Add Hexstore stuff for faster searching
        Ok(e)
    }

    pub fn get_edge_by_id<Txn>(&self, txn: &Txn, id: &LogId) -> Result<Option<Edge<EdgeT>>>
    where
        Txn: Deref<Target = RoTxn>,
    {
        let edge = self.edge_db.get(&txn, &id)?;
        Ok(edge)
    }

    pub fn get_edges_by_value<'txn, Txn: 'txn>(
        &self,
        txn: &'txn Txn,
        value: &EdgeT,
    ) -> Result<EdgeRange<'txn, EdgeT>>
    where
        Txn: Deref<Target = RoTxn>,
    {
        let prefix: Vec<u8> = Edge::<EdgeT>::value_to_db(value)?;
        let iter = self.edge_idx_db.prefix_iter(&txn, &prefix)?;
        Ok(EdgeRange::new(iter))
    }

    pub fn get_edge_by_value<'txn, Txn: 'txn>(
        &self,
        txn: &'txn Txn,
        value: &EdgeT,
    ) -> Result<Option<Edge<EdgeT>>>
    where
        Txn: Deref<Target = RoTxn>,
        EdgeT: Clone,
    {
        Ok(self.get_edges_by_value(txn, value)?.next())
    }

    pub fn edge_count<Txn>(&self, txn: &Txn) -> Result<usize>
    where
        Txn: Deref<Target = RoTxn>,
    {
        assert_eq!(self.edge_db.len(&txn)?, self.edge_idx_db.len(&txn)?);
        Ok(self.edge_db.len(&txn)?)
    }

    pub fn edges<'txn, Txn>(&self, txn: &'txn Txn) -> Result<EdgeIter<'txn, EdgeT>>
    where
        Txn: Deref<Target = RoTxn>,
    {
        // Ok(EdgeRange::new(self.edge_db.range(&txn, &(..))?))

        Ok(EdgeIter {
            iter: self.edge_db.iter(&txn)?,
        })
    }
}

pub struct EdgeIter<'txn, Value> {
    pub(crate) iter: RoIter<'txn, LogId, Edge<Value>>,
}

impl<'txn, Value: 'txn + DeserializeOwned> Iterator for EdgeIter<'txn, Value> {
    type Item = Edge<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.iter.next();
        match next {
            Some(Ok(edge)) => Some(edge.1),
            Some(Err(_)) => None,
            None => None,
        }
    }
}

pub struct EdgeRange<'txn, Value> {
    pub(crate) iter: RoRange<'txn, OwnedSlice<u8>, LogId>,
    _marker: PhantomData<Value>,
}

impl<'txn, Value: 'txn + DeserializeOwned> Iterator for EdgeRange<'txn, Value> {
    type Item = Edge<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.iter.next();
        match next {
            Some(Ok(next)) => Some(Edge::rev_from_db(&next.0).unwrap()),
            Some(Err(_)) => None,
            None => None,
        }
    }
}

impl<'txn, Value> EdgeRange<'txn, Value> {
    pub fn new(iter: RoRange<'txn, OwnedSlice<u8>, LogId>) -> Self {
        Self {
            iter,
            _marker: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::{fixture, rstest};
    use tempfile::{tempdir, TempDir};

    use super::*;
    use crate::graph::Node;

    #[allow(dead_code)]
    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[fixture]
    fn tmpdir() -> TempDir {
        TempDir::new().unwrap()
    }
    #[fixture]
    fn graph(tmpdir: TempDir) -> Graph<String, String> {
        Graph::new(tmpdir.path()).unwrap()
    }
    struct Pair(Node<String>, Node<String>);

    #[fixture]
    fn nodes(graph: Graph<String, String>) -> Pair {
        let mut txn = graph.write_txn().unwrap();
        let ferb = graph
            .put_node(&mut txn, &Node::new("ferb".into()).unwrap())
            .unwrap();
        let phineas = graph
            .put_node(&mut txn, &Node::new("phineas".into()).unwrap())
            .unwrap();
        txn.commit().unwrap();
        Pair(ferb, phineas)
    }

    #[rstest]
    fn test_edge_put(graph: Graph<String, String>, nodes: Pair) -> Result<()> {
        let Pair(ferb, phineas) = nodes;
        let mut txn = graph.write_txn()?;
        let edge = graph.put_edge(
            &mut txn,
            &Edge::new(
                &ferb,
                &phineas,
                "brothers".into(),
            )?,
        )?;
        txn.commit()?;

        let txn = graph.read_txn()?;
        let fetched = graph.get_edge_by_id(&txn, &edge.id.unwrap())?;
        assert!(fetched.is_some());
        let fetched = fetched.unwrap();
        assert_eq!(fetched.value, edge.value);
        Ok(())
    }

    #[rstest]
    fn test_edge_get_by_value(graph: Graph<String, String>, nodes: Pair) -> Result<()> {
        let Pair(ferb, phineas) = nodes;
        let mut txn = graph.write_txn()?;
        let value: String = "brothers".into();
        let edge = graph.put_edge(
            &mut txn,
            &Edge::new(
                &ferb,
                &phineas,
                value.clone(),
            )?,
        )?;
        txn.commit()?;

        let txn = graph.read_txn()?;
        let fetched = graph.get_edge_by_value(&txn, &value)?;
        assert!(fetched.is_some());
        let fetched = fetched.unwrap();
        assert_eq!(fetched.value, edge.value);
        assert_eq!(fetched.to, edge.to);
        assert_eq!(fetched.from, edge.from);
        Ok(())
    }

    #[rstest]
    fn test_edges(graph: Graph<String, String>, nodes: Pair) -> Result<()> {
        let Pair(ferb, phineas) = nodes;
        let mut txn = graph.write_txn()?;
        let mut returned = vec![];

        for i in 0..10 {
            returned.push(graph.put_edge(
                &mut txn,
                &Edge::new(
                    &ferb,
                    &phineas,
                    format!("test {}", i).into(),
                )?,
            )?);
        }
        txn.commit()?;

        let txn = graph.read_txn()?;
        let edges: Vec<_> = graph.edges(&txn)?.collect();
        assert_eq!(edges, returned);
        Ok(())
    }

    #[rstest]
    fn test_put_existing_edge(graph: Graph<String, String>, nodes: Pair) -> Result<()> {
        init();
        let Pair(ferb, phineas) = nodes;
        let mut txn = graph.write_txn()?;

        let value: String = "brothers".into();
        let edge = &Edge::new(
            &ferb,
            &phineas,
            value.clone(),
        )?;

        let mut returned = graph.put_edge(&mut txn, &edge.clone())?;
        returned.value = "sisters".to_string();
        graph.put_edge(&mut txn, &returned.clone())?;
        txn.commit()?;

        let txn = graph.read_txn()?;

        assert_eq!(graph.edge_count(&txn)?, 1);
        let n = graph.get_edge_by_id(&txn, returned.id.as_ref().unwrap())?;
        assert!(n.is_some());
        assert_eq!(n.unwrap().value, returned.value);

        Ok(())
    }
}
