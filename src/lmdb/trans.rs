use lmdb_zero::{
    error::Error as LmdbError, put, ConstTransaction, Cursor, Database, ReadTransaction as RTrans,
    WriteTransaction as WTrans,
};
use serde::{de::DeserializeOwned, Serialize};
use std::{marker::PhantomData, ops::Deref, sync::Arc};

use super::LmdbGraph;
use crate::{
    error::{Error, Result},
    graph::{edge::ORDERS, Edge, FromDB, LogId, Node, ReadTransaction, ToDB, WriteTransaction},
};

pub struct LmdbWriteTransaction<'graph> {
    pub(crate) node_db: Arc<Database<'graph>>,
    pub(crate) node_idx_db: Arc<Database<'graph>>,
    pub(crate) edge_db: Arc<Database<'graph>>,
    pub(crate) edge_idx_db: Arc<Database<'graph>>,
    pub(crate) hexstore_db: Arc<Database<'graph>>,

    pub(crate) txn: Arc<WTrans<'graph>>,
}

impl<'graph> ReadTransaction for LmdbWriteTransaction<'graph> {
    type Graph = LmdbGraph<'graph>;

    fn get_node<Type, Value>(&self, id: LogId) -> Result<Option<Node<Type, Value>>>
    where
        Type: Clone + Serialize + DeserializeOwned,
        Value: Clone + Serialize + DeserializeOwned,
    {
        LmdbReadTransaction::_get_node(self.txn.clone(), self.node_db.clone(), id)
    }

    fn get_node_by_value<Type, Value>(
        &self,
        n: &Node<Type, Value>,
    ) -> Result<Option<Node<Type, Value>>>
    where
        Type: Clone + DeserializeOwned + Serialize,
        Value: Clone + DeserializeOwned + Serialize,
    {
        LmdbReadTransaction::_get_node_by_value(self.txn.clone(), self.node_idx_db.clone(), n)
    }

    // fn get_all_nodes<Type, Value>(&self) -> Result<self::NodeIter<Type, Value>>
    // where
    //     Type: Clone + DeserializeOwned + Serialize,
    //     Value: Clone + DeserializeOwned + Serialize,
    // {
    //     LmdbReadTransaction::_get_all_nodes(&self.txn, self.node_db.clone())
    // }
}

impl<'a> WriteTransaction for LmdbWriteTransaction<'a> {
    type Graph = LmdbGraph<'a>;

    fn put_node<Type, Value>(&mut self, n: Node<Type, Value>) -> Result<Node<Type, Value>>
    where
        Type: Clone + Serialize + DeserializeOwned,
        Value: Clone + Serialize + DeserializeOwned,
    {
        let id = LogId::new();
        let mut access = self.txn.access();
        access.put(
            &self.node_db,
            &Node::<Type, Value>::key_to_db(&id)?,
            &n.to_db()?,
            put::NODUPDATA,
        )?;
        access.put(
            &self.node_idx_db,
            &n.value_to_db()?,
            &Node::<Type, Value>::key_to_db(&id)?,
            put::Flags::empty(),
        )?;
        let node = Node { id: Some(id), ..n };
        Ok(node)
    }

    fn put_edge<Type, Value>(&mut self, e: Edge<Type, Value>) -> Result<Edge<Type, Value>>
    where
        Type: Clone + Serialize + DeserializeOwned,
        Value: Clone + Serialize + DeserializeOwned,
    {
        let id = LogId::new();
        let mut access = self.txn.access();
        access.put(
            &self.edge_db,
            &Edge::<Type, Value>::key_to_db(&id)?,
            &e.to_db()?,
            put::NODUPDATA,
        )?;
        access.put(
            &self.edge_idx_db,
            &e.value_to_db()?,
            &Edge::<Type, Value>::key_to_db(&id)?,
            put::Flags::empty(),
        )?;
        for order in ORDERS.iter() {
            access.put(
                &self.hexstore_db,
                &order.to_db(id, e.to, e.from)?,
                &e.to_db()?,
                put::Flags::empty(),
            )?;
        }
        let edge = Edge { id: Some(id), ..e };
        Ok(edge)
    }

    fn commit(self) -> Result<()> {
        let txn = Arc::try_unwrap(self.txn).unwrap();
        Ok(txn.commit()?)
    }

    fn clear(&mut self) -> Result<()> {
        let mut access = self.txn.access();
        access.clear_db(&self.node_db)?;
        access.clear_db(&self.node_idx_db)?;
        Ok(())
    }
}

pub struct LmdbReadTransaction<'graph> {
    pub(crate) node_db: Arc<Database<'graph>>,
    pub(crate) node_idx_db: Arc<Database<'graph>>,
    pub(crate) edge_db: Arc<Database<'graph>>,
    pub(crate) edge_idx_db: Arc<Database<'graph>>,
    pub(crate) hexstore_db: Arc<Database<'graph>>,

    pub(crate) txn: Arc<RTrans<'graph>>,
}

impl<'graph> LmdbReadTransaction<'graph> {
    fn _get_node<Type, Value>(
        txn: Arc<impl Deref<Target = ConstTransaction<'graph>>>,
        db: Arc<Database<'graph>>,
        id: LogId,
    ) -> Result<Option<Node<Type, Value>>>
    where
        Type: Clone + Serialize + DeserializeOwned,
        Value: Clone + Serialize + DeserializeOwned,
    {
        let access = txn.access();
        let buf: Result<&[u8]> =
            match access.get::<[u8], [u8]>(&db, &Node::<Type, Value>::key_to_db(&id)?) {
                Ok(buf) => Ok(buf),
                Err(LmdbError::Code(lmdb_zero::error::NOTFOUND)) => return Ok(None),
                Err(e) => Err(Error::from(e)),
            };
        let node = Node::from_db(&id, &buf?);
        node.map(Option::Some)
    }

    fn _get_node_by_value<Type, Value>(
        txn: Arc<impl Deref<Target = ConstTransaction<'graph>>>,
        db: Arc<Database<'graph>>,
        n: &Node<Type, Value>,
    ) -> Result<Option<Node<Type, Value>>>
    where
        Type: Clone + DeserializeOwned + Serialize,
        Value: Clone + DeserializeOwned + Serialize,
    {
        let access = txn.access();
        let buf: Result<&[u8]> = match access.get::<[u8], [u8]>(&db, &n.value_to_db()?) {
            Ok(buf) => Ok(buf),
            Err(LmdbError::Code(lmdb_zero::error::NOTFOUND)) => return Ok(None),
            Err(e) => Err(Error::from(e)),
        };
        let id = Node::<Type, Value>::key_from_db(buf?)?;
        Ok(Some(Node {
            id: Some(id),
            _type: n._type.clone(),
            value: n.value.clone(),
        }))
    }

    fn _get_edge<Type, Value>(
        txn: Arc<ConstTransaction>,
        db: Arc<Database<'graph>>,
        id: LogId,
    ) -> Result<Option<Edge<Type, Value>>>
    where
        Type: Clone + Serialize + DeserializeOwned,
        Value: Clone + Serialize + DeserializeOwned,
    {
        let access = txn.access();
        let buf: Result<&[u8]> =
            match access.get::<[u8], [u8]>(&db, &Edge::<Type, Value>::key_to_db(&id)?) {
                Ok(buf) => Ok(buf),
                Err(LmdbError::Code(lmdb_zero::error::NOTFOUND)) => return Ok(None),
                Err(e) => Err(Error::from(e)),
            };
        let node = Edge::from_db(&id, &buf?);
        node.map(Option::Some)
    }

    // fn _get_all_nodes<Type, Value>(
    //     txn: Arc<ConstTransaction>,
    //     db: Arc<Database<'graph>>,
    // ) -> Result<NodeIter<Type, Value>> {
    //     Ok(NodeIter::new(Arc::new(txn.cursor(db.clone())?)))
    // }
}

impl<'graph> ReadTransaction for LmdbReadTransaction<'graph> {
    type Graph = LmdbGraph<'graph>;

    fn get_node<Type, Value>(&self, id: LogId) -> Result<Option<Node<Type, Value>>>
    where
        Type: Clone + Serialize + DeserializeOwned,
        Value: Clone + Serialize + DeserializeOwned,
    {
        Self::_get_node(self.txn.clone(), self.node_db.clone(), id)
    }

    fn get_node_by_value<Type, Value>(
        &self,
        n: &Node<Type, Value>,
    ) -> Result<Option<Node<Type, Value>>>
    where
        Type: Clone + DeserializeOwned + Serialize,
        Value: Clone + DeserializeOwned + Serialize,
    {
        Self::_get_node_by_value(self.txn.clone(), self.node_idx_db.clone(), n)
    }
    // fn get_all_nodes<Type, Value>(&self) -> Result<NodeIter<Type, Value>> {
    //     Self::_get_all_nodes(&self.txn, self.node_db.clone())
    // }
}

#[derive(Debug)]
pub struct NodeIter<Type, Value> {
    cursor: Arc<Cursor<'static, 'static>>,
    _marker: PhantomData<(Type, Value)>,
}

impl<Type, Value> NodeIter<Type, Value> {
    pub fn new(cursor: Arc<Cursor<'static, 'static>>) -> Self {
        Self {
            cursor,
            _marker: PhantomData,
        }
    }
}

impl<Type, Value> Iterator for NodeIter<Type, Value> {
    type Item = Node<Type, Value>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use rstest::{fixture, rstest};

    use super::*;
    use crate::graph::Graph;
    use itertools::assert_equal;
    use tempdir::TempDir;

    #[fixture]
    fn tmpdir() -> TempDir {
        TempDir::new("test").unwrap()
    }
    #[fixture]
    fn graph(tmpdir: TempDir) -> Result<impl Graph> {
        unsafe { LmdbGraph::new(tmpdir.path().to_str().unwrap()) }
    }

    #[rstest]
    fn test_graph(graph: Result<impl Graph>) -> Result<()> {
        let mut graph = graph?;
        let node = Node::new("Name".to_string(), "test".to_string()).unwrap();
        let mut txn = graph.write_transaction().unwrap();
        let returned = txn.put_node(node.clone()).unwrap();
        assert_eq!(node.id, None);
        assert_ne!(returned.id, None);
        assert_eq!(returned.get_value(), node.get_value());

        Ok(())
    }

    #[rstest]
    fn test_get_node(graph: Result<impl Graph>) -> Result<()> {
        let mut graph = graph?;

        let node = Node::new("Name".to_string(), "test".to_string())?;
        let mut txn = graph.write_transaction()?;
        let returned = txn.put_node(node.clone())?;
        txn.commit()?;
        assert_ne!(returned.id, None);
        assert_eq!(returned._type, node._type);
        assert_eq!(returned.get_value(), node.get_value());
        let txn = graph.read_transaction()?;

        let fetched = txn.get_node::<String, String>(returned.id.unwrap())?;
        assert!(fetched.is_some());
        let fetched = fetched.unwrap();
        assert_eq!(fetched.id, returned.id);
        assert_eq!(fetched._type, returned._type);
        assert_eq!(fetched.get_value(), returned.get_value());

        Ok(())
    }

    #[rstest]
    fn node_not_exist(graph: Result<impl Graph>) -> Result<()> {
        let graph = graph?;

        let txn = graph.read_transaction()?;
        let id = LogId::new();
        let ret = txn.get_node::<String, String>(id);
        match ret {
            Ok(n) => assert!(n.is_none()),
            Err(e) => panic!("Wrong error {:?}", e),
        }
        Ok(())
    }

    #[rstest]
    fn node_reverse_lookup(graph: Result<impl Graph>) -> Result<()> {
        let mut graph = graph?;

        let name = "Kevin".to_string();
        let node = Node::new("name".to_string(), name)?;

        let mut txn = graph.write_transaction()?;
        let put = txn.put_node(node.clone())?;
        // Put some more to make sure writes don't affect things
        let charles = txn.put_node(Node::new("name".to_string(), "Charles".to_string())?)?;
        txn.put_node(Node::new("name".to_string(), "James".to_string())?)?;
        txn.put_node(Node::new("name".to_string(), "Isabella".to_string())?)?;
        txn.commit()?;
        assert!(put.id.is_some());

        let txn = graph.read_transaction()?;
        let fetched = txn.get_node_by_value(&node)?;
        assert!(fetched.is_some());
        let fetched = fetched.unwrap();
        assert_eq!(fetched.id, put.id);
        let charles_ret =
            txn.get_node_by_value(&Node::new("name".to_string(), "Charles".to_string())?)?;
        assert!(charles_ret.is_some());
        assert_eq!(charles.id, charles_ret.unwrap().id);

        Ok(())
    }

    // #[rstest]
    // fn all_nodes(graph: Result<impl Graph>) -> Result<()> {
    //     let mut graph = graph?;

    //     let _type = "name".to_string();
    //     let name = "Kevin".to_string();
    //     let node = Node::new(_type.clone(), name.clone())?;

    //     let mut txn = graph.write_transaction()?;
    //     let put = txn.put_node(node.clone())?;
    //     // Put some more to make sure writes don't affect things
    //     let charles = txn.put_node(Node::new("name".to_string(), "Charles".to_string())?)?;
    //     txn.commit()?;

    //     let txn = graph.read_transaction()?;
    //     assert_eq!(
    //         txn.get_all_nodes::<String, String>()?
    //             .collect::<Vec<Node<String, String>>>(),
    //         vec![put, charles]
    //     );

    //     Ok(())
    // }
}
