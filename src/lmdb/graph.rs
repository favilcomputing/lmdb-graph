use lmdb_zero::{
    db, error::Error as LmdbError, open, put, ConstTransaction, Database, DatabaseOptions,
    EnvBuilder, Environment, ReadTransaction as RTrans, WriteTransaction as WTrans,
};
use serde::{de::DeserializeOwned, Serialize};

use std::sync::Arc;

use crate::{
    error::{Error, Result},
    graph::{FromDB, Graph, LogId, Node, ReadTransaction, ToDB, WriteTransaction},
};

const DEFAULT_PERMISSIONS: u32 = 0o600;

#[derive(Debug)]
pub struct LmdbGraph<'a> {
    env: Arc<Environment>,
    node_db: Arc<Database<'a>>,
    node_idx_db: Arc<Database<'a>>,
}

impl<'a> LmdbGraph<'a> {
    pub unsafe fn new(path: &str) -> Result<Self> {
        let mut builder = EnvBuilder::new()?;
        builder.set_maxdbs(10)?;
        let env = Arc::new(builder.open(path, open::Flags::empty(), DEFAULT_PERMISSIONS)?);
        let node_db = Arc::new(Database::open(
            env.clone(),
            Some("nodes:v1"),
            &DatabaseOptions::new(db::CREATE),
        )?);
        let node_idx_db = Arc::new(Database::open(
            env.clone(),
            Some("rev_nodes:v1"),
            &DatabaseOptions::new(db::CREATE),
        )?);
        Ok(Self {
            env,
            node_db,
            node_idx_db,
        })
    }
}

impl<'graph> Graph for LmdbGraph<'graph> {
    type WriteT = LmdbWriteTransaction<'graph>;
    type ReadT = LmdbReadTransaction<'graph>;

    fn write_transaction(&mut self) -> Result<Self::WriteT> {
        Ok(LmdbWriteTransaction {
            node_db: self.node_db.clone(),
            node_idx_db: self.node_idx_db.clone(),
            txn: WTrans::new(self.env.clone())?,
        })
    }

    fn read_transaction(&self) -> Result<Self::ReadT> {
        Ok(LmdbReadTransaction {
            node_db: self.node_db.clone(),
            node_idx_db: self.node_idx_db.clone(),
            txn: RTrans::new(self.env.clone())?,
        })
    }
}

pub struct LmdbWriteTransaction<'graph> {
    node_db: Arc<Database<'graph>>,
    node_idx_db: Arc<Database<'graph>>,
    txn: WTrans<'graph>,
}

impl<'graph> ReadTransaction for LmdbWriteTransaction<'graph> {
    type Graph = LmdbGraph<'graph>;

    fn get_node<Type, Value>(&self, id: LogId) -> Result<Option<Node<Type, Value>>>
    where
        Type: Clone + Serialize + DeserializeOwned,
        Value: Clone + Serialize + DeserializeOwned,
    {
        LmdbReadTransaction::_get_node(&self.txn, self.node_db.clone(), id)
    }

    fn get_node_by_value<Type, Value>(
        &self,
        n: &Node<Type, Value>,
    ) -> Result<Option<Node<Type, Value>>>
    where
        Type: Clone + DeserializeOwned + Serialize,
        Value: Clone + DeserializeOwned + Serialize,
    {
        LmdbReadTransaction::_get_node_by_value(&self.txn, self.node_idx_db.clone(), n)
    }
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
        let buf = n.to_db()?;
        access.put(
            &self.node_db,
            &Node::<Type, Value>::key_to_db(&id)?,
            &buf,
            put::NODUPDATA,
        )?;
        access.put(
            &self.node_idx_db,
            &buf,
            &Node::<Type, Value>::key_to_db(&id)?,
            put::Flags::empty(),
        )?;
        let node = Node { id: Some(id), ..n };
        Ok(node)
    }

    fn commit(self) -> Result<()> {
        Ok(self.txn.commit()?)
    }

    fn clear(&mut self) -> Result<()> {
        let mut access = self.txn.access();
        access.clear_db(&self.node_db)?;
        access.clear_db(&self.node_idx_db)?;
        Ok(())
    }
}

pub struct LmdbReadTransaction<'graph> {
    node_db: Arc<Database<'graph>>,
    node_idx_db: Arc<Database<'graph>>,
    txn: RTrans<'graph>,
}

impl<'graph> LmdbReadTransaction<'graph> {
    fn _get_node<Type, Value>(
        txn: &ConstTransaction,
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
        txn: &ConstTransaction,
        db: Arc<Database<'graph>>,
        n: &Node<Type, Value>,
    ) -> Result<Option<Node<Type, Value>>>
    where
        Type: Clone + DeserializeOwned + Serialize,
        Value: Clone + DeserializeOwned + Serialize,
    {
        let access = txn.access();
        let buf: Result<&[u8]> = match access.get::<[u8], [u8]>(&db, &n.to_db()?) {
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
}

impl<'graph> ReadTransaction for LmdbReadTransaction<'graph> {
    type Graph = LmdbGraph<'graph>;

    fn get_node<Type, Value>(&self, id: LogId) -> Result<Option<Node<Type, Value>>>
    where
        Type: Clone + Serialize + DeserializeOwned,
        Value: Clone + Serialize + DeserializeOwned,
    {
        Self::_get_node(&self.txn, self.node_db.clone(), id)
    }

    fn get_node_by_value<Type, Value>(
        &self,
        n: &Node<Type, Value>,
    ) -> Result<Option<Node<Type, Value>>>
    where
        Type: Clone + DeserializeOwned + Serialize,
        Value: Clone + DeserializeOwned + Serialize,
    {
        Self::_get_node_by_value(&self.txn, self.node_idx_db.clone(), n)
    }
}

#[cfg(test)]
mod tests {
    use rstest::{fixture, rstest};

    use super::*;
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
        let node = Node::new("Name", "test".to_string()).unwrap();
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

        let node = Node::new("Name", "test".to_string())?;
        let mut txn = graph.write_transaction()?;
        let returned = txn.put_node(node.clone())?;
        txn.commit()?;
        assert_ne!(returned.id, None);
        assert_eq!(returned._type, node._type);
        assert_eq!(returned.get_value(), node.get_value());
        let txn = graph.read_transaction()?;

        let fetched = txn.get_node::<String>(returned.id.unwrap())?;
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
        let ret = txn.get_node::<String>(id);
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
        let node = Node::new("name", name)?;

        let mut txn = graph.write_transaction()?;
        let put = txn.put_node(node.clone())?;
        // Put some more to make sure writes don't affect things
        let charles = txn.put_node(Node::new("name", "Charles".to_string())?)?;
        txn.put_node(Node::new("name", "James".to_string())?)?;
        txn.put_node(Node::new("name", "Isabella".to_string())?)?;
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
}
