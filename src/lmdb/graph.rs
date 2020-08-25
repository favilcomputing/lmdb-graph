use lmdb_zero::{
    db, error::Error as LmdbError, open, put, ConstTransaction, Database, DatabaseOptions,
    EnvBuilder, Environment, ReadTransaction as RTrans, WriteTransaction as WTrans,
};
use serde::{de::DeserializeOwned, Serialize};
use ulid::Ulid;

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
    type WriteTransaction = LmdbWriteTransaction<'graph>;
    type ReadTransaction = LmdbReadTransaction<'graph>;

    fn write_transaction(&mut self) -> Result<Self::WriteTransaction> {
        Ok(LmdbWriteTransaction {
            node_db: self.node_db.clone(),
            node_idx_db: self.node_idx_db.clone(),
            txn: WTrans::new(self.env.clone())?,
        })
    }

    fn read_transaction(&self) -> Result<Self::ReadTransaction> {
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

    fn get_node<T>(&self, id: LogId) -> Result<Option<Node<T>>>
    where
        T: Clone + Serialize + DeserializeOwned,
    {
        LmdbReadTransaction::_get_node(&self.txn, self.node_db.clone(), id)
    }

    fn get_node_by_value<T>(&self, n: &Node<T>) -> Result<Option<Node<T>>>
    where
        T: Clone + DeserializeOwned + Serialize,
    {
        LmdbReadTransaction::_get_node_by_value(&self.txn, self.node_idx_db.clone(), n)
    }
}

impl<'a> WriteTransaction for LmdbWriteTransaction<'a> {
    type Graph = LmdbGraph<'a>;

    fn put_node<T>(&mut self, n: Node<T>) -> Result<Node<T>>
    where
        T: Clone + Serialize + DeserializeOwned,
    {
        let id = Ulid::new();
        let mut access = self.txn.access();
        let buf = n.to_db()?;
        access.put(
            &self.node_db,
            id.to_string().as_str(),
            &buf,
            put::Flags::empty(),
        )?;
        access.put(
            &self.node_idx_db,
            &buf,
            id.to_string().as_str(),
            put::Flags::empty(),
        )?;
        let node = Node { id: Some(id), ..n };
        Ok(node)
    }

    fn commit(self) -> Result<()> {
        Ok(self.txn.commit()?)
    }
}

pub struct LmdbReadTransaction<'graph> {
    node_db: Arc<Database<'graph>>,
    node_idx_db: Arc<Database<'graph>>,
    txn: RTrans<'graph>,
}

impl<'graph> LmdbReadTransaction<'graph> {
    fn _get_node<T>(
        txn: &ConstTransaction,
        db: Arc<Database<'graph>>,
        id: LogId,
    ) -> Result<Option<Node<T>>>
    where
        T: Clone + Serialize + DeserializeOwned,
    {
        let access = txn.access();
        let buf: Result<&[u8]> = match access.get::<str, [u8]>(&db, &id.to_string()) {
            Ok(buf) => Ok(buf),
            Err(LmdbError::Code(lmdb_zero::error::NOTFOUND)) => return Ok(None),
            Err(e) => Err(Error::from(e)),
        };
        let node = Node::from_db(id, &buf?);
        node.map(Option::Some)
    }

    fn _get_node_by_value<T>(
        txn: &ConstTransaction,
        db: Arc<Database<'graph>>,
        n: &Node<T>,
    ) -> Result<Option<Node<T>>>
    where
        T: Clone + DeserializeOwned + Serialize,
    {
        let access = txn.access();
        let buf: Result<&str> = match access.get::<[u8], str>(&db, &n.to_db()?) {
            Ok(buf) => Ok(buf),
            Err(LmdbError::Code(lmdb_zero::error::NOTFOUND)) => return Ok(None),
            Err(e) => Err(Error::from(e)),
        };
        let id = Ulid::from_string(buf?)?;
        Ok(Some(Node {
            id: Some(id),
            type_name: n.type_name.clone(),
            value: n.value.clone(),
        }))
    }
}

impl<'graph> ReadTransaction for LmdbReadTransaction<'graph> {
    type Graph = LmdbGraph<'graph>;

    fn get_node<T>(&self, id: LogId) -> Result<Option<Node<T>>>
    where
        T: Clone + Serialize + DeserializeOwned,
    {
        Self::_get_node(&self.txn, self.node_db.clone(), id)
    }

    fn get_node_by_value<T>(&self, n: &Node<T>) -> Result<Option<Node<T>>>
    where
        T: Clone + DeserializeOwned + Serialize,
    {
        Self::_get_node_by_value(&self.txn, self.node_idx_db.clone(), n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;

    #[test]
    fn test_graph() {
        let tmpdir = TempDir::new("test").unwrap();
        let mut graph = unsafe { LmdbGraph::new(tmpdir.path().to_str().unwrap()) }.unwrap();

        let node = Node::new("Name", "test".to_string()).unwrap();
        let mut txn = graph.write_transaction().unwrap();
        let returned = txn.put_node(node.clone()).unwrap();
        assert_eq!(node.id, None);
        assert_ne!(returned.id, None);
        assert_eq!(returned.get_value(), node.get_value());

        tmpdir.close().unwrap();
    }

    #[test]
    fn test_get_node() -> Result<()> {
        let tmpdir = TempDir::new("test")?;
        let mut graph = unsafe { LmdbGraph::new(tmpdir.path().to_str().unwrap()) }?;

        let node = Node::new("Name", "test".to_string())?;
        let mut txn = graph.write_transaction()?;
        let returned = txn.put_node(node.clone())?;
        txn.commit()?;
        assert_ne!(returned.id, None);
        assert_eq!(returned.type_name, node.type_name);
        assert_eq!(returned.get_value(), node.get_value());
        let txn = graph.read_transaction()?;

        let fetched = txn.get_node::<String>(returned.id.unwrap())?.unwrap();
        assert_eq!(fetched.id, returned.id);
        assert_eq!(fetched.type_name, returned.type_name);
        assert_eq!(fetched.get_value(), returned.get_value());

        tmpdir.close()?;
        Ok(())
    }

    #[test]
    fn node_not_exist() -> Result<()> {
        let tmpdir = TempDir::new("test")?;
        let graph = unsafe { LmdbGraph::new(tmpdir.path().to_str().unwrap()) }?;

        let txn = graph.read_transaction()?;
        let id = Ulid::new();
        let ret = txn.get_node::<String>(id);
        match ret {
            Ok(n) => assert!(n.is_none()),
            Err(e) => panic!("Wrong error {:?}", e),
        }
        tmpdir.close()?;
        Ok(())
    }

    #[test]
    fn node_reverse_lookup() -> Result<()> {
        let tmpdir = TempDir::new("test")?;
        let mut graph = unsafe { LmdbGraph::new(tmpdir.path().to_str().unwrap()) }?;

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
        let charles_ret = txn.get_node_by_value(&Node::new("name", "Charles".to_string())?)?;
        assert!(charles_ret.is_some());
        assert_eq!(charles.id, charles_ret.unwrap().id);

        tmpdir.close()?;
        Ok(())
    }
}
