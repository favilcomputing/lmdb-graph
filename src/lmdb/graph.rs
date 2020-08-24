use lmdb_zero::{
    db, open, put, Database, DatabaseOptions, EnvBuilder, Environment, ReadTransaction as RTrans,
    WriteTransaction as WTrans,
};
use rmp_serde::{from_read_ref, Serializer};
use serde::{de::DeserializeOwned, Serialize};
use ulid::Ulid;

use std::sync::Arc;

use crate::{
    error::Result,
    graph::{Graph, LogId, Node, ReadTransaction, WriteTransaction},
};

const DEFAULT_PERMISSIONS: u32 = 0o600;

#[derive(Debug)]
pub struct LmdbGraph<'a> {
    env: Arc<Environment>,
    node_db: Arc<Database<'a>>,
}

impl<'a> LmdbGraph<'a> {
    pub unsafe fn new(path: &str) -> Result<Self> {
        let mut builder = EnvBuilder::new()?;
        builder.set_maxdbs(10)?;
        let env = Arc::new(builder.open(path, open::Flags::empty(), DEFAULT_PERMISSIONS)?);
        let node_db = Arc::new(Database::open(
            env.clone(),
            Some("nodes"),
            &DatabaseOptions::new(db::CREATE),
        )?);
        Ok(Self { env, node_db })
    }
}

impl<'graph> Graph for LmdbGraph<'graph> {
    type WriteTransaction = LmdbWriteTransaction<'graph>;
    type ReadTransaction = LmdbReadTransaction<'graph>;

    fn write_transaction(&mut self) -> Result<Self::WriteTransaction> {
        Ok(LmdbWriteTransaction {
            node_db: self.node_db.clone(),
            txn: WTrans::new(self.env.clone())?,
        })
    }

    fn read_transaction(&self) -> Result<Self::ReadTransaction> {
        Ok(LmdbReadTransaction {
            node_db: self.node_db.clone(),
            txn: RTrans::new(self.env.clone())?,
        })
    }
}

pub struct LmdbWriteTransaction<'graph> {
    node_db: Arc<Database<'graph>>,
    txn: WTrans<'graph>,
}

impl<'graph> ReadTransaction for LmdbWriteTransaction<'graph> {
    type Graph = LmdbGraph<'graph>;

    fn get_node<T>(&self, id: LogId) -> Result<Node<T>>
    where
        T: Clone + Serialize + DeserializeOwned,
    {
        let access = self.txn.access();
        let buf = access.get::<str, [u8]>(&self.node_db, &id.to_string())?;
        let (type_name, value) = from_read_ref(&buf)?;
        Ok(Node {
            id: Some(id),
            next_id: None,
            type_name,
            value,
        })
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
        let mut buf = Vec::new();
        (&n.type_name, &n.value).serialize(&mut Serializer::new(&mut buf))?;
        access.put(
            &self.node_db,
            id.to_string().as_str(),
            &buf,
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
    txn: RTrans<'graph>,
}

impl<'graph> ReadTransaction for LmdbReadTransaction<'graph> {
    type Graph = LmdbGraph<'graph>;

    fn get_node<T>(&self, id: LogId) -> Result<Node<T>>
    where
        T: Clone + Serialize + DeserializeOwned,
    {
        let access = self.txn.access();
        let buf = access.get::<str, [u8]>(&self.node_db, &id.to_string())?;
        let (type_name, value) = from_read_ref(&buf)?;
        Ok(Node {
            id: Some(id),
            next_id: None,
            type_name,
            value,
        })
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

        let fetched = txn.get_node::<String>(returned.id.unwrap())?;
        assert_eq!(fetched.id, returned.id);
        assert_eq!(fetched.type_name, returned.type_name);
        assert_eq!(fetched.get_value(), returned.get_value());

        tmpdir.close()?;
        Ok(())
    }
}
