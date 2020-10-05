pub mod edge;
pub mod node;

use heed::{types::OwnedSlice, Database, Env, EnvOpenOptions, RoTxn, RwTxn};
use serde::{de::DeserializeOwned, Serialize};
use std::{fmt::Debug, ops::Deref, path::Path};
use parking_lot::Mutex;

use crate::{
    error::Result,
    graph::{Edge, LogId, Node},
};

use ulid::Generator;

pub struct Graph<NodeT, EdgeT> {
    pub(crate) env: Env,
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
            .map_size(2<<40)
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

    pub fn write_txn(&self) -> Result<WriteTxn> {
        let txn = self.env.write_txn()?;
        Ok(WriteTxn(txn))
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
