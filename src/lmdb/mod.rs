// pub mod graph;
pub mod trans;

use lmdb_zero::{
    db, open, Database, DatabaseOptions, EnvBuilder, Environment, ReadTransaction as RTrans,
    WriteTransaction as WTrans,
};
use std::sync::Arc;

pub use self::trans::{LmdbReadTransaction, LmdbWriteTransaction};
use crate::{error::Result, graph::Graph};

const DEFAULT_PERMISSIONS: u32 = 0o600;

#[derive(Debug)]
pub struct LmdbGraph<'a> {
    pub(crate) env: Arc<Environment>,
    pub(crate) node_db: Arc<Database<'a>>,
    pub(crate) node_idx_db: Arc<Database<'a>>,
    pub(crate) edge_db: Arc<Database<'a>>,
    pub(crate) edge_idx_db: Arc<Database<'a>>,
    pub(crate) hexstore_db: Arc<Database<'a>>,
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
        let edge_db = Arc::new(Database::open(
            env.clone(),
            Some("edges:v1"),
            &DatabaseOptions::new(db::CREATE),
        )?);
        let edge_idx_db = Arc::new(Database::open(
            env.clone(),
            Some("rev_edges:v1"),
            &DatabaseOptions::new(db::CREATE),
        )?);
        let hexstore_db = Arc::new(Database::open(
            env.clone(),
            Some("hexstore:v1"),
            &DatabaseOptions::new(db::CREATE),
        )?);
        Ok(Self {
            env,
            node_db,
            node_idx_db,
            edge_db,
            edge_idx_db,
            hexstore_db,
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
            edge_db: self.edge_db.clone(),
            edge_idx_db: self.edge_idx_db.clone(),
            hexstore_db: self.hexstore_db.clone(),

            txn: Arc::new(WTrans::new(self.env.clone())?),
        })
    }

    fn read_transaction(&self) -> Result<Self::ReadT> {
        Ok(LmdbReadTransaction {
            node_db: self.node_db.clone(),
            node_idx_db: self.node_idx_db.clone(),
            edge_db: self.edge_db.clone(),
            edge_idx_db: self.edge_idx_db.clone(),
            hexstore_db: self.hexstore_db.clone(),

            txn: Arc::new(RTrans::new(self.env.clone())?),
        })
    }
}
