pub mod edge;
pub mod node;

use heed::{types::OwnedSlice, Database, Env, EnvOpenOptions, RoTxn, RwTxn};
use parking_lot::{Condvar, Mutex};
use serde::{de::DeserializeOwned, Serialize};
use std::{fmt::Debug, ops::Deref, path::Path, ptr::null_mut, time::Duration};

use crate::{
    error::{Error, Result},
    graph::{Edge, LogId, Node},
};

use ulid::Generator;

pub struct Graph<NodeT, EdgeT> {
    env: Env,
    write_busy: Mutex<()>,
    write_cond: Condvar,
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
        let write_busy = Mutex::new(());
        let generator = Mutex::new(Generator::new());
        let node_db = env.create_database(Some("nodes:v1"))?;
        let node_idx_db = env.create_database(Some("nodes_idx:v1"))?;
        let edge_db = env.create_database(Some("edges:v1"))?;
        let edge_idx_db = env.create_database(Some("edges_idx:v1"))?;
        let write_cond = Condvar::new();
        Ok(Self {
            env,
            write_busy,
            write_cond,
            generator,

            node_db,
            node_idx_db,
            edge_db,
            edge_idx_db,
        })
    }

    pub fn write_txn(&self) -> Result<WriteTxn> {
        self.write_txn_wait(Duration::from_secs(30))
    }

    pub fn write_txn_wait(&self, d: Duration) -> Result<WriteTxn> {
        let mut busy = self.write_busy.lock();
        let mut txn = self.env.write_txn();
        while let Err(heed::Error::Mdb(heed::MdbError::Busy)) = txn {
            let result = self.write_cond.wait_for(&mut busy, d);
            if result.timed_out() {
                log::error!("Timed out");
                return Err(Error::TimedOut(d));
            }
            txn = self.env.write_txn();
        }
        // Making this safe by the lock.
        // Heed doesn't need the mutex, this is just to ensure that
        // we can support a delay
        // TODO: Add Condvar and notify inside drop.
        let txn = unsafe { std::mem::transmute(txn?) };
        Ok(WriteTxn(txn, &self.write_cond))
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

pub struct WriteTxn<'graph>(RwTxn<'graph>, &'graph Condvar);

impl Debug for WriteTxn<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Txn")
    }
}

impl Drop for WriteTxn<'_> {
    fn drop(&mut self) {
        self.1.notify_one();
    }
}

impl<'graph> WriteTxn<'graph> {
    pub fn commit(mut self) -> Result<()> {
        let null: *mut u8 = null_mut();
        // This should still be safe, because we drop before using this value.
        let bad = unsafe { std::mem::transmute(null) };
        let txn = std::mem::replace(&mut self.0, bad);
        drop(self);
        let result = txn.commit();
        Ok(result?)
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
    use tempfile::{tempdir, TempDir};

    use super::*;
    use crate::{error::Error, graph::Node};

    #[fixture]
    fn tmpdir() -> TempDir {
        TempDir::new().unwrap()
    }

    #[fixture]
    fn graph(tmpdir: TempDir) -> Graph<String, String> {
        Graph::new(tmpdir.path()).unwrap()
    }

    #[rstest]
    fn test_mult_trans(graph: Graph<String, String>) -> Result<()> {
        let w1 = graph.write_txn()?;
        let w2 = graph.write_txn_wait(Duration::from_secs(0));
        if let Err(Error::TimedOut(d)) = w2 {
            assert_eq!(d, Duration::from_secs(0));
        } else {
            panic!("Not correct error {:?}", w2);
        }
        Ok(())
    }

    #[rstest]
    fn test_mult_trans_threads(graph: Graph<String, String>) -> Result<()> {
        let t1 = std::thread::spawn(|| {
            graph.write_txn()?;
            std::thread::sleep(Duration::from_secs(1));
        });
        let w2 = graph.write_txn_wait(Duration::from_secs(0));
        if let Err(Error::TimedOut(d)) = w2 {
            assert_eq!(d, Duration::from_secs(0));
        } else {
            panic!("Not correct error {:?}", w2);
        }
        Ok(())
    }
}
