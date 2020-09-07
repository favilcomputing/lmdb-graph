use heed::{types::Str, Database, Env, EnvOpenOptions};

use lmdb_graph::error::Result;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use std::{
    fs,
    path::Path,
    sync::{Arc, RwLock},
};

fn main() -> Result<()> {
    log::info!("Setting up environment");
    fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;

    let env: Env = EnvOpenOptions::new().open(Path::new("target").join("zerocopy.mdb"))?;

    log::info!("Creating database");
    let db: Database<Str, Str> = env.create_database(None)?;
    {
        let mut txn = env.write_txn()?;
        db.clear(&mut txn)?;
        db.put(
            &mut txn,
            "Phineas",
            &thread_rng()
                .sample_iter(&Alphanumeric)
                .take(30)
                .collect::<String>(),
        )?;
        db.put(
            &mut txn,
            "Ferb",
            &thread_rng()
                .sample_iter(&Alphanumeric)
                .take(30)
                .collect::<String>(),
        )?;
        db.put(
            &mut txn,
            "Candace",
            &thread_rng()
                .sample_iter(&Alphanumeric)
                .take(30)
                .collect::<String>(),
        )?;
        db.put(&mut txn, "Isabella", "🍔∈🌏")?;
        txn.commit()?;
    }
    {
        let txn = env.read_txn()?;
        {
            for ret in db.iter(&txn)? {
                println!("{:?}", ret?);
            }
            // let mut access = txn.access();
            // let cursor = Arc::new(RwLock::new(txn.cursor(db.clone())?));
            // let mut owned_cursor = Csr::new(cursor.clone(), &access);
            // let mut next = owned_cursor.next::<str, str>();
            // while next.is_ok() {
            //     println!("{:?}", next?);
            //     next = owned_cursor.next();
            // }
        }
    }

    Ok(())
}

struct ReadTxn {}

impl ReadTxn {
    // fn new<'env>(env: Arc<Environment>, db: Arc<Database<'db>>) -> Self
    // where
    //     'env: 'db,
    // {
    //     let txn = Arc::new(ReadTransaction::new(env.clone()).unwrap());
    //     Self {
    //         db,
    //         txn: txn.clone(),
    //     }
    // }

    // fn cursor<'access>(&self) -> Csr<'access, 'txn, 'db>
    // where
    //     'access: 'txn,
    //     'txn: 'db,
    // {
    //     let cursor: Arc<RwLock<Cursor>> =
    //         Arc::new(RwLock::new(self.txn.cursor(self.db.clone()).unwrap()));
    //     let access = self.txn.access();
    //     Csr::new(cursor, &access)
    // }
}

struct Csr {
    // cursor: Arc<RwLock<Cursor<'txn, 'db>>>,
// access: &'access ConstAccessor<'access>,
}

impl Csr {
    // fn new(
    //     cursor: Arc<RwLock<Cursor<'txn, 'db>>>,
    //     access: &'access ConstAccessor<'access>,
    // ) -> Self {
    //     Self { cursor, access }
    // }

    // fn next<K, V>(&mut self) -> Result<(&'access K, &'access V)>
    // where
    //     K: FromLmdbBytes + ?Sized,
    //     V: FromLmdbBytes + ?Sized,
    // {
    //     Ok(self.cursor.write().unwrap().next(&self.access)?)
    // }
}
