use lmdb::{
    db,
    open::Flags,
    traits::{CreateCursor, FromLmdbBytes},
    ConstAccessor, Cursor, Database, DatabaseOptions, Environment, ReadTransaction, WriteAccessor,
};
use lmdb_graph::error::Result;
use lmdb_zero::{self as lmdb, EnvBuilder};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use std::sync::{Arc, RwLock};

fn main() -> Result<()> {
    log::info!("Setting up environment");
    let mut builder = EnvBuilder::new()?;
    builder.set_maxdbs(10)?;
    let env = Arc::new(unsafe { builder.open("./test.db", Flags::empty(), 0o600) }?);

    log::info!("Creating database");
    let db: Arc<Database> = Arc::new(Database::open(
        env.clone(),
        None,
        &DatabaseOptions::new(db::CREATE),
    )?);
    {
        let txn = lmdb::WriteTransaction::new(env.clone())?;
        {
            let mut access: WriteAccessor = txn.access();
            access.clear_db(&db)?;
            access.put(
                &db,
                "Phineas",
                thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(30)
                    .collect::<String>()
                    .as_bytes(),
                lmdb::put::Flags::empty(),
            )?;
            access.put(
                &db,
                "Ferb",
                thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(30)
                    .collect::<String>()
                    .as_bytes(),
                lmdb::put::Flags::empty(),
            )?;
            access.put(
                &db,
                "Candace",
                thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(30)
                    .collect::<String>()
                    .as_bytes(),
                lmdb::put::Flags::empty(),
            )?;
            access.put(&db, "Isabella", "🍔∈🌏", lmdb::put::Flags::empty())?;
        }
        txn.commit()?;
    }
    {
        let txn: Arc<ReadTransaction> = Arc::new(ReadTransaction::new(env.clone())?);
        {
            #[allow(unused_mut)]
            let mut access = txn.access();
            let cursor = Arc::new(RwLock::new(txn.cursor(db.clone())?));
            let mut owned_cursor = Csr::new(cursor.clone(), &access);
            let mut next = owned_cursor.next::<str, str>();
            while next.is_ok() {
                println!("{:?}", next?);
                next = owned_cursor.next();
            }
        }
    }

    Ok(())
}

struct ReadTxn<'txn, 'db>
where
    'txn: 'db,
{
    db: Arc<Database<'db>>,

    txn: Arc<ReadTransaction<'txn>>,
}

impl<'txn, 'db> ReadTxn<'txn, 'db>
where
    'txn: 'db,
{
    fn new<'env>(env: Arc<Environment>, db: Arc<Database<'db>>) -> Self
    where
        'env: 'db,
    {
        let txn = Arc::new(ReadTransaction::new(env.clone()).unwrap());
        Self {
            db,
            txn: txn.clone(),
        }
    }

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

struct Csr<'access, 'txn, 'db>
where
    'access: 'txn,
    'txn: 'db,
{
    cursor: Arc<RwLock<Cursor<'txn, 'db>>>,
    access: &'access ConstAccessor<'access>,
}

impl<'access, 'txn: 'access, 'db: 'txn> Csr<'access, 'txn, 'db> {
    fn new(
        cursor: Arc<RwLock<Cursor<'txn, 'db>>>,
        access: &'access ConstAccessor<'access>,
    ) -> Self {
        Self { cursor, access }
    }

    fn next<K, V>(&mut self) -> Result<(&'access K, &'access V)>
    where
        K: FromLmdbBytes + ?Sized,
        V: FromLmdbBytes + ?Sized,
    {
        Ok(self.cursor.write().unwrap().next(&self.access)?)
    }
}
