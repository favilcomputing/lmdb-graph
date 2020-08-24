use lmdb::{
    db, open::Flags, ConstAccessor, Database, DatabaseOptions, ReadTransaction, WriteAccessor,
};
use lmdb_zero::{self as lmdb, EnvBuilder};
use rand::{distributions::Alphanumeric, thread_rng, Rng};

#[derive(Debug)]
enum Error {
    LMDB(lmdb::Error),
}

type Result<T> = core::result::Result<T, Error>;

impl From<lmdb::Error> for Error {
    fn from(error: lmdb::Error) -> Self {
        Error::LMDB(error)
    }
}

#[allow(unused_mut)]
fn main() -> Result<()> {
    env_logger::init();

    log::info!("Setting up environment");
    let mut builder = EnvBuilder::new()?;
    builder.set_maxdbs(10)?;
    let env = unsafe { builder.open("./test.db", Flags::empty(), 0o600) }?;

    log::info!("Creating database");
    let db = Database::open(&env, None, &DatabaseOptions::new(db::CREATE))?;
    {
        let mut txn = lmdb::WriteTransaction::new(&env)?;
        {
            let mut access: WriteAccessor = txn.access();
            access.clear_db(&db)?;
            access.put(
                &db,
                "Kevin",
                thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(30)
                    .collect::<String>()
                    .as_bytes(),
                lmdb::put::Flags::empty(),
            )?;
            access.put(
                &db,
                "Kylie",
                thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(30)
                    .collect::<String>()
                    .as_bytes(),
                lmdb::put::Flags::empty(),
            )?;
            access.put(
                &db,
                "Jensen",
                thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(30)
                    .collect::<String>()
                    .as_bytes(),
                lmdb::put::Flags::empty(),
            )?;
            access.put(&db, "Abby", "🍔∈🌏", lmdb::put::Flags::empty())?;
        }
        txn.commit()?;
    }
    {
        let mut txn = ReadTransaction::new(&env)?;
        {
            #[allow(unused_mut)]
            let mut access: ConstAccessor = txn.access();
            log::info!("Kevin {}", access.get::<str, str>(&db, &"Kevin")?);
            log::info!("Kylie {}", access.get::<str, str>(&db, &"Kylie")?);
            log::info!("Jensen {}", access.get::<str, str>(&db, &"Jensen")?);
            log::info!("Abby {}", access.get::<str, str>(&db, &"Abby")?);
        }
    }

    Ok(())
}
