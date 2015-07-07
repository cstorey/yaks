use std::path::Path;
use std::fmt;
use lmdb::{self, EnvBuilder, DbFlags};
use store::{Store,Key, Val, Values};
use std::sync::{Arc,Mutex, Condvar};
use std::error::Error;
use yak_client::{Datum,YakError};

#[derive(Clone)]
struct LmdbStore {
  env:  Arc<Mutex<lmdb::Environment>>,
}
struct LmdbIterator;

#[derive(Debug)]
pub enum LmdbError {
  LmdbError(lmdb::MdbError)
}

impl fmt::Display for LmdbError {
  fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
    unimplemented!()
  }
}

impl Error for LmdbError {
  fn description(&self) -> &str {
    match self {
      &LmdbError::LmdbError(ref mdb) => mdb.description()
    }
  }
}

impl LmdbStore {
  fn new(path: &Path) -> Result<LmdbStore, LmdbError> {
    warn!("Path: {:?}", path);
    let mut env = try!(EnvBuilder::new().open(&path, 0o777));

    Ok(LmdbStore { env: Arc::new(Mutex::new(env)) })
  }
}

impl Iterator for LmdbIterator {
  type Item = Datum;
  fn next(&mut self) -> Option<Self::Item> { None }
}

#[derive(Debug)]
pub enum DbKey {
  Index(String),
}

impl DbKey {
  fn encode(&self) -> Option<Vec<u8>> {
    unimplemented!()
  }
}

impl Store for LmdbStore {
  type Iter = LmdbIterator;
  type Error = LmdbError;

  fn truncate(&self, space: &str) -> Result<(), LmdbError> {
    Ok(())
  }

  fn read(&self, space: &str, key: &[u8]) -> Result<Values, LmdbError> {
    Ok(vec![])
  }

  fn write(&self, space: &str, key: &[u8], val: &[u8]) -> Result<(), LmdbError> {
    let mut env = self.env.lock().unwrap();
    let db_handle = env.get_default_db(DbFlags::empty()).unwrap();
    let txn = env.new_transaction().unwrap();
    {
        let db = txn.bind(&db_handle); // get a database bound to this transaction
        let key = DbKey::Index(space.to_string()).encode().unwrap();
        let idx : u64 = db.get(&key).unwrap();
    }
    Ok(())
  }

  fn subscribe(&self, space: &str) -> Result<Self::Iter, LmdbError> {
    Ok(LmdbIterator)
  }
}

impl From<lmdb::MdbError> for LmdbError {
  fn from(err: lmdb::MdbError) -> LmdbError {
    LmdbError::LmdbError(err)
  }
}

#[cfg(test)]
mod test {
  use super::LmdbStore;
  use store::test::TestableStore;
  use quickcheck::TestResult;
  use yak_client::{Datum,YakError};
  use rand::{thread_rng, Rng};
  use std::path::{Path,PathBuf};
  

  impl TestableStore for LmdbStore {
    fn build() -> LmdbStore {
      let mut rng = ::rand::thread_rng();
      let p = PathBuf::from(format!("target/lmdb_store/{}", rng.gen_ascii_chars().take(16).collect::<String>()));
      LmdbStore::new(&p).unwrap()
    }
  }

  // build_store_tests!(LmdbStore);
}
