use std::path::{Path,PathBuf};
use std::fmt;
use std::thread;
use store::{Store, Values};
use std::sync::{Arc,Mutex, Condvar};
use std::error::Error;
use std::io::{self, Write};
use yak_client::Datum;
use rusqlite;
extern crate r2d2;
extern crate r2d2_sqlite;

type DatabaseConnection = r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>;

type SeqCVar = Arc<(Mutex<i64>, Condvar)>;

#[derive(Clone)]
pub struct SqliteStore {
  pool:  r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
  seqnotify: SeqCVar
}

#[automatically_derived]
impl fmt::Debug for SqliteStore {
  fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
    write!(fmt, "SqliteStore{{ pool: ??? }}")
  }
}

struct SqliteIterator{ 
  db: DatabaseConnection,
  space: String,
  next_idx: i64,
  seqnotify: SeqCVar
}

#[derive(Debug)]
pub enum SqliteError {
  SqliteError(rusqlite::SqliteError),
  PoolError(r2d2::GetTimeout),
  IoError(io::Error),
}

impl fmt::Display for SqliteError {
  fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
    match self {
      &SqliteError::SqliteError(ref err) => write!(fmt, "Store error:{}", err),
      &SqliteError::PoolError(ref err) => write!(fmt, "Pool error:{}", err),
      &SqliteError::IoError(ref err) => write!(fmt, "IO error:{}", err),
    }
  }
}

impl Error for SqliteError {
  fn description(&self) -> &str {
    match self {
      &SqliteError::SqliteError(ref mdb) => mdb.description(),
      &SqliteError::PoolError(ref err) => err.description(),
      &SqliteError::IoError(ref err) => err.description(),
    }
  }
}

const EMPTY_SEQ_INIT : i64 = -1;

impl SqliteStore {
  pub fn new(path: &Path) -> Result<SqliteStore, SqliteError> {
    let mut buf = path.to_path_buf();
    buf.push("queues.sqlite");

    let config = r2d2::Config::builder()
        .error_handler(Box::new(r2d2::LoggingErrorHandler))
        .build();
    let manager = r2d2_sqlite::SqliteConnectionManager::new(&buf.to_string_lossy()).unwrap();

    let pool = r2d2::Pool::new(config, manager).unwrap();


    let store = SqliteStore { pool: pool, seqnotify: Arc::new((Mutex::new(EMPTY_SEQ_INIT), Condvar::new())) };
    let mut db = try!(store.open_db());
    try!(db.execute("CREATE TABLE IF NOT EXISTS logs (
                 space           VARCHAR NOT NULL,
                 seq             INT NOT NULL,
                 key             BLOB NOT NULL,
                 value           BLOB NOT NULL,
                 PRIMARY KEY (space, seq)
               )", &[]));
    Ok(store)
  }

  fn open_db(&self) -> Result<DatabaseConnection, SqliteError> {
    let db = try!(self.pool.get());
    Ok(db)
  }
}

impl Store for SqliteStore {
  type Iter = SqliteIterator;
  type Error = SqliteError;

  fn truncate(&self, space: &str) -> Result<(), SqliteError> {
    let db = try!(self.open_db());
    let sql = "DELETE FROM logs WHERE space = ?1";
    trace!("{}@[{:?}]", sql, space);
    try!(db.execute(sql, &[&space]));
    Ok(())
  }

  fn read(&self, space: &str, key: &[u8]) -> Result<Values, SqliteError> {
    trace!("#read:{:?}", key);

    let db = try!(self.open_db());
    let res = {
      let sql = "SELECT value FROM logs WHERE space = ?1 AND key = ?2";
      let mut stmt = try!(db.prepare(sql));
      trace!("{}@[{:?}, {:?}]", sql, space, key);
      let rows = try!(stmt.query_map(&[&space, &key], |row| {
            let vec = row.get::<Vec<u8>>(0);
            trace!("Row:{:?}", vec);
            vec
            }));
      try!(rows.collect())
    };

    Ok(res)
  }

  fn write(&self, space: &str, key: &[u8], val: &[u8]) -> Result<(), SqliteError> {
    trace!("#write: {:?}/{:?}={:?}", space, key, val);
    let db = try!(self.open_db());

    let sql = "SELECT seq+1 FROM logs WHERE space = ? ORDER BY seq DESC LIMIT 1";
    trace!("{}@[{:?}]", sql, space);
    let mut stmt = try!(db.prepare(sql));
    let idxo = try!(stmt.query_map(&[&space], |r| r.get(0))).next();
    let idx = try!(idxo.unwrap_or(Ok(0)));

    let sql = "INSERT INTO logs (seq, space, key, value) VALUES (?, ?, ?, ?)";
    trace!("{}@[{:?}, {:?}, {:?}, {:?}]", sql, idx, space, key, val);
    try!(db.execute(sql, &[&idx, &space, &key, &val]));

    {
      debug!("Notify of new idx: {}", idx);
      let &(ref lock, ref cvar) = &*self.seqnotify;
      let mut idx_ref = lock.lock().unwrap();
      debug!("Obtained lock! {} → {}", &*idx_ref, idx);
      *idx_ref = idx;
      cvar.notify_all();
    }

    Ok(())
  }

  fn subscribe(&self, space: &str) -> Result<Self::Iter, SqliteError> {
    trace!("#subscribe: {:?}", space);
    let db = try!(self.open_db());
    Ok(SqliteIterator{ db: db, space: space.to_string(), next_idx: 0, seqnotify: self.seqnotify.clone() })
  }
}

impl SqliteIterator {
  fn fetch_next(&mut self) -> Result<Option<Datum>, SqliteError> {
    trace!("#fetch_next: {:?}", self);
    loop {
      let sql = "SELECT seq, key, value FROM logs WHERE space = ? AND seq = ? ORDER BY seq ASC /* LIMIT 1 */";
      let mut q = try!(self.db.prepare(sql));
      trace!("{}@[{}, {}]", sql, self.space, self.next_idx);

      let mut results = try!(q.query(&[&self.space, &self.next_idx]));

      while let Some(mut rowp) = results.next() {
        let row = try!(rowp);
        let seq : i64 = row.get::<i64>(0);
        let key = row.get(1);
        let value = row.get(2);
        let datum = Datum { key: key, content: value };
        debug!("Result: @{:?} {:?}", seq, datum);
        self.next_idx = seq+1;
        return Ok(Some(datum))
      }

      {
        let &(ref lock, ref cvar) = &*self.seqnotify;
        trace!("Nothing found: @{:?}; waiting", self);
        let mut seq = lock.lock().unwrap();
        trace!("Current seq: {:?}; db seq: {:?};", self.next_idx, *seq);
        // This assumption gets invalidated when we truncate the database; we 
        // need to invalidate existing iterators on truncate; or disallow
        // truncate when iterators are open.
        // First thought: epoch counter?
        while self.next_idx > *seq {
          trace!("Wait! want next-idx:{:?} > current:{:?}", &*seq, self.next_idx);
          let (lockp, _no_timeout) = cvar.wait_timeout_ms(seq, 1000).unwrap();
          seq = lockp;
          trace!("Awoken! current:{:?}; expected next-idx:{:?}; timeout? {:?}", &*seq, self.next_idx, _no_timeout);
        }
      }
    }
  }

}
impl Iterator for SqliteIterator {
  type Item = Datum;

  fn next(&mut self) -> Option<Self::Item> {
    trace!("Iterator#next {:?}", self);

    self.fetch_next().unwrap()
  }
}

impl fmt::Debug for SqliteIterator {
  fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
    write!(fmt, "SqliteIterator{{ db: <db>, next_idx:{:?}, space:{:?} }}",
      &self.next_idx, &self.space)
  }
}
impl From<rusqlite::SqliteError> for SqliteError {
  fn from(err: rusqlite::SqliteError) -> SqliteError {
    SqliteError::SqliteError(err)
  }
}

impl From<r2d2::GetTimeout> for SqliteError {
  fn from(err: r2d2::GetTimeout) -> SqliteError {
    SqliteError::PoolError(err)
  }
}

impl From<io::Error> for SqliteError {
  fn from(err: io::Error) -> SqliteError {
    SqliteError::IoError(err)
  }
}

#[cfg(test)]
mod test {
  use super::SqliteStore;
  use store::test::TestableStore;
  use quickcheck::TestResult;
  use rand::Rng;
  use std::path::PathBuf;
  use std::fs;
  
  impl TestableStore for SqliteStore {
    fn build() -> SqliteStore {
      let mut rng = ::rand::thread_rng();
      let p = PathBuf::from(format!("target/sqlite3_store/{}", rng.gen_ascii_chars().take(16).collect::<String>()));
      fs::create_dir_all(&p).unwrap();
      SqliteStore::new(&p).unwrap()
    }
  }

  build_store_tests!(SqliteStore);
}
