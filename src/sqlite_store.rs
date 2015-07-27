use std::path::{Path,PathBuf};
use std::fmt;
use std::thread;
use store::{Store, Values};
use std::sync::{Arc,Mutex, Condvar};
use std::error::Error;
use std::io::{self, Write};
use yak_client::Datum;
use sqlite3::{self, DatabaseConnection, ResultRowAccess};

type SeqCVar = Arc<(Mutex<i64>, Condvar)>;

#[derive(Clone)]
pub struct SqliteStore {
  path:  PathBuf,
  seqnotify: SeqCVar
}

#[automatically_derived]
impl fmt::Debug for SqliteStore {
  fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
    write!(fmt, "SqliteStore{{ path: {:?} }}", self.path)
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
  SqliteError(sqlite3::SqliteError),
  IoError(io::Error),
}

impl fmt::Display for SqliteError {
  fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
    match self {
      &SqliteError::SqliteError(ref err) => write!(fmt, "Store error:{}", err),
      &SqliteError::IoError(ref err) => write!(fmt, "IO error:{}", err),
    }
  }
}

impl Error for SqliteError {
  fn description(&self) -> &str {
    match self {
      &SqliteError::SqliteError(ref mdb) => mdb.description(),
      &SqliteError::IoError(ref err) => err.description(),
    }
  }
}

const EMPTY_SEQ_INIT : i64 = -1;

impl SqliteStore {
  pub fn new(path: &Path) -> Result<SqliteStore, SqliteError> {
    let mut buf = path.to_path_buf();
    buf.push("queues.sqlite");
    let store = SqliteStore { path: buf, seqnotify: Arc::new((Mutex::new(EMPTY_SEQ_INIT), Condvar::new())) };
    let mut db = try!(store.open_db());
    try!(db.exec("CREATE TABLE IF NOT EXISTS logs (
                 space           VARCHAR NOT NULL,
                 seq             INT NOT NULL,
                 key             BLOB NOT NULL,
                 value           BLOB NOT NULL,
                 PRIMARY KEY (space, seq)
               )"));
    Ok(store)
  }

  fn open_db(&self) -> Result<DatabaseConnection, SqliteError> {
    let res = sqlite3::access::open(&self.path.to_string_lossy(), None);
    match &res {
      &Ok(_) => trace!("Opened {:?}!", self.path),
      &Err(ref e) => error!("Open failed:{}", e)
    }
    let db = try!(res);
    Ok(db)
  }
}

impl Store for SqliteStore {
  type Iter = SqliteIterator;
  type Error = SqliteError;

  fn truncate(&self, space: &str) -> Result<(), SqliteError> {
    let db = try!(self.open_db());
    {
      let sql = "DELETE FROM logs WHERE space = ?1";
      trace!("{}@[{:?}]", sql, space);
      let mut stmt = try!(db.prepare(sql));
      try!(stmt.bind_text(1, space));
      let mut results = stmt.execute();
      while let Some(_) = try!(results.step()) { }
    }
    Ok(())
  }

  fn read(&self, space: &str, key: &[u8]) -> Result<Values, SqliteError> {
    trace!("#read:{:?}", key);

    let db = try!(self.open_db());
    let mut res = Vec::new();

    {
      let sql = "SELECT value FROM logs WHERE space = ?1 AND key = ?2";
      let mut stmt = try!(db.prepare(sql));
      trace!("{}@[{:?}, {:?}]", sql, space, key);
      try!(stmt.bind_text(1, space));
      try!(stmt.bind_blob(2, key));
      let mut results = stmt.execute();
      while let Some(mut row) = try!(results.step()) {
        let v = row.get(0);
        res.push(v)
      }
    }

    Ok(res)
  }

  fn write(&self, space: &str, key: &[u8], val: &[u8]) -> Result<(), SqliteError> {
    trace!("#write: {:?}/{:?}={:?}", space, key, val);
    let db = try!(self.open_db());

    let idx = {
      try!(retry_on_locked(|| {
        let sql = "SELECT seq+1 FROM logs WHERE space = ? ORDER BY seq DESC LIMIT 1";
        trace!("{}@[{:?}]", sql, space);
        let mut stmt = try!(db.prepare(sql));
        try!(stmt.bind_text(1, space));
        let mut results = stmt.execute();
        let mut idx = 0;
        while let Some(mut row) = try!(results.step()) {
          idx = row.get(0);
        }
        Ok(idx)
      }))
    };

    try!(retry_on_locked(|| {
      let sql = "INSERT INTO logs (seq, space, key, value) VALUES (?, ?, ?, ?)";
      trace!("{}@[{:?}, {:?}, {:?}, {:?}]", sql, idx, space, key, val);
      let mut ins = try!(db.prepare(sql));
      try!(ins.bind_int64(1, idx));
      try!(ins.bind_text(2, space));
      try!(ins.bind_blob(3, key));
      try!(ins.bind_blob(4, val));
      let mut results = ins.execute();

      while let Some(_) = try!(results.step()) { }
      Ok(())
    }));

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

fn retry_on_locked<T, F>(mut f: F) -> Result<T, sqlite3::SqliteError> where F: FnMut() -> Result<T, sqlite3::SqliteError> {
  loop {
    let r = f();
    match r { 
      Err(sqlite3::SqliteError { kind, .. }) if kind == sqlite3::SqliteErrorCode::SQLITE_LOCKED || kind == sqlite3::SqliteErrorCode::SQLITE_BUSY => {
        trace!("retry-redo: {:?}", kind);
        thread::sleep_ms(1);
      },
      Ok(_) => return r,
      Err(e) => return Err(e),
    }
  }
}

impl SqliteIterator {
  fn fetch_next(&mut self) -> Result<Option<Datum>, SqliteError> {
    trace!("#fetch_next: {:?}", self);
    loop {
      let query = retry_on_locked(|| {
        let sql = "SELECT seq, key, value FROM logs WHERE space = ? AND seq = ? ORDER BY seq ASC /* LIMIT 1 */";
        trace!("{}@[{}, {}]", sql, self.space, self.next_idx);
        let mut q = try!(self.db.prepare(sql));
        try!(q.bind_text(1, &self.space));
        try!(q.bind_int64(2, self.next_idx));
        let mut results = q.execute();

        let mut answer = None;
        while let Some(mut row) = try!(results.step()) {
          let seq : i64 = row.get(0);
          let key = row.get(1);
          let value = row.get(2);
          let datum = Datum { key: key, content: value };
          debug!("Result:{} / @{:?} {:?}", row.column_count(), seq, datum);
          self.next_idx = seq+1;
          answer = Some(datum);
        }
        Ok(answer)
      });
      match query {
        Ok(None) => (),
        ret => return ret.map_err(From::from),
      }

      {
        let &(ref lock, ref cvar) = &*self.seqnotify;
        trace!("Nothing found: @{:?}; waiting", self);
        let mut seq = lock.lock().unwrap();
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
    write!(fmt, "MemStoreIter{{ db: <db>, next_idx:{:?}, space:{:?} }}",
      &self.next_idx, &self.space)
  }
}
impl From<sqlite3::SqliteError> for SqliteError {
  fn from(err: sqlite3::SqliteError) -> SqliteError {
    SqliteError::SqliteError(err)
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
