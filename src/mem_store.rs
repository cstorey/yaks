use std::default::Default;
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::io::{self,Read,Write};
use std::fmt;
use std::sync::{Arc,Mutex, Condvar};
use std::collections::BTreeMap;
use std::clone::Clone;
use std::error::Error;

use yak_client::{Datum,YakError};

use store::{Store,Key, Val, Values};

#[derive(Clone)]
struct MemInner {
  by_key: BTreeMap<Key, Vec<usize>>,
  vals: BTreeMap<usize, (Key, Val)>,
  idx: usize,
}

#[derive(Clone)]
pub struct MemStore {
  inner: Arc<Mutex<MemInner>>,
  cvar: Arc<Condvar>,
}

struct MemStoreIter {
  inner: Arc<Mutex<MemInner>>,
  cvar: Arc<Condvar>,
  off: usize,
  space: String,
}

impl MemStore {
  pub fn new() -> MemStore {
    let inner = MemInner {
      by_key: BTreeMap::new(),
      vals: BTreeMap::new(),
      idx: 0,
    };
    MemStore {
      inner: Arc::new(Mutex::new(inner)),
      cvar: Arc::new(Condvar::new())
    }
  }
}

impl fmt::Debug for MemInner {
  fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
    fmt.debug_struct("Foo")
      .field("nkeys", &self.by_key.len())
      .field("nvals", &self.vals.len())
      .field("idx", &self.idx)
      .finish()
  }
}
impl fmt::Debug for MemStoreIter {
  fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
    let mut inner = self.inner.lock().unwrap();
    fmt.debug_struct("Foo")
      .field("inner", &*inner)
      .field("off", &self.off)
      .field("space", &self.space)
      .finish()
  }
}

impl Store for MemStore {
  type Iter = MemStoreIter;

  fn truncate(&self, space: &str) {
    let mut inner = self.inner.lock().unwrap();
    let to_rm : Vec<_> = inner.by_key.iter().filter_map(|(k, v)| if k.0 == space { Some((k.clone(), v.clone())) } else { None }).collect();
    for (k, vs) in to_rm {
      inner.by_key.remove(&k);
      for v in vs {
        inner.vals.remove(&v);
      }
    }
  }

  fn read(&self, space: &str, key: &[u8]) -> Values {
    let inner = self.inner.lock().unwrap();
    let k = (space.into(), key.into());
    inner.by_key.get(&k)
      .map(|idxs|
          idxs.iter().filter_map(|i|
            inner.vals.get(i).iter().map(|&&(_, ref v)| v.clone()).next()
            ).collect() )
      .unwrap_or(vec![])
  }

  fn write(&self, space: &str, key: &[u8], val: &[u8]) {
    let mut inner = self.inner.lock().unwrap();
    let k = (space.into(), key.into());
    let idx = inner.idx;
    {
      let entry = inner.by_key.entry(k).or_insert(vec![]);
      entry.push(idx);
    }
    inner.vals.insert(idx, ((space.into(), key.into()), val.into()));
    inner.idx += 1;
    debug!("Wrote @{}", inner.idx);
    self.cvar.notify_all();
  }

  fn subscribe(&self, space: &str) -> Self::Iter {
    MemStoreIter { inner: self.inner.clone(), cvar: self.cvar.clone(), off: 0, space: space.to_string() }
  }
}

impl Iterator for MemStoreIter {
  type Item = Datum;

  fn next(&mut self) -> Option<Self::Item> {
    let mut inner = self.inner.lock().unwrap();
    loop {
      let range = self.off..inner.idx;
      debug!("offset {:?} / {:?}", self.off, range);
      for off in range {
        self.off = off+1;
        let slot = inner.vals.get(&off);
        debug!("slot {:?} / {:?}", off, slot);
        if let Some(v) = slot {
          debug!("Item@{}! {:?}", off, v);
          let &(ref keyspace, ref val) = v;
          let &(ref kspace, ref key) = keyspace;
          if kspace == &self.space {
            let datum = Datum { key: key.clone(), content: val.clone() };
            debug!("Datum: {:?}", datum);
            return Some(datum)
          }
        }
      }
      trace!("Nothing found: @{:?}; waiting", &*inner);
      inner = self.cvar.wait(inner).unwrap();
      trace!("Awoken! @{:?}", &*inner);
    }
  }
}

#[cfg(test)]
mod test {
  use super::MemStore;
  use store::test::TestableStore;
  use quickcheck::TestResult;
  use yak_client::{Datum,YakError};

  impl TestableStore for MemStore {}

  #[quickcheck]
  fn test_put_read_values_qc(kvs: Vec<(Vec<u8>, Vec<u8>)>, needle_sel: usize) -> Result<TestResult, YakError> {
    let mut store = MemStore::new();
    TestableStore::test_put_read_values_qc(&mut store, kvs, needle_sel)
  }


  #[quickcheck]
  fn test_put_subscribe_values_qc(kvs: Vec<(Vec<u8>, Vec<u8>)>) -> Result<bool, YakError> {
    let mut store = MemStore::new();
    TestableStore::test_put_subscribe_values_qc(&mut store, kvs)
  }

  #[quickcheck]
  fn test_put_subscribe_values_per_space(kvs: Vec<(bool, Vec<u8>, Vec<u8>)>) -> Result<bool, YakError> {
    let mut store = MemStore::new();
    TestableStore::test_put_subscribe_values_per_space(&mut store, kvs)
  }

  #[quickcheck]
  fn test_put_async_subscribe_values_qc(kvs: Vec<(Vec<u8>, Vec<u8>)>) -> Result<bool, YakError> {
    let mut store = MemStore::new();
    TestableStore::test_put_async_subscribe_values_qc(&mut store, kvs)
  }
}
