use std::fmt;
use std::sync::{Arc,Mutex, Condvar};
use std::collections::BTreeMap;
use std::clone::Clone;
use std::error::Error;

use yak_client::Datum;

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

#[derive(Debug)]
struct MemError;

impl fmt::Display for MemError {
  fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
    write!(fmt, "MemError")
  }
}

impl Error for MemError {
  fn description(&self) -> &str {
    "In memory error"
  }
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
    write!(fmt,
        "MemInner{{ nkeys:{:?}, nvals:{:?}, idx:{:?} }}",
        &self.by_key.len(),
      &self.vals.len(),
      &self.idx)
  }
}
impl fmt::Debug for MemStoreIter {
  fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
    match self.inner.try_lock() {
      Ok(ref inner) =>
        write!(fmt, "MemStoreIter{{inner:{:?}, off:{:?}, space:{:?} }}",
          &**inner, &self.off, &self.space),
      Err(_) =>
        write!(fmt, "MemStoreIter{{inner: ???, off:{:?}, space:{:?} }}",
          &self.off, &self.space)
    }
  }
}

impl Store for MemStore {
  type Iter = MemStoreIter;
  type Error = MemError;

  fn truncate(&self, space: &str) -> Result<(), MemError> {
    let mut inner = self.inner.lock().unwrap();
    let to_rm : Vec<_> = inner.by_key.iter().filter_map(|(k, v)| if k.0 == space { Some((k.clone(), v.clone())) } else { None }).collect();
    for (k, vs) in to_rm {
      inner.by_key.remove(&k);
      for v in vs {
        inner.vals.remove(&v);
      }
    }
    Ok(())
  }

  fn read(&self, space: &str, key: &[u8]) -> Result<Values, MemError> {
    let inner = self.inner.lock().unwrap();
    let k = (space.into(), key.into());
    let res = inner.by_key.get(&k)
      .map(|idxs|
          idxs.iter().filter_map(|i|
            inner.vals.get(i).iter().map(|&&(_, ref v)| v.clone()).next()
            ).collect() )
      .unwrap_or(vec![]);
    Ok(res)
  }

  fn write(&self, space: &str, key: &[u8], val: &[u8]) -> Result<(), MemError> {
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
    Ok(())
  }

  fn subscribe(&self, space: &str) -> Result<Self::Iter, MemError> {
    Ok(MemStoreIter { inner: self.inner.clone(), cvar: self.cvar.clone(), off: 0, space: space.to_string() })
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
  use yak_client::YakError;

  impl TestableStore for MemStore {
    fn build() -> MemStore {
      MemStore::new()
    }
  }

  build_store_tests!(MemStore);
}
