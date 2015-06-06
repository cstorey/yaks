extern crate quickcheck;
use std::default::Default;
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::io::{self,Read,Write};
use std::fmt;
use std::sync::{Arc,Mutex};
use std::collections::BTreeMap;
use std::clone::Clone;
use std::error::Error;

use yak_client::{Datum,YakError};

type Key = (String, Vec<u8>);
type Val = Vec<u8>;
type Values = Vec<Val>;
#[derive(Debug,Clone)]
struct MemInner {
  by_key: BTreeMap<Key, Vec<usize>>,
  vals: BTreeMap<usize, (Key, Val)>,
  idx: usize,
}

#[derive(Debug,Clone)]
pub struct MemStore (Arc<Mutex<MemInner>>);
pub trait Store {
  type Iter: Iterator<Item=Datum>;
  fn truncate(&self, space: &str);
  fn read(&self, space: &str, key: &[u8]) -> Values;
  fn write(&self, space: &str, key: &[u8], val: &[u8]);
  fn subscribe(&self, space: &str) -> Self::Iter ;
}

impl MemStore {
  pub fn new() -> MemStore {
    let inner = MemInner { 
      by_key: BTreeMap::new(),
      vals: BTreeMap::new(),
      idx: 0,
    };
    MemStore(Arc::new(Mutex::new(inner)))
  }
}

impl Store for MemStore {
  type Iter = ::std::vec::IntoIter<Datum>;

  fn truncate(&self, space: &str) {
    let mut inner = self.0.lock().unwrap();
    let to_rm : Vec<_> = inner.by_key.iter().filter_map(|(k, v)| if k.0 == space { Some((k.clone(), v.clone())) } else { None }).collect();
    for (k, vs) in to_rm {
      inner.by_key.remove(&k);
      for v in vs {
        inner.vals.remove(&v);
      }
    }
  }

  fn read(&self, space: &str, key: &[u8]) -> Values {
    let inner = self.0.lock().unwrap();
    let k = (space.into(), key.into());
    inner.by_key.get(&k)
      .map(|idxs|
          idxs.iter().filter_map(|i|
            inner.vals.get(i).iter().map(|&&(_, ref v)| v.clone()).next()
            ).collect() )
      .unwrap_or(vec![])
  }
  fn write(&self, space: &str, key: &[u8], val: &[u8]) {
    let mut inner = self.0.lock().unwrap();
    let k = (space.into(), key.into());
    let idx = inner.idx;
    {
      let entry = inner.by_key.entry(k).or_insert(vec![]);
      entry.push(idx);
    }
    inner.vals.insert(idx, ((space.into(), key.into()), val.into()));
    inner.idx += 1;
  }

  fn subscribe(&self, space: &str) -> Self::Iter {
    let mut out = Vec::new();
    let &MemStore(ref ptr) = self;
    let inner = ptr.lock().unwrap();
    for v in inner.vals.values() {
      let &(ref keyspace, ref val) = v;
      let &(ref kspace, ref key) = keyspace;
      if kspace == space {
        out.push(Datum { key: key.clone(), content: val.clone() })
      }
    }

    let res : _ = out.into_iter();
    res
  }
}

#[cfg(test)]
mod test {
  use super::*;
  use yak_client::{Datum,YakError};
  use env_logger;
  use quickcheck::TestResult;

  #[quickcheck]
  fn test_put_read_values_qc(kvs: Vec<(Vec<u8>, Vec<u8>)>, needle_sel: usize) -> Result<TestResult, YakError> {
    env_logger::init().unwrap_or(());
    let store = MemStore::new();
    let space = "X";
    for &(ref key, ref val) in &kvs {
      store.write(&space, &key, &val);
    }

    if kvs.len() > 0 {
      let (ref needle, _) = kvs[needle_sel % kvs.len()];
      let expected : Vec<super::Val> = kvs.iter().filter_map(|&(ref k, ref v)| if k == needle { Some(v.clone()) } else { None }).collect();

      let actual : Vec<_> = store.read(&space, &needle);
      debug!("Got     : {:?}", actual);
      debug!("Expected: {:?}", expected);
      debug!("Ok?     : {:?}", expected == actual);
      Ok(TestResult::from_bool(expected == actual))
    } else {
      Ok(TestResult::discard())
    }
  }

  #[quickcheck]
  fn test_put_subscribe_values_qc(kvs: Vec<(Vec<u8>, Vec<u8>)>) -> Result<bool, YakError> {
    env_logger::init().unwrap_or(());
    let store = MemStore::new();
    let space = "test_put_subscribe_values_per_space";
    for &(ref key, ref val) in &kvs {
      store.write(&space, &key, &val);
    }

    let actual : Vec<_> = store.subscribe(&space).map(|d| (d.key, d.content) ).collect();
    debug!("Got     : {:?}", actual);
    debug!("Expected: {:?}", kvs);
    debug!("Ok?     : {:?}", kvs == actual);
    Ok(kvs == actual)
  }

  #[quickcheck]
  fn test_put_subscribe_values_per_space(kvs: Vec<(bool, Vec<u8>, Vec<u8>)>) -> Result<bool, YakError> {
    env_logger::init().unwrap_or(());
    let store = MemStore::new();
    let space_prefix = "test_put_subscribe_values_qc";
    for &(ref space_suff, ref key, ref val) in &kvs {
      let space = format!("{}/{}", space_prefix, space_suff);
      store.write(&space, &key, &val);
    }

    let actual : Vec<_> = store.subscribe(&format!("{}/{}", space_prefix, true)).map(|d| (true, d.key, d.content) ).collect();
    let expected : Vec<_> = kvs.iter().filter_map(|x| if x.0 { Some(x.clone()) } else { None }).collect();
    debug!("Data    : {:?}", kvs);
    debug!("Got     : {:?}", actual);
    debug!("Expected: {:?}", expected);
    debug!("Ok?     : {:?}", expected == actual);
    Ok(expected == actual)
  }
}
