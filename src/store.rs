extern crate quickcheck;
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

pub type Key = (String, Vec<u8>);
pub type Val = Vec<u8>;
pub type Values = Vec<Val>;

pub trait Store {
  type Iter: Iterator<Item=Datum>;
  fn truncate(&self, space: &str);
  fn read(&self, space: &str, key: &[u8]) -> Values;
  fn write(&self, space: &str, key: &[u8], val: &[u8]);
  fn subscribe(&self, space: &str) -> Self::Iter ;
}

#[cfg(test)]
#[macro_use]
pub mod test {
  use super::*;
  use std::thread;
  use std::sync::{Arc, Mutex, Barrier, Once, ONCE_INIT};
  use yak_client::{Datum,YakError};
  use env_logger;
  use quickcheck::TestResult;
  use log4rs;

  static LOG_INIT: Once = ONCE_INIT;
  static LOG_FILE: &'static str = "log-test.toml";

  fn log_init() {
    LOG_INIT.call_once(|| {
      log4rs::init_file(LOG_FILE, Default::default()).unwrap();
    })
  }


  pub trait TestableStore : Store + Sync + Sized {
    fn build() -> Self;
    fn test_put_read_values_qc(kvs: Vec<(Vec<u8>, Vec<u8>)>, needle_sel: usize) -> Result<TestResult, YakError> {
      log_init();
      let mut store = Self::build();

      let space = "X";
      for &(ref key, ref val) in &kvs {
        store.write(&space, &key, &val);
      }

      if kvs.len() > 0 {
        let (ref needle, _) = kvs[needle_sel % kvs.len()];
        let expected : Vec<super::Val> = kvs.iter()
          .filter_map(|&(ref k, ref v)| if k == needle { Some(v.clone()) } else { None })
          .collect();

        let actual : Vec<_> = store.read(&space, &needle);
        debug!("Got     : {:?}", actual);
        debug!("Expected: {:?}", expected);
        debug!("Ok?     : {:?}", expected == actual);
        Ok(TestResult::from_bool(expected == actual))
      } else {
        Ok(TestResult::discard())
      }
    }

    fn test_put_subscribe_values_qc(kvs: Vec<(Vec<u8>, Vec<u8>)>) -> Result<bool, YakError> {
      log_init();
      let mut store = Self::build();

      let space = "test_put_subscribe_values_qc";
      for &(ref key, ref val) in &kvs {
        store.write(&space, &key, &val);
      }

      debug!("Expected: {:?}", kvs);
      let actual : Vec<_> = store.subscribe(&space).take(kvs.len()).map(|d| (d.key, d.content) ).collect();

      debug!("Got     : {:?}", actual);
      debug!("Ok?     : {:?}", kvs == actual);
      Ok(kvs == actual)
    }

    fn test_put_subscribe_values_per_space(kvs: Vec<(bool, Vec<u8>, Vec<u8>)>) -> Result<bool, YakError> {
      log_init();
      let mut store = Self::build();

      let space_prefix = "test_put_subscribe_values_qc";
      for &(ref space_suff, ref key, ref val) in &kvs {
        let space = format!("{}/{}", space_prefix, space_suff);
        store.write(&space, &key, &val);
      }

      let expected : Vec<_> = kvs.iter().filter_map(|x| if x.0 { Some(x.clone()) } else { None }).collect();

      let actual : Vec<_> = store.subscribe(&format!("{}/{}", space_prefix, true))
        .take(expected.len())
        .map(|d| (true, d.key, d.content) )
        .collect();
      debug!("Data    : {:?}", kvs);
      debug!("Got     : {:?}", actual);
      debug!("Expected: {:?}", expected);
      debug!("Ok?     : {:?}", expected == actual);
      Ok(expected == actual)
    }

    fn test_put_async_subscribe_values_qc(kvs: Vec<(Vec<u8>, Vec<u8>)>) -> Result<bool, YakError> {
      log_init();
      static TEST_NAME : &'static str = "test_put_async_subscribe_values_qc";

      let mut store = Self::build();
      let space = TEST_NAME;
      let barrier = Arc::new(Barrier::new(2));
      let builder = thread::Builder::new().name(format!("{}::subscriber", thread::current().name().unwrap_or(TEST_NAME)));

      let b = barrier.clone();
      let expected_items = kvs.len();

      let child = builder.scoped(|| {
          let mut sub = store.subscribe(&space);
          barrier.wait();
          sub.take(expected_items).map(|d| (d.key, d.content) ).collect()
          }).unwrap();

      barrier.wait();
      for &(ref key, ref val) in &kvs {
        debug!("Write:{:?}={:?}", key, val);
        store.write(&space, &key, &val);
      }

      let actual : Vec<_> = child.join();
      debug!("Got     : {:?}", actual);
      debug!("Expected: {:?}", kvs);
      debug!("Ok?     : {:?}", kvs == actual);
      Ok(kvs == actual)
    }
  }

  macro_rules! build_store_tests {
    ($t:ident) => {
      #[test]
      fn test_put_read_values_qc() {
        ::quickcheck::quickcheck($t::test_put_read_values_qc as fn (kvs: Vec<(Vec<u8>, Vec<u8>)>, needle_sel: usize) -> Result<TestResult, YakError>)
      }

      #[test]
      fn test_put_subscribe_values_qc() {
        ::quickcheck::quickcheck($t::test_put_subscribe_values_qc as fn(kvs: Vec<(Vec<u8>, Vec<u8>)>) -> Result<bool, YakError>)
      }

      #[test]
      fn test_put_subscribe_values_per_space() {
        ::quickcheck::quickcheck($t::test_put_subscribe_values_per_space as fn (kvs: Vec<(bool, Vec<u8>, Vec<u8>)>) -> Result<bool, YakError>)
      }

      #[test]
      fn test_put_async_subscribe_values_qc() {
        ::quickcheck::quickcheck($t::test_put_async_subscribe_values_qc as fn(kvs: Vec<(Vec<u8>, Vec<u8>)>) -> Result<bool, YakError>)
      }
    }
  }
}
