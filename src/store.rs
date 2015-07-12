extern crate quickcheck;

use std::error::Error;
use std::any::Any;
use yak_client::Datum;

pub type Key = (String, Vec<u8>);
pub type Val = Vec<u8>;
pub type Values = Vec<Val>;

pub trait Store : Clone {
  type Iter: Iterator<Item=Datum>;
  type Error: Error + Any + Send + 'static;
  fn truncate(&self, space: &str) -> Result<(), Self::Error>;
  fn read(&self, space: &str, key: &[u8]) -> Result<Values, Self::Error>;
  fn write(&self, space: &str, key: &[u8], val: &[u8]) -> Result<(), Self::Error>;
  fn subscribe(&self, space: &str) -> Result<Self::Iter, Self::Error> ;
}

macro_rules! try_as_any {
    ($expr:expr) => { try!($expr.map_err(|e| { error!("Error: {}", e); Box::new(e) as Box<Any + Send>} )) }
}

#[cfg(test)]
#[macro_use]
pub mod test {
  use super::*;
  use std::thread;
  use std::sync::{Arc, Barrier, Once, ONCE_INIT};
  use std::any::Any;
  use quickcheck::TestResult;
  use log4rs;

  static LOG_INIT: Once = ONCE_INIT;
  static LOG_FILE: &'static str = "log-test.toml";

  fn log_init() {
    LOG_INIT.call_once(|| {
      log4rs::init_file(LOG_FILE, Default::default()).unwrap();
    })
  }

  type BoxedError = Box<Any + Send>;

  pub trait TestableStore : Store + Sync + Sized + Send + 'static {
    fn build() -> Self;
    fn test_put_read_values_qc(kvs: Vec<(Vec<u8>, Vec<u8>)>, needle_sel: usize) -> Result<TestResult, BoxedError> {
      log_init();
      let store = Self::build();

      let space = "X";
      for &(ref key, ref val) in &kvs {
        trace!("Write val: {:?}/{:?}={:?}", &space, &key, &val);
        try_as_any!(store.write(&space, &key, &val));
        trace!("Wrote val: {:?}/{:?}={:?}", &space, &key, &val);
      }

      if kvs.len() > 0 {
        let (ref needle, _) = kvs[needle_sel % kvs.len()];
        trace!("Needle: {:?}", needle);
        let expected : Vec<super::Val> = kvs.iter()
          .filter_map(|&(ref k, ref v)| if k == needle { Some(v.clone()) } else { None })
          .collect();

        trace!("Reading: {:?}/{:?}", &space, &needle);
        let actual : Vec<_> = try_as_any!(store.read(&space, &needle));
        debug!("Got     : {:?}", actual);
        debug!("Expected: {:?}", expected);
        debug!("Ok?     : {:?}", expected == actual);
        Ok(TestResult::from_bool(expected == actual))
      } else {
        Ok(TestResult::discard())
      }
    }

    fn test_put_subscribe_values_qc(kvs: Vec<(Vec<u8>, Vec<u8>)>) -> Result<bool, BoxedError> {
      log_init();
      let store = Self::build();

      let space = "test_put_subscribe_values_qc";
      for &(ref key, ref val) in &kvs {
        try_as_any!(store.write(&space, &key, &val));
      }

      debug!("Expected: {:?}", kvs);
      let actual : Vec<_> = try_as_any!(store.subscribe(&space)).take(kvs.len()).map(|d| (d.key, d.content) ).collect();

      debug!("Got     : {:?}", actual);
      debug!("Ok?     : {:?}", kvs == actual);
      Ok(kvs == actual)
    }

    fn test_put_subscribe_values_per_space(kvs: Vec<(bool, Vec<u8>, Vec<u8>)>) -> Result<bool, BoxedError> {
      log_init();
      debug!("Data    : {:?}", kvs);
      let store = Self::build();

      let space_prefix = "test_put_subscribe_values_qc";
      for &(ref space_suff, ref key, ref val) in &kvs {
        let space = format!("{}/{}", space_prefix, space_suff);
        try_as_any!(store.write(&space, &key, &val));
      }

      let expected : Vec<_> = kvs.iter().filter_map(|x| if x.0 { Some(x.clone()) } else { None }).collect();

      let actual : Vec<_> = try_as_any!(store.subscribe(&format!("{}/{}", space_prefix, true)))
        .take(expected.len())
        .map(|d| (true, d.key, d.content) )
        .collect();
      debug!("Got     : {:?}", actual);
      debug!("Expected: {:?}", expected);
      debug!("Ok?     : {:?}", expected == actual);
      Ok(expected == actual)
    }

    fn test_put_async_subscribe_values_qc(kvs: Vec<(Vec<u8>, Vec<u8>)>) -> Result<bool, BoxedError> {
      use rand::{thread_rng, Rng};
      log_init();
      static TEST_NAME : &'static str = "test_put_async_subscribe_values_qc";

      debug!("-------------------------------------------------------------------------------");

      let store = Self::build();
      let space = TEST_NAME;
      let barrier = Arc::new(Barrier::new(2));
      let mut rng = thread_rng();
      let tname = format!("{}::subscriber-{}", thread::current().name().unwrap_or(TEST_NAME), rng.gen_ascii_chars().take(4).collect::<String>());
      debug!("Subscriber thread: {}", tname);
      let builder = thread::Builder::new().name(tname);

      let expected_items = kvs.len();

      let child = {
        let barrier = barrier.clone();
        let store = store.clone();
        builder.spawn(move || {
            let sub = store.subscribe(&space).unwrap();
            barrier.wait();
            sub.take(expected_items).map(|d| (d.key, d.content) ).collect()
          }).unwrap()
      };

      barrier.wait();
      for &(ref key, ref val) in &kvs {
        debug!("Write:{:?}={:?}", key, val);
        try_as_any!(store.write(&space, &key, &val));
      }

      let actual : Vec<_> = try!(child.join());
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
        ::quickcheck::quickcheck($t::test_put_read_values_qc as fn (kvs: Vec<(Vec<u8>, Vec<u8>)>, needle_sel: usize) -> Result<TestResult, Box<::std::any::Any+Send>>)
      }

      #[test]
      fn test_put_subscribe_values_qc() {
        ::quickcheck::quickcheck($t::test_put_subscribe_values_qc as fn(kvs: Vec<(Vec<u8>, Vec<u8>)>) -> Result<bool, Box<::std::any::Any+Send>>)
      }

      #[test]
      fn test_put_subscribe_values_per_space() {
        ::quickcheck::quickcheck($t::test_put_subscribe_values_per_space as fn (kvs: Vec<(bool, Vec<u8>, Vec<u8>)>) -> Result<bool, Box<::std::any::Any+Send>>)
      }

      #[test]
      fn test_put_async_subscribe_values_qc() {
        ::quickcheck::quickcheck($t::test_put_async_subscribe_values_qc as fn(kvs: Vec<(Vec<u8>, Vec<u8>)>) -> Result<bool, Box<::std::any::Any+Send>>)
      }
    }
  }
}
