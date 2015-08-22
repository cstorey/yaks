#[macro_use]
extern crate log;
extern crate log4rs;
extern crate yak_client;

use std::thread;

mod common;
use common::*;
use std::sync::{Arc, Barrier, Once, ONCE_INIT};

static LOG_INIT: Once = ONCE_INIT;
static LOG_FILE: &'static str = "log-test.toml";

fn log_init() {
  LOG_INIT.call_once(|| {
    log4rs::init_file(LOG_FILE, Default::default()).unwrap();
  })
}

#[test]
fn test_put_read_empty() {
  log_init();

  let (mut head, mut tail) = open_client("test_put_read_empty");
  let key = "key";

  head.truncate().unwrap();

  let resp = tail.read(key.as_bytes()).unwrap();
  assert_eq!(resp.len(), 0);
}

#[test]
fn test_put_read_single_value() {
  log_init();
  let (mut head, mut tail) = open_client("test_put_read_single_value");
  let key = "mykey";
  let val = "myvalue";

  head.truncate().unwrap();

  head.write(key.as_bytes(), val.as_bytes()).unwrap();

  let resp = tail.read(key.as_bytes()).unwrap();
  let expected_datum = yak_client::Datum { key: vec![], content: val.as_bytes().to_vec() };
  assert_eq!(resp, vec![expected_datum])
}

#[test]
fn test_put_read_two_values() {
  log_init();
  let (mut head, mut tail) = open_client("test_put_read_two_values");
  let key = "key";
  let vals = vec!["a".as_bytes(), "b".as_bytes()];

  head.truncate().unwrap();

  for val in &vals {
    head.write(key.as_bytes(), &val).unwrap();
  }

  let resp = tail.read(key.as_bytes()).unwrap();
  let returned : Vec<Vec<u8>> = resp.iter().map(|v| v.content.clone()).collect();
  assert_eq!(returned, vals);
}

#[test]
fn test_truncate() {
  log_init();
  let (mut head, mut tail) = open_client("test_truncate");
  let key = "key";
  let val = "value";

  head.truncate().unwrap();
  head.write(key.as_bytes(), val.as_bytes()).unwrap();
  head.truncate().unwrap();

  let resp = tail.read(key.as_bytes()).unwrap();
  assert_eq!(resp.len(), 0);
}

#[test]
fn test_subscribe_after_put_single_value() {
  static TEST_NAME: &'static str = "test_subscribe_after_put_single_value";
  log_init();
  let (mut head, tail) = open_client(TEST_NAME);
  let key = b"key";
  let val = b"value";
  head.truncate().unwrap();
  head.write(key, val).unwrap();

  let mut subscription = tail.subscribe().unwrap();
  let maybe_message = subscription.fetch_next().unwrap();

  assert_eq!(maybe_message.map(|message| (message.key, message.content)), Some((key.to_vec(), val.to_vec())))
}

#[test]
fn test_subscribe_async_deliveries() {
  static TEST_NAME: &'static str = "test_subscribe_async_deliveries";
  log_init();
  let (mut head, tail) = open_client(TEST_NAME);
  let key = b"key";
  let val = b"value";
  head.truncate().unwrap();

  let barrier = Arc::new(Barrier::new(2));

  let builder = thread::Builder::new().name(format!("{}::subscriber", thread::current().name().unwrap_or(TEST_NAME)));
  let b = barrier.clone();
  let sub_task = builder.spawn(move || {
    debug!("Starting subscriber");
    let mut subscription = tail.subscribe().unwrap();
    debug!("Await barrier");
    b.wait();
    debug!("Await next:");
    let ret = subscription.fetch_next().unwrap();
    debug!("Received: {:?}", ret);
    ret
  }).unwrap();

  debug!("Await barrier");
  barrier.wait();
  debug!("Write value");
  head.write(key, val).unwrap();

  let maybe_message = sub_task.join().unwrap();

  assert_eq!(maybe_message.map(|message| (message.key, message.content)), Some((key.to_vec(), val.to_vec())))
}
