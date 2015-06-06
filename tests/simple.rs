#![feature(convert)]

#[macro_use]
extern crate log;
extern crate env_logger;
extern crate yak_client;

mod common;
use common::*;

#[test]
fn test_put_read_empty() {
  env_logger::init().unwrap_or(());

  let (mut head, mut tail) = open_client("test_put_read_empty");
  let key = "key";

  head.truncate().unwrap();

  let resp = tail.read(key.as_bytes()).unwrap();
  assert_eq!(resp.len(), 0);
}

#[test]
fn test_put_read_single_value() {
  env_logger::init().unwrap_or(());
  let (mut head, mut tail) = open_client("test_put_read_single_value");
  let key = "key";
  let val = "value";

  head.truncate().unwrap();

  head.write(key.as_bytes(), val.as_bytes()).unwrap();

  let resp = tail.read(key.as_bytes()).unwrap();
  assert_eq!(resp.len(), 1);
  assert_eq!(resp[0].content, val.as_bytes())
}

#[test]
fn test_put_read_two_values() {
  env_logger::init().unwrap_or(());
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
  env_logger::init().unwrap_or(());
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
  env_logger::init().unwrap_or(());
  let (mut head, mut tail) = open_client("test_subscribe_after_put_single_value");
  let key = b"key";
  let val = b"value";
  head.truncate().unwrap();
  head.write(key, val).unwrap();

  let mut subscription = tail.subscribe().unwrap();
  let maybe_message = subscription.fetch_next().unwrap();

  assert_eq!(maybe_message.map(|message| (message.key, message.content)), Some((key.to_vec(), val.to_vec())))
}
