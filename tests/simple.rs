#![feature(convert)]
#![feature(plugin)]
#![plugin(quickcheck_macros)]

#[macro_use]
extern crate log;
extern crate env_logger;
extern crate yak_client;

extern crate quickcheck;

use quickcheck::quickcheck;
use std::env;

use yak_client::{Client, YakError};

fn open_from_env(env_var: &str, name: &str) -> Client {
  let yak_url = env::var(env_var).ok()
    .expect(format!("env var {} not found", env_var).as_str());
  let full_url = format!("{}-{}", yak_url, name);
  Client::connect(&full_url).unwrap()
}

fn open_client(name: &str) -> (Client, Client) {
  let head = open_from_env("YAK_HEAD", name);
  let tail = open_from_env("YAK_TAIL", name);
  (head, tail)
}

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

#[quickcheck]
fn test_put_read_single_value_qc(key: Vec<u8>, val: Vec<u8>) -> Result<bool, YakError> {
  let (mut head, mut tail) = open_client("test_put_read_single_value_qc");
  try!(head.truncate());
  try!(head.write(&key, &val));

  let resp = try!(tail.read(&key));
  let contents : Vec<_> = resp.iter().map(|x| x.content.to_vec()).collect();
  Ok(contents == vec![val])
}
