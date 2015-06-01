#![feature(convert)]
#![feature(plugin)]
#![plugin(quickcheck_macros)]

#[macro_use]
extern crate log;
extern crate env_logger;
extern crate yak_client;
extern crate quickcheck;

use std::collections::HashMap;
use yak_client::YakError;

mod common;
use common::*;

#[quickcheck]
fn test_put_read_values_qc(kvs: Vec<(String, String)>) -> Result<bool, YakError> {
  env_logger::init().unwrap_or(());

  let (mut head, mut tail) = open_client("test_put_read_single_value_qc");
  try!(head.truncate());
  info!("Kvs: {:?}", kvs);
  let mut expected = HashMap::new();
  for &(ref key, ref val) in &kvs {
    try!(head.write(key.as_bytes(), val.as_bytes()));
    let mut items = expected.entry(key).or_insert(vec![]);
    items.push(val.clone());
  }
  let mut actual = HashMap::new();
  for &(ref key, ref _val) in &kvs {
    let resp : Vec<String> = try!(tail.read(key.as_bytes())).iter().map(|d| String::from_utf8_lossy(&d.content).to_string()).collect();
    actual.insert(key, resp);
  }
  Ok(expected == actual)
}
