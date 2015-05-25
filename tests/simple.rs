#[macro_use]
extern crate log;
extern crate env_logger;
extern crate yak_client;
use std::env;

use yak_client::Client;

fn open_client() -> Client {
  let yak_url = env::var("YAK_URL").unwrap();
  Client::connect(&yak_url).unwrap()
}

#[test]
fn test_put_read_empty() {
  env_logger::init().unwrap_or(());

  let mut client = open_client();
  let key = "key";
  let val = "value";

  client.truncate().unwrap();

  let resp = client.read(key.as_bytes()).unwrap();
  assert_eq!(resp.len(), 0);
}

#[test]
fn test_put_read_single_value() {
  env_logger::init().unwrap_or(());
  let mut client = open_client();
  let key = "key";
  let val = "value";

  client.truncate().unwrap();

  client.write(key.as_bytes(), val.as_bytes()).unwrap();

  let resp = client.read(key.as_bytes()).unwrap();
  assert_eq!(resp.len(), 1);
  assert_eq!(resp[0].content, val.as_bytes())
}

#[test]
fn test_put_read_two_values() {
  env_logger::init().unwrap_or(());
  let mut client = open_client();
  let key = "key";
  let vals = vec!["a".as_bytes(), "b".as_bytes()];

  client.truncate().unwrap();

  for val in &vals {
    client.write(key.as_bytes(), &val).unwrap();
  }

  let resp = client.read(key.as_bytes()).unwrap();
  let returned : Vec<Vec<u8>> = resp.iter().map(|v| v.content.clone()).collect();
  assert_eq!(returned, vals);
}



#[test]
fn test_truncate() {
  env_logger::init().unwrap_or(());
  let mut client = open_client();
  let key = "key";
  let val = "value";

  client.truncate().unwrap();
  client.write(key.as_bytes(), val.as_bytes()).unwrap();
  client.truncate().unwrap();

  let resp = client.read(key.as_bytes()).unwrap();
  assert_eq!(resp.len(), 0);
}
