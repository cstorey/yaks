#[macro_use]
extern crate log;
extern crate env_logger;

extern crate yak_client;

use yak_client::Client;

#[test]
fn test_put_read_empty() {
  env_logger::init().unwrap_or(());

  let mut client = Client::connect("yak://127.0.0.1:7777/tests").unwrap();
  let key = "key";
  let val = "value";

  client.truncate().unwrap();

  let resp = client.read(key.as_bytes()).unwrap();
  assert_eq!(resp.len(), 0);
}

#[test]
fn test_put_read_single_value() {
  env_logger::init().unwrap_or(());
  let mut client = Client::connect("yak://127.0.0.1:7777/tests").unwrap();
  let key = "key";
  let val = "value";

  client.truncate().unwrap();

  client.write(key.as_bytes(), val.as_bytes()).unwrap();

  let resp = client.read(key.as_bytes()).unwrap();
  assert_eq!(resp.len(), 1);
  assert_eq!(resp[0].content, val.as_bytes())
}

#[test]
fn test_truncate() {
  env_logger::init().unwrap_or(());
  let mut client = Client::connect("yak://127.0.0.1:7777/tests").unwrap();
  let key = "key";
  let val = "value";

  client.truncate().unwrap();
  client.write(key.as_bytes(), val.as_bytes()).unwrap();
  client.truncate().unwrap();

  let resp = client.read(key.as_bytes()).unwrap();
  assert_eq!(resp.len(), 0);
}
