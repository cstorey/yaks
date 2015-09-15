#![feature(convert)]
#![feature(plugin,test)]

#[macro_use]
extern crate log;
extern crate env_logger;
extern crate yak_client;
extern crate test;

use std::collections::HashMap;
use yak_client::YakError;

use std::env;

use yak_client::Client;
use test::Bencher;

pub fn open_from_env(env_var: &str, name: &str) -> Client {
  let yak_url = env::var(env_var).ok()
    .expect(format!("env var {} not found", env_var).as_str());
  let full_url = format!("{}-{}", yak_url, name);
  Client::connect(&full_url).unwrap()
}

pub fn open_client(name: &str) -> (Client, Client) {
  let head = open_from_env("YAK_HEAD", name);
  let tail = open_from_env("YAK_TAIL", name);
  (head, tail)
}

#[bench]
fn bench_simple_write(b: &mut Bencher) {
  env_logger::init().unwrap_or(());

  let (mut head, mut tail) = open_client("bench_simple_put");
  let key = "foo";
  let val = "bar";

  b.iter(|| head.write(key.as_bytes(), val.as_bytes()).unwrap());
}

#[bench]
fn bench_simple_read(b: &mut Bencher) {
  env_logger::init().unwrap_or(());

  let (mut head, mut tail) = open_client("bench_simple_put");
  let key = "foo";
  let val = "bar";
  head.write(key.as_bytes(), val.as_bytes()).unwrap();

  b.iter(|| tail.read(key.as_bytes()).unwrap());
}
