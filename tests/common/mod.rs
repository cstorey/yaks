use std::env;
extern crate rand;
use self::rand::Rng;

use yak_client::Client;

pub fn open_from_env(env_var: &str, name: &str, test_id: u64) -> Client {
  let yak_url = env::var(env_var).ok()
    .expect(&format!("env var {} not found", env_var));
  let full_url = format!("{}-{}-{:x}", yak_url, name, test_id);
  info!("Connecting to:{:?}", full_url);
  Client::connect(&full_url).unwrap()
}

pub fn open_client(name: &str) -> (Client, Client) {
  let test_id = rand::thread_rng().next_u64();
  let head = open_from_env("YAK_HEAD", name, test_id);
  let tail = open_from_env("YAK_TAIL", name, test_id);
  (head, tail)
}
