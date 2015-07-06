use std::env;

use yak_client::Client;

pub fn open_from_env(env_var: &str, name: &str) -> Client {
  let yak_url = env::var(env_var).ok()
    .expect(&format!("env var {} not found", env_var));
  let full_url = format!("{}-{}", yak_url, name);
  Client::connect(&full_url).unwrap()
}

pub fn open_client(name: &str) -> (Client, Client) {
  let head = open_from_env("YAK_HEAD", name);
  let tail = open_from_env("YAK_TAIL", name);
  (head, tail)
}
