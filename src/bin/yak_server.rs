#![feature(convert)]
#![feature(buf_stream)]
#[macro_use] extern crate log;
extern crate env_logger;
extern crate yak_client;
extern crate capnp;

use std::net::{TcpListener, TcpStream, Ipv4Addr};
use std::thread;
use std::io::{self,Read,Write,BufStream,BufRead};
use std::str::FromStr;
use std::fmt;
use std::collections::HashMap;

use capnp::serialize_packed;
use capnp::{MessageBuilder, MessageReader, MallocMessageBuilder, ReaderOptions};

use yak_client::yak_capnp::*;

type Key = Vec<u8>;
type Values = Vec<Vec<u8>>;
struct MemStore (HashMap<Key, Values>);

impl MemStore {
  fn new() -> MemStore {
    MemStore(HashMap::new())
  }

  fn truncate(&mut self) {
    let MemStore(ref mut map) = *self;
    map.clear();
  }

  fn read(&self, key: &[u8]) -> Values {
    let MemStore(ref map) = *self;
    map.get(key).map(|x| x.clone()).unwrap_or(vec![])
  }
  fn write(&mut self, key: &[u8], val: &[u8]) {
    let MemStore(ref mut map) = *self;
    let entry = map.entry(key.into()).or_insert(vec![]);
    entry.push(val.into())
  }
}

#[derive(Debug)]
enum ServerError {
  CapnpError(capnp::Error),
  CapnpNotInSchema(capnp::NotInSchema),
  IoError(std::io::Error)
}

pub fn main() {
  env_logger::init().unwrap();

  let mut a = std::env::args().skip(1);
  let local : String = a.next().unwrap();
  let next : Option<String> = a.next();

  let listener = TcpListener::bind(local.as_str()).unwrap();
  info!("listening started on {}, ready to accept", local);
  for stream in listener.incoming() {
    let next = next.clone();
    thread::spawn(move || {
	let mut sock = &stream.unwrap();
	let peer = sock.peer_addr().unwrap();
	info!("Accept stream from {:?}", peer);
        let mut strm = BufStream::new(sock);
        process_requests(peer, strm).unwrap()
      });
  }
}

fn process_requests<Id: fmt::Display, S: Read + Write>(id: Id, mut strm: BufStream<S>) -> Result<(), ServerError> {
  let mut store = MemStore::new();
  loop {
    debug!("{}: Waiting for message", id);
    let len = try!(strm.fill_buf()).len();
    if len == 0 {
        debug!("{}: End of client stream", id);
        return Ok(())
    }
    let message_reader = try!(serialize_packed::read_message(&mut strm, ReaderOptions::new()));
    let msg = try!(message_reader.get_root::<client_request::Reader>());
    debug!("{}: Read message", id);

    let mut message = MallocMessageBuilder::new_default();
    {
      let mut response = message.init_root::<client_response::Builder>();
      match try!(msg.which()) {
        client_request::Truncate(v) => truncate(&id, &mut store, try!(v), response),
          client_request::Read(v) => read(&id, &mut store, try!(v), response),
          client_request::Write(v) => write(&id, &mut store, try!(v), response),
      };
    }

    try!(serialize_packed::write_message(&mut strm, &mut message));
    try!(strm.flush())
  }
}

fn truncate<Id: fmt::Display>(id: &Id, store: &mut MemStore, req: truncate_request::Reader, mut response: client_response::Builder) -> Result<(), ServerError> {
  let space = try!(req.get_space());
  info!("{}/{:?}: truncate", id, space);
  store.truncate();
  response.set_ok(());
  Ok(())
}

fn read<Id: fmt::Display>(id: &Id, store: &mut MemStore, req: read_request::Reader, mut response: client_response::Builder) -> Result<(), ServerError> {
  let space = try!(req.get_space());
  let key = try!(req.get_key()).into();
  let val = store.read(key);
  info!("{}/{:?}: read:{:?}: -> {:?}", id, space, key, val);

  let mut data = response.init_ok_data(val.len() as u32);
  for i in 0..val.len() {
    let mut datum = data.borrow().get(i as u32);
    datum.set_value(&val[i])
  }
  Ok(())
}

fn write<Id: fmt::Display>(id: &Id, store: &mut MemStore, v: write_request::Reader, mut response: client_response::Builder) -> Result<(), ServerError> {
  let space = try!(v.get_space());
  let key = try!(v.get_key()).into();
  let val = try!(v.get_value()).into();
  info!("{}/{:?}: write:{:?} -> {:?}", id, space, key, val);

  store.write(key, val);
  response.set_ok(());
  Ok(())
}

impl From<capnp::Error> for ServerError {
  fn from(err: capnp::Error) -> ServerError {
    ServerError::CapnpError(err)
  }
}

impl From<capnp::NotInSchema> for ServerError {
  fn from(err: capnp::NotInSchema) -> ServerError {
    ServerError::CapnpNotInSchema(err)
  }
}

impl From<io::Error> for ServerError {
  fn from(err: io::Error) -> ServerError {
    ServerError::IoError(err)
  }
}
