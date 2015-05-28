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
use std::sync::{Arc,Mutex};
use std::collections::HashMap;

use capnp::serialize_packed;
use capnp::{MessageBuilder, MessageReader, MallocMessageBuilder, ReaderOptions};

use yak_client::yak_capnp::*;

type Key = (String, Vec<u8>);
type Val = Vec<u8>;
type Values = Vec<Val>;
#[derive(Debug,Clone)]
struct MemStore (Arc<Mutex<HashMap<Key, Values>>>);

trait Store {
  fn truncate(&self, space: &str);
  fn read(&self, space: &str, key: &[u8]) -> Values;
  fn write(&self, space: &str, key: &[u8], val: &[u8]);
}

impl MemStore {
  fn new() -> MemStore {
    MemStore(Arc::new(Mutex::new(HashMap::new())))
  }
}

impl Store for MemStore {
  fn truncate(&self, space: &str) {
    let mut map = self.0.lock().unwrap();
    let to_rm = map.keys().filter(|k| k.0 == space).map(|k| k.clone()).collect::<Vec<_>>();
    for k in to_rm {
      map.remove(&k);
    }
  }

  fn read(&self, space: &str, key: &[u8]) -> Values {
    let map = self.0.lock().unwrap();
    let k = (space.into(), key.into());
    map.get(&k).map(|x| x.clone()).unwrap_or(vec![])
  }
  fn write(&self, space: &str, key: &[u8], val: &[u8]) {
    let mut map = self.0.lock().unwrap();
    let k = (space.into(), key.into());
    let entry = map.entry(k).or_insert(vec![]);
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
  let mut store = MemStore::new();
  for stream in listener.incoming() {
    let next = next.clone();
    let store = store.clone();
    thread::spawn(move || {
	let mut sock = stream.unwrap();
	let peer = sock.peer_addr().unwrap();
	info!("Accept stream from {:?}", peer);
        Session::new(peer, sock, store).process_requests().unwrap()
      });
  }
}

struct Session<Id, S:Write, ST> {
  id: Id,
  strm: BufStream<S>,
  store: ST
}


impl<Id: fmt::Display, S: Read + Write, ST:Store> Session<Id, S, ST> {
  fn new(id: Id, sock: S, store: ST) -> Session<Id, S, ST> {
    let mut strm = BufStream::new(sock);
    Session { id: id, strm:strm, store: store }
  }
  fn process_requests(&mut self) -> Result<(), ServerError> {
    loop {
      debug!("{}: Waiting for message", self.id);
      let len = try!(self.strm.fill_buf()).len();
      if len == 0 {
          debug!("{}: End of client stream", self.id);
          return Ok(())
      }
      let message_reader = try!(serialize_packed::read_message(&mut self.strm, ReaderOptions::new()));
      let msg = try!(message_reader.get_root::<client_request::Reader>());
      debug!("{}: Read message", self.id);

      let mut message = MallocMessageBuilder::new_default();
      {
        let mut response = message.init_root::<client_response::Builder>();
        match try!(msg.which()) {
          client_request::Truncate(v) => self.truncate(try!(v), response),
          client_request::Read(v) => self.read(try!(v), response),
          client_request::Write(v) => self.write(try!(v), response),
        };
      }

      try!(serialize_packed::write_message(&mut self.strm, &mut message));
      try!(self.strm.flush())
    }
  }

  fn truncate(&self, req: truncate_request::Reader, mut response: client_response::Builder) -> Result<(), ServerError> {
    let space = try!(req.get_space());
    info!("{}/{:?}: truncate", self.id, space);
    self.store.truncate(space);
    response.set_ok(());
    Ok(())
  }

  fn read(&self, req: read_request::Reader, mut response: client_response::Builder) -> Result<(), ServerError> {
    let space = try!(req.get_space());
    let key = try!(req.get_key()).into();
    let val = self.store.read(space, key);
    info!("{}/{:?}: read:{:?}: -> {:?}", self.id, space, key, val);

    let mut data = response.init_ok_data(val.len() as u32);
    for i in 0..val.len() {
      let mut datum = data.borrow().get(i as u32);
      datum.set_value(&val[i])
    }
    Ok(())
  }

  fn write(&self, v: write_request::Reader, mut response: client_response::Builder) -> Result<(), ServerError> {
    let space = try!(v.get_space());
    let key = try!(v.get_key()).into();
    let val = try!(v.get_value()).into();
    info!("{}/{:?}: write:{:?} -> {:?}", self.id, space, key, val);

    self.store.write(space, key, val);
    response.set_ok(());
    Ok(())
  }
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
