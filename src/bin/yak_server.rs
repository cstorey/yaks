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
  let mut store = HashMap::new();
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
        client_request::Truncate(v) => truncate(&id, &mut store, v, response),
          client_request::Read(v) => read(&id, &mut store, try!(v), response),
          client_request::Write(v) => write(&id, &mut store, try!(v), response),
      };
    }

    try!(serialize_packed::write_message(&mut strm, &mut message));
    try!(strm.flush())
  }
}

fn truncate<Id: fmt::Display>(id: &Id, store: &mut HashMap<Vec<u8>, Vec<u8>>, v: (), mut response: client_response::Builder) -> Result<(), ServerError> {
  info!("{}: truncate:{:?}", id, v);
  store.clear();
  response.set_ok(());
  Ok(())
}

fn read<Id: fmt::Display>(id: &Id, store: &mut HashMap<Vec<u8>, Vec<u8>>, req: read_request::Reader, mut response: client_response::Builder) -> Result<(), ServerError> {
  let key = try!(req.get_key()).into();
  info!("{}: read:{:?}", id, key);
  match store.get(key) {
    Some(val) => {
      let mut data = response.init_ok_data(1);
      let mut datum = data.borrow().get(0);
      datum.set_value(val)
    },
    None => {
      let mut data = response.init_ok_data(0);
    }
  }
  Ok(())
}

fn write<Id: fmt::Display>(id: &Id, store: &mut HashMap<Vec<u8>, Vec<u8>>, v: write_request::Reader, mut response: client_response::Builder) -> Result<(), ServerError> {
  let key = try!(v.get_key()).into();
  let val = try!(v.get_value()).into();
  info!("{}: write:{:?} -> {:?}", id, key, val);

  store.insert(key, val);
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
