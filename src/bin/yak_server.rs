#![feature(convert)]
#![feature(buf_stream)]
#[macro_use] extern crate log;
extern crate env_logger;
extern crate yak_client;
extern crate capnp;

use std::io::{Read,Write,BufStream};
use std::net::{TcpListener, TcpStream, Ipv4Addr};
use std::thread;
use std::io::{self,BufRead};
use std::str::FromStr;
use std::fmt;

use capnp::serialize_packed;
use capnp::{MessageBuilder, MessageReader, MallocMessageBuilder, ReaderOptions};

use yak_client::yak_messages::*;

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
  info!("listening started, ready to accept");
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
  loop {
    debug!("{}: Waiting for message", id);
    let message_reader = try!(serialize_packed::read_message(&mut strm, ReaderOptions::new()));
    let msg = try!(message_reader.get_root::<client_request::Reader>());
    debug!("{}: Read message", id);

    match try!(msg.which()) {
      client_request::Truncate(v) => info!("{}: truncate:{:?}", id, v),
      client_request::Read(v) => info!("{}: read:{:?}", id, v),
    };

    let mut message = MallocMessageBuilder::new_default();
    {
      let mut rec = message.init_root::<client_response::Builder>();
    }

    try!(serialize_packed::write_message(&mut strm, &mut message));
    try!(strm.flush())
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
