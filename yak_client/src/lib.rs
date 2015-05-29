 #![feature(buf_stream)]

#[macro_use]
extern crate log;
extern crate url;
extern crate capnp;

pub mod yak_capnp;

use std::net::{TcpStream,ToSocketAddrs};
use std::io::{self,BufStream,Write};
use capnp::serialize_packed;
use capnp::{MessageBuilder, MessageReader, MallocMessageBuilder, ReaderOptions};

use url::{Url,SchemeType,UrlParser};

use yak_capnp::{client_request,client_response};

fn yak_url_scheme(_scheme: &str) -> SchemeType {
  SchemeType::Relative(0)
}

#[derive(Debug)]
pub enum YakError {
  UrlParseError(url::ParseError),
  InvalidUrl(Url),
  IoError(io::Error),
  CapnpError(capnp::Error),
  CapnpNotInSchema(capnp::NotInSchema),
  ProtocolError
}

#[derive(PartialEq,Eq,PartialOrd,Ord,Debug, Clone)]
pub struct Datum {
  pub content: Vec<u8>
}

pub struct Request {
  space: String,
  operation: Operation,
}
pub enum Operation {
  Truncate ,
  Read { key: Vec<u8> },
  Write { key: Vec<u8>, value: Vec<u8> },
}

impl Request {
  fn truncate(space: &str) -> Request {
    Request { space: space.to_string(), operation: Operation::Truncate }
  }

  fn read(space: &str, key: &[u8]) -> Request {
    Request { space: space.to_string(), operation: Operation::Read { key: key.to_vec() } }
  }

  fn write(space: &str, key: &[u8], value: &[u8]) -> Request {
    Request { space: space.to_string(), operation: Operation::Write { key: key.to_owned(), value: value.to_owned() } }
  }

  fn encode_truncate<B: MessageBuilder>(message: &mut B, space: &str) {
    let mut rec = message.init_root::<client_request::Builder>();
    let mut trunc = rec.init_truncate();
    trunc.set_space(space)
  }

  fn encode_write<B: MessageBuilder>(message: &mut B, space: &str, key: &[u8], val: &[u8]) {
    let mut rec = message.init_root::<client_request::Builder>();
    let mut req = rec.init_write();
    req.set_space(space);
    req.set_key(key);
    req.set_value(val);
  }

  fn encode_read<B: MessageBuilder>(message: &mut B, space: &str, key: &[u8]) {
      let mut rec = message.init_root::<client_request::Builder>();
      let mut req = rec.init_read();
      req.set_space(space);
      req.set_key(key)
  }
}

impl WireMessage for Request {
  fn encode<B: MessageBuilder>(&self, message: &mut B) {
    match &self.operation {
      &Operation::Truncate => Self::encode_truncate(message, &self.space),
      &Operation::Read { key: ref key } => Self::encode_read(message, &self.space, &key),
      &Operation::Write { key: ref key, value: ref val } => Self::encode_write(message, &self.space, &key, &val),
    }
  }

  fn decode<R: MessageReader>(message: &R) -> Result<Self, YakError> {
    unimplemented!()
  }
}

pub enum Response {
  Okay,
  OkayData(Vec<Datum>)
}

impl Response {
  pub fn expect_ok(&self) -> Result<(), YakError> {
    match self {
      &Response::Okay => Ok(()),
      &Response::OkayData(_) => Err(YakError::ProtocolError)
    }
  }

  pub fn expect_datum_list(&self) -> Result<Vec<Datum>, YakError> {
    match self {
      &Response::OkayData(ref result) => Ok(result.clone()),
      &Response::Okay => Err(YakError::ProtocolError)
    }
  }
}

impl WireMessage for Response {
  fn encode<B: MessageBuilder>(&self, message: &mut B) {
    unimplemented!()
  }

  fn decode<R: MessageReader>(message: &R) -> Result<Self, YakError> {
    let msg = try!(message.get_root::<client_response::Reader>());
    match try!(msg.which()) {
      client_response::Ok(()) => Ok(Response::Okay),
      client_response::OkData(d) => {
        debug!("Got response Data: ");
        let mut data = Vec::with_capacity(try!(d).len() as usize);
        for it in try!(d).iter() {
          let val : Vec<u8> = try!(it.get_value()).iter().map(|v|v.clone()).collect();
          data.push(Datum { content: val });
        }
        Ok(Response::OkayData(data))
      }
    }
  }
}
#[derive(Debug)]
struct WireProtocol<S: io::Read+io::Write> {
  connection: BufStream<S>,
}

trait WireMessage {
  fn encode<B: MessageBuilder>(&self, message: &mut B);
  fn decode<R: MessageReader>(message: &R) -> Result<Self, YakError>;
}

impl WireProtocol<TcpStream> {
  fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self, YakError> {
    let sock = try!(TcpStream::connect(addr));
    debug!("connected:{:?}", sock);
    let stream = BufStream::new(sock);
    Ok(WireProtocol { connection: stream })
  }

  fn send<M : WireMessage>(&mut self, req: M) -> Result<(), YakError> {
    let mut message = MallocMessageBuilder::new_default();
    req.encode(&mut message);
    try!(serialize_packed::write_message(&mut self.connection, &mut message));
    try!(self.connection.flush());
    Ok(())
  }

  fn read<M : WireMessage>(&mut self) -> Result<M,YakError> {
    let message_reader =
      try!(serialize_packed::read_message(
	&mut self.connection, ReaderOptions::new()));
    let resp = try!(M::decode(&message_reader));
    Ok(resp)
  }
}

#[derive(Debug)]
pub struct Client {
  protocol: WireProtocol<TcpStream>,
  space: String,
}

impl Client {
  pub fn connect(loc: &str) -> Result<Client, YakError> {
    let mut p = UrlParser::new();
    p.scheme_type_mapper(yak_url_scheme);
    let url = try!(p.parse(loc));
    debug!("yak:url: {:?}", url);
    let (addr, space) = match (url.domain(), url.port(), url.serialize_path()) {
      (Some(host), Some(port), Some(path)) => ((host, port), path),
      _ => return Err(YakError::InvalidUrl(url.clone()))
    };

    let proto = try!(WireProtocol::connect(addr));
    Ok(Client { protocol: proto, space: space })
  }

  fn expect_ok<R: MessageReader>(message: &R) -> Result<(), YakError> {
    let msg = try!(message.get_root::<client_response::Reader>());
    debug!("Got response");
    match try!(msg.which()) {
      client_response::Ok(()) => Ok(()),
      other => Err(YakError::ProtocolError)
    }
  }

  fn expect_datum_list<R: MessageReader>(message_reader: &R) -> Result<Vec<Datum>, YakError> {
    let msg = try!(message_reader.get_root::<client_response::Reader>());
    match try!(msg.which()) {
      client_response::OkData(d) => {
        debug!("Got response Data: ");
        let mut data = Vec::with_capacity(try!(d).len() as usize);
        for it in try!(d).iter() {
          let val : Vec<u8> = try!(it.get_value()).iter().map(|v|v.clone()).collect();
          data.push(Datum { content: val });
        }

        Ok(data)
      },
      other => Err(YakError::ProtocolError)
    }
  }

  pub fn truncate(&mut self) -> Result<(), YakError> {
    let req = Request::truncate(&self.space);
    try!(self.protocol.send(req));
    debug!("Waiting for response");
    try!(self.protocol.read::<Response>()).expect_ok()
  }

  pub fn write(&mut self, key: &[u8], val: &[u8]) -> Result<(), YakError> {
    let req = Request::write(&self.space, key, val);
    try!(self.protocol.send(req));
    debug!("Waiting for response");
    try!(self.protocol.read::<Response>()).expect_ok()
  }

  pub fn read(&mut self, key: &[u8]) -> Result<Vec<Datum>, YakError> {
    let req = Request::read(&self.space, key);
    try!(self.protocol.send(req));
    debug!("Waiting for response");

    try!(self.protocol.read::<Response>()).expect_datum_list()
  }
}

impl From<url::ParseError> for YakError {
  fn from(err: url::ParseError) -> YakError {
    YakError::UrlParseError(err)
  }
}

impl From<io::Error> for YakError {
  fn from(err: io::Error) -> YakError {
    YakError::IoError(err)
  }
}

impl From<capnp::Error> for YakError {
  fn from(err: capnp::Error) -> YakError {
    YakError::CapnpError(err)
  }
}

impl From<capnp::NotInSchema> for YakError {
  fn from(err: capnp::NotInSchema) -> YakError {
    YakError::CapnpNotInSchema(err)
  }
}
