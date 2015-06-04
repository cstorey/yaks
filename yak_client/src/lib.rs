 #![feature(buf_stream)]

#[macro_use]
extern crate log;
extern crate url;
extern crate capnp;

mod yak_capnp;

use std::net::{TcpStream,ToSocketAddrs};
use std::io::{self,BufStream,BufRead,Write};
use std::fmt;
use std::error::Error;
use capnp::serialize_packed;
use capnp::{MessageBuilder, MessageReader, MallocMessageBuilder, ReaderOptions};

use url::{Url,SchemeType,UrlParser};

use yak_capnp::*;

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

impl fmt::Display for YakError {
  fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
    match self {
      &YakError::UrlParseError(ref e) => e.fmt(f),
      &YakError::InvalidUrl(ref u) => f.write_fmt(format_args!("Invalid URL: {}", u)),
      &YakError::IoError(ref e) => e.fmt(f),
      &YakError::CapnpError(ref e) => e.fmt(f),
      &YakError::CapnpNotInSchema(ref e) => e.fmt(f),
      &YakError::ProtocolError => "Protocol Error".fmt(f)
    }
  }
}

impl Error for YakError {
  fn description(&self) -> &str {
    match self {
      &YakError::InvalidUrl(_) => "Invalid URL",
      &YakError::ProtocolError => "Protocol Error",
      &YakError::IoError(ref e) => e.description(),
      &YakError::CapnpError(ref e) => e.description(),
      &YakError::CapnpNotInSchema(ref e) => e.description(),
      &YakError::UrlParseError(ref e) => e.description()
    }
  }
}

#[derive(PartialEq,Eq,PartialOrd,Ord,Debug, Clone)]
pub struct Datum {
  pub key: Vec<u8>,
  pub content: Vec<u8>
}

#[derive(Debug)]
pub struct Request {
  pub space: String,
  pub operation: Operation,
}

#[derive(Debug)]
pub enum Operation {
  Truncate ,
  Read { key: Vec<u8> },
  Write { key: Vec<u8>, value: Vec<u8> },
  Subscribe,
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

  fn subscribe(space: &str) -> Request {
    Request { space: space.to_string(), operation: Operation::Subscribe }
  }

  fn encode_truncate<B: MessageBuilder>(message: &mut B, space: &str) {
    let mut rec = message.init_root::<client_request::Builder>();
    rec.set_space(space);
    let mut op = rec.init_operation();
    op.set_truncate(())
  }

  fn encode_write<B: MessageBuilder>(message: &mut B, space: &str, key: &[u8], val: &[u8]) {
    let mut rec = message.init_root::<client_request::Builder>();
    rec.set_space(space);
    let mut req = rec.init_operation().init_write();
    req.set_key(key);
    req.set_value(val);
  }

  fn encode_read<B: MessageBuilder>(message: &mut B, space: &str, key: &[u8]) {
    let mut rec = message.init_root::<client_request::Builder>();
    rec.set_space(space);
    let mut req = rec.init_operation().init_read();
    req.set_key(key)
  }

  fn encode_subscribe<B: MessageBuilder>(message: &mut B, space: &str) {
    let mut rec = message.init_root::<client_request::Builder>();
    rec.set_space(space);
    rec.init_operation().set_subscribe(())
  }
}

impl WireMessage for Request {
  fn encode<B: MessageBuilder>(&self, message: &mut B) {
    match &self.operation {
      &Operation::Truncate => Self::encode_truncate(message, &self.space),
      &Operation::Read { ref key } => Self::encode_read(message, &self.space, &key),
      &Operation::Write { ref key, ref value } => Self::encode_write(message, &self.space, &key, &value),
      &Operation::Subscribe => Self::encode_subscribe(message, &self.space),
    }
  }

  fn decode<R: MessageReader>(message: &R) -> Result<Self, YakError> {
    let msg = try!(message.get_root::<client_request::Reader>());
    let space = try!(msg.get_space()).into();
    let op = try!(msg.get_operation());
    match try!(op.which()) {
      operation::Truncate(()) => {
        Ok(Request {
          space: space,
          operation: Operation::Truncate,
        })
      },
      operation::Read(v) => {
        let v = try!(v);
        Ok(Request {
          space: space,
          operation: Operation::Read {
            key: try!(v.get_key()).into(),
          }
        })
      },
      operation::Write(v) => {
        let v = try!(v);
        Ok(Request {
          space: space,
          operation: Operation::Write {
            key: try!(v.get_key()).into(),
            value: try!(v.get_value()).into(),
          }
        })
      },
      operation::Subscribe(()) => {
        Ok(Request {
          space: space,
          operation: Operation::Subscribe,
        })
      },
    }
  }
}

#[derive(Debug)]
pub enum Response {
  Okay,
  OkayData(Vec<Datum>),
  Delivery(Datum),
}

impl Response {
  pub fn expect_ok(&self) -> Result<(), YakError> {
    match self {
      &Response::Okay => Ok(()),
      &_ => Err(YakError::ProtocolError)
    }
  }

  pub fn expect_datum_list(&self) -> Result<Vec<Datum>, YakError> {
    match self {
      &Response::OkayData(ref result) => Ok(result.clone()),
      &_ => Err(YakError::ProtocolError)
    }
  }

  pub fn expect_delivery(&self) -> Result<Datum, YakError> {
    match self {
      &Response::Delivery(ref result) => Ok(result.clone()),
      &_ => Err(YakError::ProtocolError)
    }
  }
}

impl WireMessage for Response {
  fn encode<B: MessageBuilder>(&self, message: &mut B) {
    let mut response = message.init_root::<client_response::Builder>();
    match self {
      &Response::Okay => response.set_ok(()),
      &Response::OkayData(ref val) => {
        let mut data = response.init_ok_data(val.len() as u32);
        for i in 0..val.len() {
          let mut datum = data.borrow().get(i as u32);
          datum.set_value(&val[i].content)
        }
      },
      &Response::Delivery(ref val) => {
        let mut datum = response.init_delivery();
        datum.set_value(&val.content)
      }
    }
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
          data.push(Datum { key: vec![], content: val });
        }
        Ok(Response::OkayData(data))
      },
      client_response::Delivery(d) => {
        debug!("Got Delivery: ");
        let d = try!(d);
        let val = try!(d.get_value()).into();
        let datum = Datum { key: vec![], content: val };
        Ok(Response::Delivery(datum))
      }
    }
  }
}
#[derive(Debug)]
pub struct WireProtocol<S: io::Read+io::Write> {
  connection: BufStream<S>,
}

pub trait WireMessage {
  fn encode<B: MessageBuilder>(&self, message: &mut B);
  fn decode<R: MessageReader>(message: &R) -> Result<Self, YakError>;
}

impl WireProtocol<TcpStream> {
  pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self, YakError> {
    let sock = try!(TcpStream::connect(addr));
    debug!("connected:{:?}", sock);
    Ok(WireProtocol::new(sock))
  }
}

impl<S: io::Read+io::Write> WireProtocol<S> {
  pub fn new(conn: S) -> WireProtocol<S> {
    let stream = BufStream::new(conn);
    WireProtocol { connection: stream }
  }

  pub fn send<M : WireMessage + fmt::Debug>(&mut self, req: &M) -> Result<(), YakError> {
    trace!("Send: {:?}", req);
    let mut message = MallocMessageBuilder::new_default();
    req.encode(&mut message);
    try!(serialize_packed::write_message(&mut self.connection, &mut message));
    try!(self.connection.flush());
    Ok(())
  }

  pub fn read<M : WireMessage + fmt::Debug>(&mut self) -> Result<Option<M>,YakError> {
    let len = try!(self.connection.fill_buf()).len();
    if len == 0 {
      trace!("EOF");
      Ok(None)
    } else {
      trace!("Reading");
      let message_reader =
        try!(serialize_packed::read_message(
          &mut self.connection, ReaderOptions::new()));
      let resp = try!(M::decode(&message_reader));
      trace!("Read: {:?}", resp);
      Ok(Some(resp))
    }
  }
}

#[derive(Debug)]
pub struct Client {
  protocol: WireProtocol<TcpStream>,
  space: String,
}

pub struct Subscription {
  protocol: WireProtocol<TcpStream>,
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

  pub fn truncate(&mut self) -> Result<(), YakError> {
    let req = Request::truncate(&self.space);
    try!(self.protocol.send(&req));
    debug!("Waiting for response");
    try!(self.protocol.read::<Response>())
      .map(|r| r.expect_ok())
      .unwrap_or(Err(YakError::ProtocolError))
  }

  pub fn write(&mut self, key: &[u8], val: &[u8]) -> Result<(), YakError> {
    let req = Request::write(&self.space, key, val);
    try!(self.protocol.send(&req));
    debug!("Waiting for response");
    try!(self.protocol.read::<Response>())
      .map(|r| r.expect_ok())
      .unwrap_or(Err(YakError::ProtocolError))
  }

  pub fn read(&mut self, key: &[u8]) -> Result<Vec<Datum>, YakError> {
    let req = Request::read(&self.space, key);
    try!(self.protocol.send(&req));
    debug!("Waiting for response");

    try!(self.protocol.read::<Response>())
      .map(|r| r.expect_datum_list())
      .unwrap_or(Err(YakError::ProtocolError))
  }

  pub fn subscribe(mut self) -> Result<Subscription, YakError> {
    let req = Request::subscribe(&self.space);
    try!(self.protocol.send(&req));
    debug!("Waiting for response");

    let resp = try!(self.protocol.read::<Response>());
    let _ : () = try!(resp.map(|r| r.expect_ok()).unwrap_or(Err(YakError::ProtocolError)));
    Ok(Subscription { protocol: self.protocol })
  }
}

impl Subscription {
  pub fn fetch_next(&mut self) -> Result<Option<Datum>, YakError> {
    debug!("Waiting for next delivery");
    let next = try!(self.protocol.read::<Response>());
    let next = try!(next.map(Ok).unwrap_or(Err(YakError::ProtocolError)));
    match next {
      Response::Okay => Ok(None),
      Response::Delivery(d) => Ok(Some(d)),
      _ => Err(YakError::ProtocolError),
    }
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
