 #![feature(buf_stream)]

#[macro_use]
extern crate log;
extern crate url;
extern crate capnp;

pub mod yak_capnp;

use std::net::{TcpStream,ToSocketAddrs};
use std::io::{self,BufStream,BufRead,Write};
use std::fmt;
use std::error::Error;
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
      &YakError::InvalidUrl(ref u) => "Invalid URL",
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
  pub content: Vec<u8>
}

pub struct Request {
  pub space: String,
  pub operation: Operation,
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
    let msg = try!(message.get_root::<client_request::Reader>());
    match try!(msg.which()) {
      client_request::Truncate(v) => {
        let v = try!(v);
        Ok(Request {
          space: try!(v.get_space()).into(),
          operation: Operation::Truncate,
        })
      },
      client_request::Read(v) => {
        let v = try!(v);
        Ok(Request {
          space: try!(v.get_space()).into(),
          operation: Operation::Read {
            key: try!(v.get_key()).into(),
          }
        })
      },
      client_request::Write(v) => {
        let v = try!(v);
        Ok(Request {
          space: try!(v.get_space()).into(),
          operation: Operation::Write {
            key: try!(v.get_key()).into(),
            value: try!(v.get_value()).into(),
          }
        })
      },
    }
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
    let mut response = message.init_root::<client_response::Builder>();
    match self {
      &Response::Okay => response.set_ok(()),
      &Response::OkayData(ref val) => {
        let mut data = response.init_ok_data(val.len() as u32);
        for i in 0..val.len() {
          let mut datum = data.borrow().get(i as u32);
          datum.set_value(&val[i].content)
        }
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
          data.push(Datum { content: val });
        }
        Ok(Response::OkayData(data))
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

  pub fn send<M : WireMessage>(&mut self, req: M) -> Result<(), YakError> {
    let mut message = MallocMessageBuilder::new_default();
    req.encode(&mut message);
    try!(serialize_packed::write_message(&mut self.connection, &mut message));
    try!(self.connection.flush());
    Ok(())
  }

  pub fn read<M : WireMessage>(&mut self) -> Result<Option<M>,YakError> {
    let len = try!(self.connection.fill_buf()).len();
    if len == 0 {
      Ok(None)
    } else {
      let message_reader =
        try!(serialize_packed::read_message(
          &mut self.connection, ReaderOptions::new()));
      let resp = try!(M::decode(&message_reader));
      Ok(Some(resp))
    }
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
    try!(self.protocol.read::<Response>())
      .map(|r| r.expect_ok())
      .unwrap_or(Err(YakError::ProtocolError))
  }

  pub fn write(&mut self, key: &[u8], val: &[u8]) -> Result<(), YakError> {
    let req = Request::write(&self.space, key, val);
    try!(self.protocol.send(req));
    debug!("Waiting for response");
    try!(self.protocol.read::<Response>())
      .map(|r| r.expect_ok())
      .unwrap_or(Err(YakError::ProtocolError))
  }

  pub fn read(&mut self, key: &[u8]) -> Result<Vec<Datum>, YakError> {
    let req = Request::read(&self.space, key);
    try!(self.protocol.send(req));
    debug!("Waiting for response");

    try!(self.protocol.read::<Response>())
      .map(|r| r.expect_datum_list())
      .unwrap_or(Err(YakError::ProtocolError))
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
