 #![feature(buf_stream)]

#[macro_use]
extern crate log;
extern crate url;
extern crate capnp;

pub mod yak_messages;

use std::net::TcpStream;
use std::io::{self,BufStream,Write};
use capnp::serialize_packed;
use capnp::{MessageBuilder, MessageReader, MallocMessageBuilder, ReaderOptions};

use url::{Url,SchemeType,UrlParser};

use yak_messages::{client_request,client_response};

fn yak_url_scheme(_scheme: &str) -> SchemeType {
  SchemeType::Relative(0)
}

pub struct Client {
  connection: BufStream<TcpStream>
}

#[derive(Debug)]
pub enum YakError {
  UrlParseError(url::ParseError),
  InvalidUrl(Url),
  IoError(io::Error),
  CapnpError(capnp::Error),
  CapnpNotInSchema(capnp::NotInSchema),
}

#[derive(PartialEq,Eq,PartialOrd,Ord,Debug)]
pub struct Datum {
  pub content: Vec<u8>
}

impl Client {
  pub fn connect(loc: &str) -> Result<Client, YakError> {
    let mut p = UrlParser::new();
    p.scheme_type_mapper(yak_url_scheme);
    let url = try!(p.parse(loc));
    debug!("yak:url: {:?}", url);
    let addr = match (url.domain(), url.port()) {
      (Some(host), Some(port)) => (host, port),
      _ => return Err(YakError::InvalidUrl(url.clone()))
    };

    let sock = try!(TcpStream::connect(addr));
    debug!("connected:{:?}", sock);
    Ok(Client { connection: BufStream::new(sock) })
  }

  pub fn truncate(&mut self) -> Result<(), YakError> {
    let mut message = MallocMessageBuilder::new_default();
    {
      let mut rec = message.init_root::<client_request::Builder>();
      rec.set_truncate(())
    }
    serialize_packed::write_message(&mut self.connection, &mut message).unwrap();
    try!(self.connection.flush());
    debug!("Waiting for response");
    
    let message_reader = try!(serialize_packed::read_message(&mut self.connection, ReaderOptions::new()));
    let msg = try!(message_reader.get_root::<client_response::Reader>());
    debug!("Got response");

    Ok(())
  }

  pub fn write(&mut self, key: &[u8], val: &[u8]) -> Result<(), YakError> {
    let mut message = MallocMessageBuilder::new_default();
    {
      let mut rec = message.init_root::<client_request::Builder>();
      rec.set_read(())
    }
    serialize_packed::write_message(&mut self.connection, &mut message).unwrap();
    try!(self.connection.flush());
    debug!("Waiting for response");
    
    let message_reader = try!(serialize_packed::read_message(&mut self.connection, ReaderOptions::new()));
    let msg = try!(message_reader.get_root::<client_response::Reader>());
    debug!("Got response");

    Ok(())
  }

  pub fn read(&self, key: &[u8]) -> Result<Vec<Datum>, YakError> {
    Ok(vec![])
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
