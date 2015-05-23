 #![feature(buf_stream)]

#[macro_use]
extern crate log;
extern crate url;
extern crate capnp;

pub mod yak_capnp;

use std::net::TcpStream;
use std::io::{self,BufStream,Write};
use capnp::serialize_packed;
use capnp::{MessageBuilder, MessageReader, MallocMessageBuilder, ReaderOptions};

use url::{Url,SchemeType,UrlParser};

use yak_capnp::{client_request,client_response};

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
  ProtocolError
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
      let mut req = rec.init_write();
      req.set_key(key);
      req.set_value(val);
    }
    serialize_packed::write_message(&mut self.connection, &mut message).unwrap();
    try!(self.connection.flush());
    debug!("Waiting for response");

    let message_reader = try!(serialize_packed::read_message(&mut self.connection, ReaderOptions::new()));
    let msg = try!(message_reader.get_root::<client_response::Reader>());
    debug!("Got response");

    Ok(())
  }

  pub fn read(&mut self, key: &[u8]) -> Result<Vec<Datum>, YakError> {
    let mut message = MallocMessageBuilder::new_default();
    {
      let mut rec = message.init_root::<client_request::Builder>();
      let mut req = rec.init_read();
      req.set_key(key)
    }
    serialize_packed::write_message(&mut self.connection, &mut message).unwrap();
    try!(self.connection.flush());
    debug!("Waiting for response");

    let message_reader = try!(serialize_packed::read_message(&mut self.connection, ReaderOptions::new()));
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
