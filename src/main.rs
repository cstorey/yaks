#![feature(plugin)]
#![feature(scoped)]
#![plugin(quickcheck_macros)]
#![feature(convert)]
#[macro_use] extern crate log;
extern crate env_logger;
extern crate yak_client;
extern crate capnp;
extern crate log4rs;
#[cfg(test)]
extern crate quickcheck;

use std::default::Default;
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::io::{self,Read,Write};
use std::fmt;
use std::sync::{Arc,Mutex};
use std::collections::HashMap;
use std::clone::Clone;
use std::error::Error;

use yak_client::{WireProtocol,Request,Response,Operation,Datum,YakError};

#[macro_use] mod store;
mod mem_store;

#[derive(Debug)]
enum ServerError {
  CapnpError(capnp::Error),
  CapnpNotInSchema(capnp::NotInSchema),
  IoError(std::io::Error),
  DownstreamError(YakError)
}

impl fmt::Display for ServerError {
  fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
    match self {
      &ServerError::CapnpError(ref e) => e.fmt(f),
      &ServerError::CapnpNotInSchema(ref e) => e.fmt(f),
      &ServerError::IoError(ref e) => e.fmt(f),
      &ServerError::DownstreamError(ref e) => e.fmt(f)
    }
  }
}

impl Error for ServerError {
  fn description(&self) -> &str {
    match self {
      &ServerError::CapnpError(ref e) => e.description(),
      &ServerError::CapnpNotInSchema(ref e) => e.description(),
      &ServerError::IoError(ref e) => e.description(),
      &ServerError::DownstreamError(ref e) => e.description()
    }
  }
}

struct DownStream<S: Read+Write> {
  protocol: Arc<Mutex<WireProtocol<S>>>,
}

impl <S: ::std::fmt::Debug + Read + Write> ::std::fmt::Debug for DownStream<S>
 where S: ::std::fmt::Debug + 'static {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match self {
            &DownStream { protocol: ref proto } =>
            fmt.debug_struct("DownStream").field("protocol",
                                                     (proto)).finish(),
        }
    }
}

impl<S> Clone for DownStream<S> where S: Read+Write {
  fn clone(&self) -> DownStream<S> {
    DownStream { protocol: self.protocol.clone() }
  }
}

static LOG_FILE: &'static str = "log.toml";

pub fn main() {
  if let Err(e) = log4rs::init_file(LOG_FILE, Default::default()) {
    panic!("Could not init logger from file {}: {}", LOG_FILE, e);
  }

  match do_run() {
    Ok(()) => info!("Terminated normally"),
    Err(e) => panic!("Failed: {}", e),
  }
}

fn do_run() -> Result<(), ServerError> {
  let mut a = std::env::args().skip(1);
  let local : String = a.next().unwrap();
  let next = match a.next() {
      Some(ref addr) => Some(try!(DownStream::new(addr))),
      None => None
  };

  let listener = TcpListener::bind(local.as_str()).unwrap();
  info!("listening started on {}, ready to accept", local);
  let store = mem_store::MemStore::new();
  for stream in listener.incoming() {
    let next = next.clone();
    let store = store.clone();
    let sock = stream.unwrap();
    let peer = sock.peer_addr().unwrap();
    let _ = try!(thread::Builder::new().name(format!("client-{}", peer)).spawn(move || {
	debug!("Accept stream from {:?}", peer);
        match Session::new(peer, sock, store, next).process_requests() {
          Err(e) => panic!("Processing requests failed: {}", e),
          _ => ()
        }
      }));
  }
  Ok(())
}

impl DownStream<TcpStream> {
  fn new(addr: &str) -> Result<DownStream<TcpStream>, ServerError> {
    debug!("Connect downstream: {:?}", addr);
    let proto = try!(WireProtocol::connect(addr));
    debug!("Connected downstream: {:?}", proto);

    Ok(DownStream { protocol: Arc::new(Mutex::new(proto)) })
  }

}
impl<S: Read+Write> DownStream<S> {
  fn handle(&self, msg: &Request) -> Result<Response, ServerError> {
    let mut wire = self.protocol.lock().unwrap();
    trace!("Downstream: -> {:?}", msg);
    try!(wire.send(msg));
    let resp = try!(wire.read::<Response>());
    trace!("Downstream: <- {:?}", resp);
    resp.map(Ok).unwrap_or(Err(ServerError::DownstreamError(YakError::ProtocolError)))
  }
}

struct Session<Id, S:Read+Write+'static, ST> {
  id: Id,
  protocol: WireProtocol<S>,
  store: ST,
  next: Option<DownStream<S>>
}


impl<Id: fmt::Display, S: Read+Write, ST:store::Store> Session<Id, S, ST> {
  fn new(id: Id, conn: S, store: ST, next: Option<DownStream<S>>) -> Session<Id, S, ST> {
    Session {
    	id: id,
	protocol: WireProtocol::new(conn),
	store: store,
	next: next
    }
  }

  fn process_requests(&mut self) -> Result<(), ServerError> {
    trace!("{}: Waiting for message", self.id);
    while let Some(msg) = try!(self.protocol.read::<Request>()) {
      try!(self.process_one(msg));
    }

    Ok(())
  }

  fn process_one(&mut self, msg: Request) -> Result<(), ServerError> {
    trace!("{}: Handle message: {:?}", self.id, msg);

    let resp = match msg.operation {
      Operation::Truncate => {
        let resp = try!(self.truncate(&msg.space));
        try!(self.send_downstream_or(&msg, resp))
      },
        Operation::Write { ref key, ref value } => {
          let resp = try!(self.write(&msg.space, &key, &value));
          try!(self.send_downstream_or(&msg, resp))
        },
        Operation::Read { key } =>
          try!(self.read(&msg.space, &key)),
      Operation::Subscribe => {
        try!(self.subscribe(&msg.space));
        Response::Okay
      },
    };

    trace!("Response: {:?}", resp);

    try!(self.protocol.send(&resp));
    Ok(())
  }

  fn send_downstream_or(&self, msg: &Request, default: Response) -> Result<Response, ServerError> {
    match self.next {
      Some(ref d) => d.handle(msg),
      None => Ok(default),
    }
  }

  fn truncate(&self, space: &str) -> Result<Response, ServerError> {
    trace!("{}/{:?}: truncate", self.id, space);
    self.store.truncate(space);
    Ok(Response::Okay)
  }

  fn read(&self, space: &str, key: &[u8]) -> Result<Response, ServerError> {
    let val = self.store.read(space, key);
    trace!("{}/{:?}: read:{:?}: -> {:?}", self.id, space, key, val);
    let data = val.iter().map(|c| Datum { key: Vec::new(), content: c.clone() }).collect();
    Ok(Response::OkayData(data))
  }

  fn write(&self, space: &str, key: &[u8], val: &[u8]) -> Result<Response, ServerError> {
    trace!("{}/{:?}: write:{:?} -> {:?}", self.id, space, key, val);
    self.store.write(space, key, val);
    Ok(Response::Okay)
  }
  fn subscribe(&mut self, space: &str) -> Result<(), ServerError> {
    try!(self.protocol.send(&Response::Okay));
    for d in self.store.subscribe(space) {
      try!(self.protocol.send(&Response::Delivery(d)));
    }
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

impl From<YakError> for ServerError {
  fn from(err: YakError) -> ServerError {
    ServerError::DownstreamError(err)
  }
}
