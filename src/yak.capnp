@0xaef6714f4106a85d;

struct Datum {
  value @0: Data;

}

struct ReadRequest {
  key @0: Data;
}

struct WriteRequest {
  key @0: Data;
  value @1: Data;
}

struct ClientRequest {
  union {
    truncate @0 : Void;
    read @1 : ReadRequest;
    write @2 : WriteRequest;
  }
}


struct ClientResponse {
  union {
    ok @0 : Void;
    okData @1 : List(Datum);
  }
}
