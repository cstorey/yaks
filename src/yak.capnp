@0xaef6714f4106a85d;

struct Datum {
  value @0: Data;
}

struct ReadRequest {
  space @0: Text;
  key @1: Data;
}

struct WriteRequest {
  space @0: Text;
  key @1: Data;
  value @2: Data;
}

struct TruncateRequest {
  space @0: Text;
}

struct ClientRequest {
  union {
    truncate @0 : TruncateRequest;
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
