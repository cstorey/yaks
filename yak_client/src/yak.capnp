@0x8823926807b755b2;

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

struct Operation {
  union {
    truncate @0 : Void;
    read @1 : ReadRequest;
    write @2 : WriteRequest;
  }
}

struct ClientRequest {
  space @0: Text;
  operation@1: Operation;
}


struct ClientResponse {
  union {
    ok @0 : Void;
    okData @1 : List(Datum);
  }
}
