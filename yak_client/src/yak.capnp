@0x8823926807b755b2;

struct Datum {
  key @0: Data;
  value @1: Data;
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
    subscribe @3 : Void;
  }
}

struct ClientRequest {
  space @0: Text;
  sequence@2: UInt64;
  operation@1: Operation;
}

struct ClientResponse {
  sequence@3: UInt64;
  union {
    ok @0 : Void;
    okData @1 : List(Datum);
    delivery @2 : Datum;
  }
}
