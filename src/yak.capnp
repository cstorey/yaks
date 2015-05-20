@0xaef6714f4106a85d;

struct ClientRequest {
  union {
    truncate @0 : Void; 
    read @1 : Void; 
  }
}


struct ClientResponse {
  ok @0 : Void; 
}
