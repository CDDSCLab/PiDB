#ifndef PIDB_PIDB_SERVICE_IMPL_H_
#define PIDB_PIDB_SERVICE_IMPL_H_
#include "pidb.pb.h"

namespace pidb{
class Server;
class PiDBServiceImpl:public PiDBService{
    public:
    explicit PiDBServiceImpl(Server* server):server_(server){};
    void write(::google::protobuf::RpcController* controller,
             const ::pidb::PiDBRequest* request,
            ::pidb::PiDBResponse* response,
            ::google::protobuf::Closure* done);
    void read(::google::protobuf::RpcController* controller,
               const ::pidb::PiDBRequest* request,
               ::pidb::PiDBResponse* response,
               ::google::protobuf::Closure* done);
    private:
    Server *server_;
};
}
#endif