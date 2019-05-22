#ifndef PIDB_PIDB_SERVICE_IMPL_H_
#define PIDB_PIDB_SERVICE_IMPL_H_
#include "pidb.pb.h"

namespace pidb{
class Server;
class PiDBServiceImpl:public PiDBService{
    public:
    explicit PiDBServiceImpl(Server* server):server_(server){};
    void Get(::google::protobuf::RpcController* controller,
             const ::pidb::PiDBRequest* request,
            ::pidb::PiDBResponse* response,
            ::google::protobuf::Closure* done) override;

    void Put(::google::protobuf::RpcController* controller,
               const ::pidb::PiDBRequest* request,
               ::pidb::PiDBResponse* response,
               ::google::protobuf::Closure* done) override;

    void Write(::google::protobuf::RpcController* controller,
                 const ::pidb::PiDBWriteBatch* request,
                 ::pidb::PiDBResponse* response,
                 ::google::protobuf::Closure* done) override;

    void GetSnapshot(::google::protobuf::RpcController* controller,
                     const ::pidb::Empty* request,
                     ::pidb::PiDBSnapshot* response,
                     ::google::protobuf::Closure* done) override;

    void ReleaseSnapshot(::google::protobuf::RpcController* controller,
                         const ::pidb::PiDBSnapshot* request,
                         ::pidb::Success* response,
                         ::google::protobuf::Closure* done) override;

    void GetIterator(::google::protobuf::RpcController* controller,
                     const ::pidb::PiDBIterator* request,
                     ::pidb::PiDBIterator* response,
                     ::google::protobuf::Closure* done) override;

    void Iterate(::google::protobuf::RpcController* controller,
                 const ::pidb::PiDBIterator* request,
                 ::pidb::PiDBResponse* response,
                 ::google::protobuf::Closure* done) override ;

    private:
    Server *server_;
};
}
#endif