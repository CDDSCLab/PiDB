#include "pidb_service_impl.h"
#include "server.h"
#include "brpc/controller.h"
namespace pidb {
    void PiDBServiceImpl::Write(::google::protobuf::RpcController *controller,
                                const ::pidb::PiDBRequest *request,
                                ::pidb::PiDBResponse *response,
                                ::google::protobuf::Closure *done) {
        brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);
        server_->Put(request, response, done);
    }

    void PiDBServiceImpl::Put(::google::protobuf::RpcController *controller,
                               const ::pidb::PiDBRequest *request,
                               ::pidb::PiDBResponse *response,
                               ::google::protobuf::Closure *done) {
        auto cntl = static_cast<brpc::Controller *>(controller);
        brpc::ClosureGuard done_guard(done);
        server_->Read(request, response, done);
    }
    void PiDBServiceImpl::WriteBatch(::google::protobuf::RpcController *controller,
                              const ::pidb::PiDBRequest *request,
                              ::pidb::PiDBResponse *response,
                              ::google::protobuf::Closure *done) {
        auto cntl = static_cast<brpc::Controller *>(controller);
        brpc::ClosureGuard done_guard(done);
        server_->Read(request, response, done);
    }
}
//TO-DO 加入新的功能。