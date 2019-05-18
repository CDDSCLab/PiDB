#include "pidb_service_impl.h"
#include "server.h"
#include "brpc/controller.h"
namespace pidb {
    void PiDBServiceImpl::Put(::google::protobuf::RpcController *controller,
                                const ::pidb::PiDBRequest *request,
                                ::pidb::PiDBResponse *response,
                                ::google::protobuf::Closure *done) {
        brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);
        server_->Put(request, response, done);
    }

    void PiDBServiceImpl::Get(::google::protobuf::RpcController *controller,
                               const ::pidb::PiDBRequest *request,
                               ::pidb::PiDBResponse *response,
                               ::google::protobuf::Closure *done) {
        auto cntl = static_cast<brpc::Controller *>(controller);
        brpc::ClosureGuard done_guard(done);
        LOG(INFO)<<"Get";
        //server_->Read(request, response, done);
    }
    void PiDBServiceImpl::Write(::google::protobuf::RpcController *controller,
                                const ::pidb::PiDBWriteBatch *request,
                                ::pidb::PiDBResponse *response,
                                ::google::protobuf::Closure *done) {

        //brpc是使用static_cast转化，这是不安全的，https://docs.microsoft.com/en-us/cpp/cpp/static-cast-operator?view=vs-2019
        //应该使用dynamic_cast
        auto cntl = static_cast<brpc::Controller *>(controller);
        auto req = request;
        LOG(INFO)<<req->writebatch_size();
        for(const auto batch:req->writebatch()){
            LOG(INFO)<<batch.key();
        }

    }
}
//TO-DO 加入新的功能。