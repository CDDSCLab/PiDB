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
        server_->Get(request, response, done);
    }

    void PiDBServiceImpl::Write(::google::protobuf::RpcController *controller,
                                const ::pidb::PiDBWriteBatch *request,
                                ::pidb::PiDBResponse *response,
                                ::google::protobuf::Closure *done) {

        //brpc是使用static_cast转化，这是不安全的，https://docs.microsoft.com/en-us/cpp/cpp/static-cast-operator?view=vs-2019
        //应该使用dynamic_cast
        auto cntl = static_cast<brpc::Controller *>(controller);
        auto req = request;
        auto batch_size = req->writebatch_size();
        if(batch_size<1){
            response->set_success(false);
            return;
        }
        return server_->Write(request,response,done);

    }

    void PiDBServiceImpl::GetSnapshot(::google::protobuf::RpcController *controller,
                                     const ::pidb::Empty *request,
                                      ::pidb::PiDBSnapshot *response,
                                      ::google::protobuf::Closure *done){
        brpc::ClosureGuard done_guard(done);

        auto id =  server_->GetSnapshot();
        response->set_id(id);

    }

    void PiDBServiceImpl::ReleaseSnapshot(::google::protobuf::RpcController *controller,
                                          const ::pidb::PiDBSnapshot *request,
                                          ::pidb::Success *response,
                                          ::google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        auto id = request->id();
        auto s = server_->ReleaseSnapshot(id);
        if(s.ok()){
            response->set_success(true);
        }else{
            response->set_success(false);
            response->set_message(s.ToString());
        }

    }
}
//TO-DO 加入新的功能。