//
// Created by ehds on 7/14/19.
//

#include "master_service_impl.h"
#include "server.h"
namespace pidb {
     void MasterServiceImpl::RaftManage(::google::protobuf::RpcController *controller,
                                       const ::pidb::PiDBRaftManageRequest *request, ::pidb::PiDBRaftManageResponse *response,
                                       ::google::protobuf::Closure *done) {

        brpc::ClosureGuard done_guard(done);
        LOG(INFO)<<request->raft_group();
        std::string group = request->raft_group();
        LOG(INFO)<<group[0];
        //if(group[0]=='b')
        response->set_is_leader(true);
        server_->HandleRaftManage(request,NULL,NULL);
    }

    void MasterServiceImpl::PullData(::google::protobuf::RpcController *controller,
                                     const ::pidb::PiDBPullRequest *request, ::pidb::PiDBPullResponse *response,
                                     ::google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        response->set_success(true);
        LOG(INFO)<<request->raft_conf()<<request->raft_group();
    }

}//namespace pidb