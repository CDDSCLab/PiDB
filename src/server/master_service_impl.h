//
// Created by ehds on 7/14/19.
//

#ifndef PIDB_MASTER_SERVICE_IMPL_H
#define PIDB_MASTER_SERVICE_IMPL_H

#include "master.pb.h"
namespace pidb {
    class Server;
    class MasterServiceImpl : public MasterService {
    public:
        explicit MasterServiceImpl(Server *server) : server_(server) {};

        virtual void RaftManage(::google::protobuf::RpcController *controller,
                        const ::pidb::PiDBRaftManageRequest *request,
                        ::pidb::PiDBRaftManageResponse *response,
                        ::google::protobuf::Closure *done) override;

        virtual void PullData(::google::protobuf::RpcController* controller,
        const ::pidb::PiDBPullRequest* request,
                ::pidb::PiDBPullResponse* response,
        ::google::protobuf::Closure* done);


        virtual ~MasterServiceImpl() {};

    private:
        Server *server_;

    };
}

#endif //PIDB_MASTER_SERVICE_IMPL_H
