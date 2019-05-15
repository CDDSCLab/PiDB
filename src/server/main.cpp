#include <iostream>
#include <gflags/gflags.h>
#include "server.h"
#include "route_table.h"

DEFINE_string(data_path,"./data","Path of data stored on");
DEFINE_int32(port,8100,"port of server to listen on");

int main(int argc,char *argv[]){
    GFLAGS_NS::ParseCommandLineFlags(&argc,&argv,true);
    butil::AtExitManager exit_manager;
    //默认参数

//
//    pidb::RouteTable r;
//    r.AddRecord("","a","group1");
//    r.AddRecord("c","d","group2");
//    r.AddRecord("d","f","group3");
//    std::cout<<r.FindRegion("abc");
//    LOG(INFO)<<r.FindRegion("abc");

//    brpc::Server server;
//    pidb::ServerOption option(FLAGS_data_path,FLAGS_port);
//    pidb::Server s(option);
//
//    pidb::PiDBServiceImpl service(&s);
//
//    server.AddService(&service,brpc::SERVER_DOESNT_OWN_SERVICE);
//    server.Start(FLAGS_port,NULL);
//    while (!brpc::IsAskedToQuit()){
//        sleep(1);
//    }
//
//    server.Stop(0);

    brpc::Server server;

    pidb::Server s({FLAGS_data_path,FLAGS_port});
    pidb::PiDBServiceImpl service(&s);
    if (server.AddService(&service,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }

    if (braft::add_service(&server, FLAGS_port) != 0) {
        LOG(ERROR) << "Fail to add raft service";
        return -1;
    }

    if (server.Start(FLAGS_port, NULL) != 0) {
        LOG(ERROR) << "Fail to start Server";
        return -1;
    }
    s.Start();

    while (!brpc::IsAskedToQuit()){
        sleep(1);
    }
    server.Join();

    return 0;

}