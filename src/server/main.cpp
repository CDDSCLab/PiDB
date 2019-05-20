#include <iostream>
#include <gflags/gflags.h>
#include "server.h"
#include "context_cache.h"
#include "route_table.h"
#include <leveldb/db.h>

DEFINE_string(data_path,"./data","Path of data stored on");
DEFINE_int32(port,8100,"port of server to listen on");

int main(int argc,char *argv[]){
    GFLAGS_NS::ParseCommandLineFlags(&argc,&argv,true);
    butil::AtExitManager exit_manager;


    //初始化server
    brpc::Server server;
    pidb::ServerOption options(FLAGS_data_path,FLAGS_port);
    auto s = new pidb::Server(options);
    pidb::PiDBServiceImpl service(s);
    //增加Node Server的服务
    if (server.AddService(&service,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }
    //增加raft的服务，与Node Server共享

    if (braft::add_service(&server, FLAGS_port) != 0) {
        LOG(ERROR) << "Fail to add raft service";
        return -1;
    }
    //启动brpc server
    if (server.Start(FLAGS_port, NULL) != 0) {
        LOG(ERROR) << "Fail to start Server";
        return -1;
    }
    //启动Node server
    s->Start();
    //等待用户结束
    while (!brpc::IsAskedToQuit()){
        sleep(1);
    }
    server.Join();


  return 0;

}