#include <iostream>
#include <gflags/gflags.h>
#include "server.h"
#include "context_cache.h"
#include "route_table.h"
#include <leveldb/db.h>
#include "raftnode.h"
#include "master_service_impl.h"

DEFINE_string(data_path,"./data","Path of data stored on");
DEFINE_int32(port,8100,"port of server to listen on");
DEFINE_string(master_server,"127.0.1.1:8300","port of server to listen on");

int main(int argc,char *argv[]){
    GFLAGS_NS::ParseCommandLineFlags(&argc,&argv,true);
    butil::AtExitManager exit_manager;
    //创建文件夹
    if(!butil::DirectoryExists(butil::FilePath("./data"))){
        butil::CreateDirectory(butil::FilePath("./data"));
    }
    //初始化server
    brpc::Server server;
    pidb::ServerOption options(FLAGS_data_path,FLAGS_port);
    options.heartbeat_timeout_ms = 2000;
    auto s = std::shared_ptr<pidb::Server>(new pidb::Server(options));
    pidb::PiDBServiceImpl service(s.get());
    pidb::MasterServiceImpl master(s.get());

    //增加Node Server的服务
    if (server.AddService(&service,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }


    if(server.AddService(&master,brpc::SERVER_DOESNT_OWN_SERVICE)!=0){
        LOG(ERROR)<<"Fail to add master service";
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
//    pidb::RaftOption option;
//    option.port = 8200;
//    option.group = "group1";
//    option.conf = "127.0.1.1:8200:1";
//    option.data_path ="./group1";
//    auto status= s->registerRaftNode(option,pidb::Range("",""));

//    option.conf = "127.0.1.1:8100:0";
//    option.group = "group0";
//    option.data_path ="./group0";
//    status= s->registerRaftNode(option,pidb::Range("",""));

//    LOG(INFO)<<"Register Node"<<status.ToString();

    while (!brpc::IsAskedToQuit()){
        sleep(1);
    }
   server.Join();


  return 0;

}