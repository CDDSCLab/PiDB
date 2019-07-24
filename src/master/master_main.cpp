#include <iostream>
#include <gflags/gflags.h>
#include <bthread/bthread.h>
#include <brpc/controller.h>
#include <brpc/server.h>            
#include "master.h"

using std::cout;
using std::endl;
using std::string;
using namespace pidb;

DEFINE_string(conf, "127.0.1.1:8300:0", "master的raft配置信息（地址）");
DEFINE_string(group, "master", "master的raft组名");
DEFINE_int32(port, 8300, "port of server to listen on");
DEFINE_int32(store_heart, 3, "store心跳时间 s");
DEFINE_int32(check_store_interval, 100, "检查store存活情况的时间间隔 ms");
DEFINE_int32(region_heart, 3, "region心跳时间 s");
DEFINE_int32(check_region_interval, 10, "检查region存活情况的时间间隔 ms");
DEFINE_int32(split_cycle, 10, "分裂的新region选出leader时间 s");
DEFINE_int32(check_split_interval, 1000, "检查分裂的新region是否及时选出leader ms");
DEFINE_int32(raft_copy_num, 1, "默认的最大raft副本数量");
DEFINE_string(rto_path, "./route_table_ok.json", "正常路由表持久化地址");
DEFINE_string(rtb_path, "./route_table_bad.json", "不正常路由表持久化地址");
DEFINE_string(st_path, "./store_table.json", "store信息表持久化地址");



int main(int argc,char *argv[]){
    google::ParseCommandLineFlags(&argc,&argv,true);


    butil::AtExitManager exit_manager;
    // 创建文件夹
    if(!butil::DirectoryExists(butil::FilePath("./master"))){
        butil::CreateDirectory(butil::FilePath("./master"));
    }
    // 初始化master的rpc服务器
    brpc::Server server;
    pidb::MasterArg arg(FLAGS_store_heart,FLAGS_check_store_interval,
        FLAGS_region_heart, FLAGS_check_region_interval,
        FLAGS_split_cycle, FLAGS_check_split_interval,
        FLAGS_raft_copy_num, FLAGS_rto_path,
        FLAGS_rtb_path, FLAGS_st_path);
    auto master = new Master(&arg);
    MasterServiceImpl service(master);
    // 增加master的rpc服务
    if (server.AddService(&service, 
            brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add rpc service";
        return -1;
    }   
    // 增加raft的服务，与master的共享
    if (braft::add_service(&server, FLAGS_port) != 0) {
        LOG(ERROR) << "Fail to add raft service";
        return -1;
    }
    // 启动brpc server
    if (server.Start(FLAGS_port, NULL) != 0) {
        LOG(ERROR) << "Fail to start rpc";
        return -1;
    }

    if (master->Start(FLAGS_port, FLAGS_conf, FLAGS_group) != 0) {
        LOG(ERROR) << "Fail to start raft";
        return -1;
    }


    while (!brpc::IsAskedToQuit()) {
        //LOG(INFO) << "正常工作中";
            sleep(1);
    }

    // 先停raft再停rpc
    master->shutdown();
    server.Stop(0);
    master->join();
    server.Join();

  return 0;

}