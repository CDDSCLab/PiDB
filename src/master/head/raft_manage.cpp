#include "raft_manage.h"

namespace pidb{
RaftManage::RaftManage(){
    options_.protocol = "baidu_std";
    options_.connection_type = "";
    options_.timeout_ms = 100;  // ms;
    options_.max_retry = 3;
}

void RaftManage::InitialStub(pidb::MasterService_Stub** stub, const string& addr,
        brpc::Channel &channel){
    if (channel.Init(addr.c_str(), "", &options_) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        return;
    }
    *stub = new pidb::MasterService_Stub(&channel);
}

void RaftManage::AddNode(const string& group, const string& conf,
        const string& min_key, const string& max_key, 
        const string& addr,bool is_split, 
        void(*CallBack)(void*, brpc::Controller*), void* arg){

//    pidb::MasterService_Stub* stub = NULL;
//    LOG(INFO)<<"Add Node";
   brpc::Channel channel;
//    InitialStub(&stub, addr,channel);
//    brpc::ChannelOptions options;
//    if (channel.Init(addr.c_str(), "", &options_) != 0) {
//            LOG(ERROR) << "Fail to initialize channel";
//            return;
//    }

     //Initialize the channel, NULL means using default options.
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.connection_type = "";
    options.timeout_ms = 100/*milliseconds*/;
    options.max_retry = 3;
    if (channel.Init(addr.c_str(), "", &options) != 0) {
        LOG(ERROR) << "Fail to pinitialize channel";
    }

    // Normally, you should not call a Channel directly, but instead construct
    // a stub Service wrapping it. stub can be shared by all threads as well.
    pidb::MasterService_Stub stub(&channel);

    brpc::Controller *cntl = new brpc::Controller();

//    google::protobuf::Closure* done = brpc::NewCallback(
//            &HandleEchoResponse, cntl);

  //  pidb::EchoResponse response1;

    // Notice that you don't have to new request, which can be modified
    // or destroyed just after stub.Echo is called.
//    pidb::EchoRequest request1;
//    request1.set_message("hello world");
//    LOG(INFO)<<"SEND ECHO";
//    stub->Echo(cntl,&request1,&response1,done);
    //sleep(1);

   // InitialStub(&stub, addr,channel);
//        brpc::Channel channel;
//        brpc::ChannelOptions options;
//        if (channel.Init(addr.c_str(), "", &options_) != 0) {
//            LOG(ERROR) << "Fail to initialize channel";
//            return;
//        }
//        stub = new pidb::MasterService_Stub(&channel);


    LOG(INFO)<<"REMOTE RAFT MANAGE ADDR "<<addr;
    if(!is_split){
       // brpc::Controller *cntl = new brpc::Controller;
        pidb::PiDBRaftManageResponse *response = new PiDBRaftManageResponse();
        pidb::PiDBRaftManageRequest request;

        request.set_is_new(true);
        request.set_raft_group(group);
//        request.set_raft_conf(conf);
//        request.set_min_key(min_key);
//        request.set_max_key(max_key);

        // 发送请求






        pidb::EchoResponse response1;

        // Notice that you don't have to new request, which can be modified
        // or destroyed just after stub.Echo is called.
        pidb::EchoRequest request1;
        request1.set_message("hello world");
        LOG(INFO)<<"SEND ECHO";

       // stub->RaftManage(cntl, &request, response, done);
    }
    else{
        LOG(INFO)<<"hehehehehehehehehheheheheheh";
        // 异步调用需要一直保存下列内容直到回调函数完成
        pidb::PiDBRaftManageResponse response;

        //assert(CallBack != nullptr);

        pidb::PiDBRaftManageRequest request;
        pidb::PiDBRaftManageRequest request1;


        //delete &request1;
        request.set_raft_group(group);
        request.set_raft_conf(conf);
        request.set_min_key(min_key);
        request.set_max_key(max_key);
        request.set_is_new(true);

        // 发送请求
        google::protobuf::Closure* done = brpc::NewCallback(
                &HandleEchoResponse,cntl, &response);

        LOG(INFO)<<"RaftManage send request";
           stub.RaftManage(cntl, &request, &response, done);

//        if(!cntl->Failed()){
//            LOG(INFO)<<"RAFT MANAGER SEND SUCCESS"<<request.raft_group();
//        }else{
//            LOG(INFO)<<"RAFT MANAGER SEND FAIL "<<cntl->ErrorText();
//        }
    }

}

bool RaftManage::PullData(const string& old_addr, const string& group,
        const string& conf, const string& new_addr){
    MasterService_Stub* stub = NULL;
    brpc::Channel channel;
    InitialStub(&stub, new_addr,channel);
    
    // 同步调用
    PiDBPullResponse response;
    pidb::PiDBPullRequest request;
    request.set_leader_addr(old_addr);
    request.set_raft_group(group);
    request.set_raft_conf(conf);

    // 发送请求
    brpc::Controller* cntl = new brpc::Controller();
    stub->PullData(cntl, &request, &response, NULL);
    LOG(INFO)<<"PullData";
    if(cntl->Failed()){
        delete cntl;
        return false;
    }
    return true;
}

void RaftManage::RemoveNode(const string& group, 
        const string& addr){
    pidb::MasterService_Stub* stub = NULL;
    brpc::Channel channel;
    InitialStub(&stub, addr,channel);
    
    // 异步调用需要一直保存下列内容直到回调函数完成
    pidb::PiDBRaftManageResponse* response = new pidb::PiDBRaftManageResponse();
    brpc::Controller* cntl = new brpc::Controller();

    pidb::PiDBRaftManageRequest request;
    request.set_raft_group(group);
    request.set_is_new(false);

    // 发送请求
    stub->RaftManage(cntl, &request, response, NULL);
}
}