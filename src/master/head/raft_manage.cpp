#include "raft_manage.h"

namespace pidb{
RaftManage::RaftManage(){
    options_.protocol = "baidu_std";
    options_.connection_type = "";
    options_.timeout_ms = 100;  // ms;
    options_.max_retry = 3;
    max_time_out_ = 22;   // s
    normal_time_out_ = 100; // ms
}

bool RaftManage::InitialStub(MasterService_Stub** stub, const string& addr,
        brpc::Channel &channel){

    if (channel.Init(addr.c_str(), "", &options_) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        return false;
    }
    *stub = new pidb::MasterService_Stub(&channel); 
    return true;
}

void RaftManage::AddNode(const string& group, const string& conf,
        const string& min_key, const string& max_key, 
        const string& addr,bool is_split, 
        void(*CallBack)(void*, brpc::Controller*,
        PiDBRaftManageResponse* response), void* arg){
    pidb::MasterService_Stub* stub = NULL;
    brpc::Channel channel;

    if(!InitialStub(&stub, addr, channel)) return;
    
    if(!is_split){
        PiDBRaftManageResponse response;
        pidb::PiDBRaftManageRequest request;
       
        request.set_raft_group(group);
        request.set_raft_conf(conf);
        request.set_min_key(min_key);
        request.set_max_key(max_key);
        request.set_is_new(true);

        // 发送请求
        brpc::Controller cntl;
        stub->RaftManage(&cntl, &request, &response, NULL);
    }
    else{
        // 异步调用需要一直保存下列内容直到回调函数完成
        PiDBRaftManageResponse* response = new PiDBRaftManageResponse();
        brpc::Controller* cntl = new brpc::Controller();
        cntl->set_timeout_ms(max_time_out_ * 1000);

        pidb::PiDBRaftManageRequest request;
        request.set_raft_group(group);
        request.set_raft_conf(conf);
        request.set_min_key(min_key);
        request.set_max_key(max_key);
        request.set_is_new(true);

        // 发送请求
        google::protobuf::Closure* done = brpc::NewCallback(
            CallBack, arg, cntl, response);
        stub->RaftManage(cntl, &request, response, done);
    } 
    // 清理内存？?????
}

bool RaftManage::PullData(const string& old_addr, const string& old_group,
        const string& old_conf, const string& new_conf,
        const string& new_group){
    // 这是是给raft发请求，跟上面的单纯的rpc不一样
    if (braft::rtb::update_configuration(new_group,new_conf) != 0) {
        LOG(ERROR) << "Fail to register configuration " << new_conf
                   << " of group " << new_group;
        return false;
    }
    braft::PeerId leader;  // 新region的leader
    while (true) {
        // 选新region的leader
        if (braft::rtb::select_leader(new_group, &leader) != 0) {
            // 如果没有leader，直接返回失败重新建raft（）
            // 因为是选出leader后才会调用此函数
            braft::rtb::refresh_leader(new_group, normal_time_out_);
            LOG(INFO)<<"分裂的新raft没有leader\n";
            continue;
        }

        // 现在有leader了，发具体通知
        MasterService_Stub* stub = NULL;
        brpc::Channel channel;

        auto addr = leader.to_string();
        addr.erase(addr.begin()+addr.rfind(":"),addr.end());
        LOG(INFO)<<addr;
        if(!InitialStub(&stub, addr, channel))
            // 可能是leader变了
            continue;

        // 同步调用
        PiDBPullResponse response;
        pidb::PiDBPullRequest request;
        request.set_leader_addr(old_addr);
        request.set_raft_group(old_group);
        request.set_raft_conf(old_conf);

        // 发送请求
        brpc::Controller* cntl = new brpc::Controller();
        cntl->set_timeout_ms(normal_time_out_);
        LOG(INFO)<<"PULL dATA";
        assert(stub!= nullptr);
        stub->PullData(cntl, &request, &response, NULL);
        LOG(INFO)<<"PULL dATA";
        // 直接失败，不管了直接重新建立raft
        if(cntl->Failed()){
            LOG(INFO)<<cntl->ErrorText();
            delete cntl;
            return false;
        }
        // 有更新leader
        if (!response.success() && response.has_redirect()) {
            braft::rtb::update_leader(new_group, response.redirect());
            continue;
        }

//        delete cntl;
//        delete stub;
        return true;
    }
}

void RaftManage::RemoveNode(const string& group, 
        const string& addr){
    pidb::MasterService_Stub* stub = NULL;
    brpc::Channel channel;
    if(!InitialStub(&stub, addr, channel)) return;
    
    // 异步调用需要一直保存下列内容直到回调函数完成
    PiDBRaftManageResponse* response = new PiDBRaftManageResponse();
    brpc::Controller* cntl = new brpc::Controller();

    pidb::PiDBRaftManageRequest request;
    request.set_raft_group(group);
    request.set_is_new(false);

    // 发送请求
    stub->RaftManage(cntl, &request, response, NULL);
}
}