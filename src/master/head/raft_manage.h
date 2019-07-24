// 此部分主要目的是CheckAlive
// 具体实现是维持一个周期，在此周期范围内
// 每隔一定的时间间隔执行某件事情（若有事）
// 例如周期3s,时间间隔10ms,那么每个周期就有300
// 次去处理对应时刻可能存在的事情

#ifndef PIDB_RAFT_MANAGE_H
#define PIDB_RAFT_MANAGE_H

#include <vector>
#include <string>
#include <bthread/bthread.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <braft/raft.h>
#include <braft/util.h>
#include "master.pb.h"

using std::vector;
using std::string;

namespace  pidb{

class RaftManage{
private:
    brpc::ChannelOptions options_; // brpc的一个参数

public:
    RaftManage();
    ~RaftManage(){};

    // 初始化Stub配置,主要是server(store)地址不一样
    void InitialStub(pidb::MasterService_Stub** stub, const string& addr,brpc::Channel &channel);

    // 新增一个节点,addr是需要新增节点的store的地址
    // 若分裂会有回调函数及其参数
    void AddNode(const string& group, const string& conf,
        const string& min_key, const string& max_key,
        const string& addr, bool is_split, 
        void(*CallBack)(void*, brpc::Controller*), void* arg);
    
    // 删除一个节点,即删除addr上的组名为group的region
    void RemoveNode(const string& group, const string& addr);

    // 通知某个节点开始拉数据
    // old_addr和new_addr分别是老region和新region的地址
    // 返回值代表是否成功通知到
    bool PullData(const string& old_addr, const string& group,
        const string& conf, const string& new_addr);

    // shanchu
    static void HandleEchoResponse(
            brpc::Controller* cntl,pidb::PiDBRaftManageResponse* response) {
        // std::unique_ptr makes sure cntl/response will be deleted before returning.
        //std::unique_ptr<brpc::Controller> cntl_guard(cntl);

        if (cntl->Failed()) {
            LOG(WARNING) << "Fail to send EchoRequest, " << cntl->ErrorText();
            return;
        }
        LOG(INFO) << "Received response from " << cntl->remote_side()
                  << ": " <<  " (attached="
                  << ")"
                  << " latency=" << cntl->latency_us() << "us";
    }
};
}// namespace pidb

#endif //PIDB_RAFT_DISPATCH_H
