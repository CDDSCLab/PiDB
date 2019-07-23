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
#include <braft/route_table.h>
#include "./master.pb.h"

using std::vector;
using std::string;

namespace  pidb{

class RaftManage{
private:
    brpc::ChannelOptions options_; // brpc的一个参数
    int max_time_out_;   // 回调函数最大超时（s）
    int normal_time_out_; // 一般请求的超时（ms）

public:
    RaftManage();
    ~RaftManage(){};

    // 初始化Stub配置,主要是server(store)地址不一样
    bool InitialStub(MasterService_Stub** stub, const string& addr,
        brpc::Channel& channel);

    // 新增一个节点,addr是需要新增节点的store的地址
    // 若分裂会有回调函数及其参数
    void AddNode(const string& group, const string& conf,
        const string& min_key, const string& max_key,
        const string& addr, bool is_split, 
        void(*CallBack)(void*, brpc::Controller*,
        PiDBRaftManageResponse*), void* arg);
    
    // 删除一个节点,即删除addr上的组名为group的region
    void RemoveNode(const string& group, const string& addr);

    // 通知某个节点开始拉数据
    // old_addr和new_addr分别是老region和新region的地址
    // 返回值代表是否成功通知到
    bool PullData(const string& old_addr, const string& old_group,
        const string& old_conf, const string& new_conf,
        const string& new_group);
};
}// namespace pidb

#endif //PIDB_RAFT_DISPATCH_H
