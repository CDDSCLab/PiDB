// Created by blue on 2019/6/3.

// master主要提供三方面功能
// 和client交互返回路由信息、处理来自server的心跳
// 处理来自region的leader的心跳

#ifndef PIDB_MASTER_H
#define PIDB_MASTER_H

#include <brpc/controller.h>
#include <braft/raft.h>
#include <braft/util.h> 
#include <bthread/bthread.h>
#include <string>
#include "./route_table.h"
#include "./store_heartbeat.h"
#include "./master.pb.h"
#include "./guard_dog.h"
#include "./raft_manage.h"

using std::string;

namespace  pidb{

class ClientClosure; // 跟client交互的rpc闭包
class StoreClosure;  // 接收store心跳的rpc闭包
class RegionClosure; // 接收region心跳的rpc闭包
class SplitClosure;  // 处理Region分裂的rpc闭包

class Master;
// 用于raft调度的参数封装，因为创建线程只有一个参数
struct RM{
	Master* master_;
    string min_key_;
    string max_key_;
    bool is_split_;  // 区分是初始化新增0还是分裂新增1
    void* arg_;      // raft调度的回调函数的参数封装
	RM(Master* m,const string& n,const string& x,bool i,void* v):
        master_(m),min_key_(n),max_key_(x),is_split_(i),arg_(v){};
};
// 用于检测分裂的region是否及时选出leader的回调函数的参数封装
struct ISHL{
    Master* master_;
    string old_group_;
    string new_group_;
    ISHL(Master* m, const string& o, const string& n):
        master_(m), old_group_(o), new_group_(n){};
};
// 用于初始化Master的参数封装（太多了）
struct MasterArg{
    int store_heart;           // store心跳间隔（s）
    int check_store_interval;  // 看门狗检测store的间隔（ms）
    int region_heart;          // region心跳间隔（s）
    int check_region_interval; // 看门狗检测region的间隔（ms）
    int raft_copy_num;         // 默认最大raft副本数
    const string& rto_path;    // ok路由表的相对路径
    const string& rtb_path;    // bad路由表的相对路径
    const string& st_path;     // store信息表的相对路径

    MasterArg(int a, int b, int c, int d, int e, int f, int g,
        const string& h, const string& i, const string& j):
        store_heart(a), check_store_interval(b), region_heart(c),
        check_region_interval(d), raft_copy_num(g),
        rto_path(h), rtb_path(i), st_path(j){};
};
// 把Master实现为一个状态机
class Master : public braft::StateMachine {
private:
    // 自己的参数
    RouteTable* route_table_ok_;        // 正常路由表
    RouteTable* route_table_bad_;       // 非正常路由表
    StoreHeartbeat* store_table_; // stroe信息表
    GuardDog* guard_dog_store_; // 检查store存活的看门狗
    GuardDog* guard_dog_region_; // 检查region存活的看门狗
    RaftManage* rm_;  // 用于raft节点调度
    int raft_copy_num_;  // raft组默认的最大副本数
    int region_heart_;   // region的心跳，有特殊用
    // 不同的Raft操作区分标志（因为都是在同一个on_apply中执行）
    enum RaftOpType{ 
        DefalutOp_, StoreHeartOp_, RegionHeartOp_, RegionSplitOp_
    };

    // braft的参数
    braft::Node* volatile node_;  // raft的一个节点
    butil::atomic<int64_t> leader_term_;  // 任期号

public:
    // store心跳时间(s),检查store存活的时间间隔(ms)
    Master(MasterArg* master_arg);
    ~Master();

    // 与client的交互（即路由信息查询）
    void QueryRoute(const PiDBClientRequest* request,
        PiDBClientResponse* response);

    // 与Store的交互
    // 新增3个region
    void AddRegion(const string& min_key, const string& max_key,
        bool is_split, void* arg); 
    static void* DoAddRegion(void* arg);
    // 接收心跳
    void HandleStore(const PiDBStoreRequest* request,
        PiDBStoreResponse* response, google::protobuf::Closure* done);

    // 与Region的交互
    // 处理分裂
    void RegionSplit(const PiDBSplitRequest* request,
        PiDBSplitResponse* response, google::protobuf::Closure* done);
    // 接收心跳并处理（普通新增或删除节点）
    void HandleRegion(const PiDBRegionRequest* request,
        PiDBRegionResponse* response, google::protobuf::Closure* done);

    // 分裂后的region选出leader后的回调函数
    static void IfSplitHasLeader(void* arg, brpc::Controller* cntl,
        PiDBRaftManageResponse* response);

    // raft节点启动
    int Start(int port, string conf, string group);

    // 关闭此raft节点.
    void shutdown() {if (node_) node_->shutdown(NULL);}

    // 阻塞线程直到节点完成关闭
    void join() {if (node_) node_->join();}

private:
    friend class ClientClosure;
    friend class StoreClosure;
    friend class RegionClosure;
    friend class SplitClosure;
    // leader的重定向
    void Redirect(PiDBClientResponse* response);
    void Redirect(PiDBStoreResponse* response);
    void Redirect(PiDBRegionResponse* response);
    void Redirect(PiDBSplitResponse* response);
    
    // 具体处理来自Store的心跳信息
    void DoHandleStore(braft::Iterator& iter);

    // 具体处理来自Store的心跳信息
    void DoHandleRegion(braft::Iterator& iter);

    // 具体处理Region的分裂
    void DoRegionSplit(braft::Iterator& iter);

    // @braft::StateMachine
    void on_apply(braft::Iterator& iter);;
    
    // braft的一堆东西
    void on_leader_start(int64_t term);
    void on_leader_stop(const butil::Status& status);
    void on_shutdown();
    void on_error(const ::braft::Error& e);
    void on_configuration_committed(const ::braft::Configuration& conf);
    void on_stop_following(const ::braft::LeaderChangeContext& ctx);
    void on_start_following(const ::braft::LeaderChangeContext& ctx);
};

class ClientClosure : public braft::Closure {
public:
    ClientClosure(Master* master, const PiDBClientRequest* request,
        PiDBClientResponse* response, google::protobuf::Closure* done)
        : master_(master), _request(request)
        , _response(response), _done(done) {}
    ~ClientClosure() {}

    const PiDBClientRequest* request() const { return _request; }
    PiDBClientResponse* response() const { return _response; }
    void Run();

private:
    Master* master_;
    const PiDBClientRequest* _request;
    PiDBClientResponse* _response;
    google::protobuf::Closure* _done;
};

class StoreClosure : public braft::Closure {
public:
    StoreClosure(Master* master, const PiDBStoreRequest* request,
        PiDBStoreResponse* response, google::protobuf::Closure* done)
        : master_(master), request_(request)
        , response_(response), done_(done) {}
    ~StoreClosure() {}

    const PiDBStoreRequest* request() const { return request_; }
    PiDBStoreResponse* response() const { return response_; }
    void Run();

private:
    Master* master_;
    const PiDBStoreRequest* request_;
    PiDBStoreResponse* response_;
    google::protobuf::Closure* done_;
};

class RegionClosure : public braft::Closure {
public:
    RegionClosure(Master* master, const PiDBRegionRequest* request,
        PiDBRegionResponse* response, google::protobuf::Closure* done)
        : master_(master), request_(request)
        , response_(response), done_(done) {}
    ~RegionClosure() {}

    const PiDBRegionRequest* request() const { return request_; }
    PiDBRegionResponse* response() const { return response_; }
    void Run();

private:
    Master* master_;
    const PiDBRegionRequest* request_;
    PiDBRegionResponse* response_;
    google::protobuf::Closure* done_;
};

class SplitClosure : public braft::Closure {
public:
    SplitClosure(Master* master, const PiDBSplitRequest* request,
        PiDBSplitResponse* response, google::protobuf::Closure* done)
        : master_(master), request_(request)
        , response_(response), done_(done) {}
    ~SplitClosure() {}

    const PiDBSplitRequest* request() const { return request_; }
    PiDBSplitResponse* response() const { return response_; }
    void Run();

private:
    Master* master_;
    const PiDBSplitRequest* request_;
    PiDBSplitResponse* response_;
    google::protobuf::Closure* done_;
};

// 实现MasterService(brpc)
class MasterServiceImpl : public MasterService {
public:
    explicit MasterServiceImpl(Master* master) : master_(master) {}
    void QueryRoute(google::protobuf::RpcController* controller,
        const PiDBClientRequest* request, PiDBClientResponse* response,
        google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        return master_->QueryRoute(request, response);
    }
    void StoreHeartbeat(google::protobuf::RpcController* controller,
        const PiDBStoreRequest* request, PiDBStoreResponse* response,
        google::protobuf::Closure* done) {
        return master_->HandleStore(request, response, done);
    }
    void RegionHeartbeat(google::protobuf::RpcController* controller,
        const PiDBRegionRequest* request, PiDBRegionResponse* response,
        google::protobuf::Closure* done) {
        return master_->HandleRegion(request, response, done);
    }
    void RegionSplit(google::protobuf::RpcController* controller,
        const PiDBSplitRequest* request, PiDBSplitResponse* response,
        google::protobuf::Closure* done) {
        return master_->RegionSplit(request, response, done);
    }
private:
    Master* master_;
};

}// namespace pidb

#endif //PIDB_MASTER_H
