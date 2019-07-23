#include "master.h"

using std::string;
namespace pidb{
Master::Master(MasterArg* master_arg){
    route_table_ok_ = new RouteTable();
    route_table_bad_ = new RouteTable();
    store_table_ = StoreHeartbeat::Initial();
    route_table_ok_->ReadFromFile(master_arg->rto_path);
    route_table_bad_->ReadFromFile(master_arg->rtb_path);
    store_table_->ReadFromFile(master_arg->st_path);
    rm_ = new RaftManage();
    raft_copy_num_ = master_arg->raft_copy_num;
    region_heart_ = master_arg->region_heart;
    // 启动看门狗检查存活
    guard_dog_store_ = new GuardDog(
        master_arg->store_heart, master_arg->check_store_interval);
    guard_dog_region_ = new GuardDog(
        master_arg->region_heart, master_arg->check_region_interval);
    guard_dog_store_->HandleThings();
    guard_dog_region_->HandleThings();

    node_ = NULL;
    leader_term_ = -1;

    // 如果路由表为空考虑新建第一个region
    if(route_table_bad_->GetSize() == 0 && 
            route_table_ok_->GetSize() == 0){
        route_table_bad_->UpdateRouteInfo("", "", "group", "", "", -1);
        AddRegion("", "", false, nullptr);
    }
}

Master::~Master(){ 
    route_table_ok_->Clear();
    route_table_bad_->Clear();
    store_table_->Clear();
    delete guard_dog_store_;
    delete guard_dog_region_;
    delete node_;
    delete rm_;
}

void Master::QueryRoute(const PiDBClientRequest* request,
                        PiDBClientResponse* response) {
    // 考虑leader是否变了
    const int64_t term = leader_term_.load(butil::memory_order_relaxed);
    if (term < 0) return Redirect(response);

    string key = request->key();
    string group("");
    string addr("");
    string conf("");
    int state = 0;
    // 正常找到
    if(this->route_table_ok_->GetRouteInfo(key,group,addr,conf,state)){
        response->set_success(true);
        response->set_leader_addr(addr);
        response->set_raft_group(group);
        response->set_raft_conf(conf);
    }
    else if(this->route_table_bad_->GetRouteInfo(key,group,addr,conf,state)){
        // 数据不完整（只有leader），但已经可以写入
        if(state == -3 ){//&& (request->action() == PiDBClientRequest_Action_PUT || 
                //request->action() == PiDBClientRequest_Action_DELETE)){
            response->set_success(true);
            response->set_leader_addr(addr);
            response->set_raft_group(group);
            response->set_raft_conf(conf);
        }
        else response->set_success(false);
    }
    else response->set_success(false);
}

void Master::AddRegion(const string& min_key,const string& max_key,
        bool is_split, void* arg){
    RM* rm = new RM(this, min_key, max_key, is_split, arg);
    // 开线程执行，（一直等到store足够才能新增）
    bthread_t t;
    if (bthread_start_background(&t, NULL, 
        Master::DoAddRegion, rm) != 0) {
        LOG(ERROR) << "create bthread AddRegion fail";
    }
    else LOG(INFO) << "create bthread AddRegion success";
}
void* Master::DoAddRegion(void* arg){
    RM*rm = (RM*)arg;
    Master* master = rm->master_;
    string min_key = rm->min_key_;
    string max_key = rm->max_key_;
    bool is_split = rm->is_split_; 
    string group = min_key + "group"; 
    ISHL* arg_ISHL = (ISHL*)rm->arg_;
    
    // 一直等到有足够的store
    hehe:
    while(master->store_table_->GetSize() < master->raft_copy_num_) 
        sleep(3);
    std::vector<string> v;
    if(master->store_table_->GetStoreInfo(master->raft_copy_num_,
            nullptr, &v, true) != 0) goto hehe;

    // 拼接raft配置
    string conf("");
    for(auto&s : v) conf = conf + s + ":0";
    // 通知找出来的store新增region
    for(auto&s : v){
        if(is_split)
            master->rm_->AddNode(group, conf, min_key, max_key, s,
                is_split, Master::IfSplitHasLeader, arg_ISHL);
        else
            master->rm_->AddNode(group, conf, min_key, max_key, s,
                is_split, nullptr, nullptr);
    }
    if(is_split) master->route_table_bad_->UpdateRouteInfo(min_key, 
        max_key, group, "", conf, -2);
    else master->route_table_bad_->UpdateRouteInfo(min_key, 
        max_key, group, "", conf, -1);
    // 释放内存
    delete rm;
}

void Master::HandleStore(const PiDBStoreRequest* request,
        PiDBStoreResponse* response, google::protobuf::Closure* done){
    brpc::ClosureGuard done_guard(done);
    // 考虑leader是否变了
    const int64_t term = leader_term_.load(butil::memory_order_relaxed);
    if (term < 0) return Redirect(response);
    
    // 串行化请求到IOBuf中
    butil::IOBuf log;
    log.push_back((uint8_t)RaftOpType::StoreHeartOp_);
    butil::IOBufAsZeroCopyOutputStream wrapper(&log);
    if (!request->SerializeToZeroCopyStream(&wrapper)) {
        LOG(ERROR) << "串行化请求失败";
        response->set_success(false);
        return;
    }
    // Apply this log as a braft::Task
    braft::Task task;
    task.data = &log;
    // 下面的回调会在task启动或失败时调用
    task.done = new StoreClosure(this, request, 
        response, done_guard.release());
    
    task.expected_term = term;

    // task放入raft组，等待执行结果
    return node_->apply(task);
}

void Master::HandleRegion(const PiDBRegionRequest* request,
        PiDBRegionResponse* response, google::protobuf::Closure* done){
    brpc::ClosureGuard done_guard(done);
    // 考虑leader是否变了
    const int64_t term = leader_term_.load(butil::memory_order_relaxed);
    if (term < 0) return Redirect(response);
    
    // 串行化请求到IOBuf中
    butil::IOBuf log;
    log.push_back((uint8_t)RaftOpType::RegionHeartOp_);
    butil::IOBufAsZeroCopyOutputStream wrapper(&log);
    if (!request->SerializeToZeroCopyStream(&wrapper)) {
        LOG(ERROR) << "串行化请求失败";
        response->set_success(false);
        return;
    }
    // Apply this log as a braft::Task
    braft::Task task;
    task.data = &log;
    // 下面的回调会在task启动或失败时调用
    task.done = new RegionClosure(this, request, 
        response, done_guard.release());
    
    task.expected_term = term;

    // task放入raft组，等待执行结果
    return node_->apply(task);
}

void Master::RegionSplit(const PiDBSplitRequest* request,
        PiDBSplitResponse* response, google::protobuf::Closure* done){
    brpc::ClosureGuard done_guard(done);
    // 考虑leader是否变了
    const int64_t term = leader_term_.load(butil::memory_order_relaxed);
    if (term < 0) return Redirect(response);
    
    // 串行化请求到IOBuf中
    butil::IOBuf log;
    log.push_back((uint8_t)RaftOpType::RegionSplitOp_);
    butil::IOBufAsZeroCopyOutputStream wrapper(&log);
    if (!request->SerializeToZeroCopyStream(&wrapper)) {
        LOG(ERROR) << "串行化请求失败";
        response->set_success(false);
        return;
    }
    // Apply this log as a braft::Task
    braft::Task task;
    task.data = &log;
    // 下面的回调会在task启动或失败时调用
    task.done = new SplitClosure(this, request, 
        response, done_guard.release());
    
    task.expected_term = term;

    // task放入raft组，等待执行结果
    return node_->apply(task);
}

void Master::IfSplitHasLeader(void* arg, brpc::Controller* cntl,
        PiDBRaftManageResponse* response){
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    std::unique_ptr<pidb::PiDBRaftManageResponse> response_guard(response);

    Master* master = ((ISHL*)arg)->master_;
    string old_group = ((ISHL*)arg)->old_group_;
    string new_group = ((ISHL*)arg)->new_group_;
    auto rto = master->route_table_ok_, rtb = master->route_table_bad_;
    string new_min_key, new_max_key;   // 新region的
    rtb->GetRange(new_group, new_min_key, new_max_key);

    if (cntl->Failed()) {
        // 重新建立分裂的新raft组
        //master->AddRegion(new_min_key, new_max_key, true, arg);
        LOG(WARNING) << "分裂的新raft选leader失败, " << cntl->ErrorText();
        return;
    }
    // leader选出来了
    else if(response->is_leader()){
        // 开始通知leader拉数据
        string old_conf, new_conf, old_addr;
        string new_addr = response->leader_addr();
        string old_min_key, old_max_key;  // 老region的
        rto->GetRange(old_group, old_min_key, old_max_key);
        rtb->GetConf(new_group, new_conf);

        if(rto->GetConf(old_group, old_conf) && 
                rto->GetAddr(old_group, old_addr)){
            // 更新路由表
            rto->UpdateRouteInfo(old_min_key, new_min_key,
                old_group, old_addr, old_conf, 1);
            rtb->UpdateRouteInfo(new_min_key, new_max_key,
                new_group, new_addr, new_conf, -3);
            // 通知拉数据
            if(!master->rm_->PullData(old_addr, old_group, 
                old_conf, new_conf, new_group)) { 
                // 重新生成新的raft
                LOG(WARNING)<<"某个分裂的新region凉了，新建raft\n"; 
                master->AddRegion(new_min_key, new_max_key, true, arg);
            }
        }
        else LOG(WARNING)<<"某个待分裂的老region凉了，无法解决\n"; 
    }
}

int Master::Start(int port, string conf, string group){
    // raft节点的启动
    butil::EndPoint addr(butil::my_ip(), port);
    braft::NodeOptions node_options;
    if (node_options.initial_conf.parse_from(conf) != 0) {
        LOG(ERROR) << "解析配置失败:" << conf << ';';
        return -1;
    }
    node_options.election_timeout_ms = 5000;
    node_options.fsm = this;
    node_options.node_owns_fsm = false;
    node_options.snapshot_interval_s = 30;
    // raft的日志就放在raft组名同名文件家夹下
    std::string prefix = "local://" + group; 
    node_options.log_uri = prefix + "/log";
    node_options.raft_meta_uri = prefix + "/raft_meta";
    node_options.snapshot_uri = prefix + "/snapshot";
    node_options.disable_cli = false;
    braft::Node* node = new braft::Node(group, braft::PeerId(addr));
    if (node->init(node_options) != 0) {
        LOG(ERROR) << "初始化节点失败";
        delete node;
        return -1;
    }
    node_ = node;
    return 0;
}

void Master::Redirect(PiDBClientResponse* response) {
        response->set_success(false);
    if (node_) {
        braft::PeerId leader = node_->leader_id();
        if (!leader.is_empty()) {
            response->set_redirect(leader.to_string());
        }
    }
}
void Master::Redirect(PiDBStoreResponse* response) {
        response->set_success(false);
    if (node_) {
        braft::PeerId leader = node_->leader_id();
        if (!leader.is_empty()) {
            response->set_redirect(leader.to_string());
        }
    }
}
void Master::Redirect(PiDBRegionResponse* response) {
    response->set_success(false);
    if (node_) {
        braft::PeerId leader = node_->leader_id();
        if (!leader.is_empty()) {
            response->set_redirect(leader.to_string());
        }
    }
}
void Master::Redirect(PiDBSplitResponse* response) {
    response->set_success(false);
    if (node_) {
        braft::PeerId leader = node_->leader_id();
        if (!leader.is_empty()) {
            response->set_redirect(leader.to_string());
        }
    }
}

void Master::DoHandleStore(braft::Iterator& iter){
    PiDBStoreResponse* response = NULL;
    string* store_addr = new string("");
    int region_num = -1;
    int leader_num = -1;

    // 解析参数
    if (iter.done()) {
        // task应用到节点了，从闭包中获取值避免额外解析
        StoreClosure* s = (StoreClosure*)(iter.done());
        response = s->response();
        *store_addr = s->request()->store_addr();
        if (s->request()->has_region_num())
            region_num = s->request()->region_num();
        if (s->request()->has_leader_num())
            leader_num = s->request()->leader_num();
    } 
    else {
        // 否则从日志中解析
        butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
        PiDBStoreRequest request;
        CHECK(request.ParseFromZeroCopyStream(&wrapper));
        *store_addr = request.store_addr();
        if (request.has_region_num())
            region_num = request.region_num();
        if (request.has_leader_num())
            leader_num = request.leader_num();
    }

    // 处理并回复
    if (response) {
        // 若是新的store就放入看门狗测活
        if(!store_table_->UpdateStoreInfo(*store_addr,region_num, leader_num))
            guard_dog_store_->AddThing(StoreHeartbeat::CheckIsAlive, 
                store_addr);
        response->set_success(true);
    }
}

void Master::DoHandleRegion(braft::Iterator& iter){
    PiDBRegionResponse* response = NULL;
    string leader_addr("");  // 发送心跳的leader的地址
    string raft_group("");   // region所在raft组名
    int peer_num = -1;       // region所在raft组副本数（除去；leader）
    std::vector<string> v;   // 副本地址（调度用）
    string conf("");         // region的raft组配置
    
    // 解析参数
    if (iter.done()) {
        // task应用到节点了，从闭包中获取值避免额外解析
        RegionClosure* s = (RegionClosure*)(iter.done());
        response = s->response();
        leader_addr = s->request()->leader_addr();
        raft_group = s->request()->raft_group();
        peer_num = s->request()->peer_addr_size();
        for(int i = 0; i<peer_num; ++i){
            v.push_back(s->request()->peer_addr(i));
            conf = conf + s->request()->peer_addr(i) + ":0";
        }
        v.push_back(leader_addr);
        conf = conf + leader_addr + ":0";
    } 
    else {
        // 否则从日志中解析
        butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
        PiDBRegionRequest request;
        CHECK(request.ParseFromZeroCopyStream(&wrapper));
        leader_addr = request.leader_addr();
        raft_group = request.raft_group();
        peer_num = request.peer_addr_size();
        for(int i =0; i<peer_num; ++i){
            v.push_back(request.peer_addr(i));
            conf = conf + request.peer_addr(i) + ":0";
        }
        v.push_back(leader_addr);
        conf = conf + leader_addr + ":0";
    }

    // 处理并回复
    if (response) {
        // 获取范围，如果是新的region，那么肯定在bad表中有记录
        string min_key, max_key;
        if(!route_table_ok_->GetRange(raft_group, min_key, max_key))
            route_table_bad_->GetRange(raft_group, min_key, max_key);
 
        // 更新路由表
        int state = route_table_bad_->GetState(raft_group);
        // 分裂后的新region选出leader了
        if(state == -2)
            route_table_bad_->UpdateRouteInfo(min_key, max_key,
                raft_group, leader_addr, conf, -3);
        else if(state == -1){
            route_table_bad_->RemoveRouteInfo(raft_group);
            route_table_ok_->UpdateRouteInfo(min_key, max_key,
                raft_group, leader_addr, conf, 0);
        }
        // bad表里没有
        else if(state == -4)
            route_table_ok_->UpdateRouteInfo(min_key, max_key,
                raft_group, leader_addr, conf, 0);

        // 路由表处理好了来处理raft节点的问题（维持在默认最大副本数）
        int need_num = peer_num + 1 - raft_copy_num_;
        std::vector<string> vout;
        // 需要新增
        if(need_num < 0 && store_table_->
                GetStoreInfo(-need_num, &v, &vout, true) == 0){
            for(auto&s : vout) conf = conf + s + ":0";
            for(auto&s : vout)
                rm_->AddNode(raft_group, conf, min_key, max_key, s,
                    false, nullptr, nullptr);
            response->set_success(true);
            response->set_conf(conf);
        }
        // 需要删除
        else if(need_num > 0 && store_table_->
                GetStoreInfo(need_num, &v, &vout, false) == 0){
            for(auto&s : vout) rm_->RemoveNode(raft_group, s);
            conf = "";
            bool flag = false; // 下面的s1是否等于s2
            // 更新配置信息
            for(auto&s1 : v){
                flag = false;
                for(auto&s2 : vout){
                    if(s1 == s2){
                        flag = true;
                        break;
                    }
                }
                if(!flag) conf = conf + s1 + ":0";
            }
            response->set_success(true);
            response->set_conf(conf);
        }
        else if(need_num == 0) response->set_success(true);
        else response->set_success(false);
    }
}

void Master::DoRegionSplit(braft::Iterator& iter){
    PiDBSplitResponse* response = NULL;
    string leader_addr = "";
    string raft_group = "";
    string split_key = "";

    // 解析参数
    if (iter.done()) {
        // task应用到节点了，从闭包中获取值避免额外解析
        SplitClosure* s = (SplitClosure*)(iter.done());
        response = s->response();
        leader_addr = s->request()->leader_addr();
        raft_group = s->request()->raft_group();
        split_key = s->request()->split_key();
    } 
    else {
        // 否则从日志中解析
        butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
        PiDBSplitRequest request;
        CHECK(request.ParseFromZeroCopyStream(&wrapper));
        leader_addr = request.leader_addr();
        raft_group = request.raft_group();
        split_key = request.split_key();
    }

    // 处理并回复
    if (response) {
        // 首先更新路由
        string min_key;
        string max_key;
        route_table_ok_->GetRange(raft_group, min_key, max_key);
        string new_group = split_key + "group";
        route_table_bad_->UpdateRouteInfo(split_key, max_key,
            new_group, "", "", -2);
        
        // 然后通知生成新的raft
        ISHL* arg = new ISHL(this, raft_group, new_group);
        AddRegion(split_key, max_key, true, arg);

        response->set_success(true);
    }
}

void Master::on_apply(braft::Iterator& iter) {
    // 提交的批量任务须经过iter处理
    for (; iter.valid(); iter.next()) {
        // ..guard帮助异步调用iter.done()->Run()以免阻塞状态机
        braft::AsyncClosureGuard closure_guard(iter.done());

        // 解析是哪种操作
        butil::IOBuf data = iter.data();
        uint8_t type = RaftOpType::DefalutOp_;
        data.cutn(&type, sizeof(uint8_t));
        switch (type) {
            case RaftOpType::StoreHeartOp_:{
                DoHandleStore(iter);
                break;
            }
            case RaftOpType::RegionHeartOp_:{
                DoHandleRegion(iter);
                break;
            }
            case RaftOpType::RegionSplitOp_:{
                DoRegionSplit(iter);
                break;
            }
        }
    }
}

void Master::on_leader_start(int64_t term) {
    leader_term_.store(term, butil::memory_order_release);
    LOG(INFO) << "节点成为leader";
}
void Master::on_leader_stop(const butil::Status& status) {
    leader_term_.store(-1, butil::memory_order_release);
    LOG(INFO) << "节点下台(非leader):" << status;
}
void Master::on_shutdown() {
    LOG(INFO) << "节点关闭";
}
void Master::on_error(const ::braft::Error& e) {
    LOG(ERROR) << "发生raft错误" << e;
}
void Master::on_configuration_committed(const ::braft::Configuration& conf) {
    LOG(INFO) << "此组配置信息为：" << conf;
}
void Master::on_stop_following(const ::braft::LeaderChangeContext& ctx) {
    LOG(INFO) << "节点停止后：" << ctx;
}
void Master::on_start_following(const ::braft::LeaderChangeContext& ctx) {
    LOG(INFO) << "节点启动后：" << ctx;
}

void ClientClosure::Run() {
    // Auto delete this after Run()
    std::unique_ptr<ClientClosure> self_guard(this);
    // Repsond this RPC.
    brpc::ClosureGuard done_guard(_done);
    if (status().ok()) return ;
    // Try redirect if this request failed.
    master_->Redirect(this->_response);
}
void StoreClosure::Run() {
    // Auto delete this after Run()
    std::unique_ptr<StoreClosure> self_guard(this);
    // Repsond this RPC.
    brpc::ClosureGuard done_guard(done_);
    if (status().ok()) return ;
    // Try redirect if this request failed.
    this->master_->Redirect(this->response_);
}
void RegionClosure::Run() {
    // Auto delete this after Run()
    std::unique_ptr<RegionClosure> self_guard(this);
    // Repsond this RPC.
    brpc::ClosureGuard done_guard(done_);
    if (status().ok()) return ;
    // Try redirect if this request failed.
    this->master_->Redirect(this->response_);
}
void SplitClosure::Run() {
    // Auto delete this after Run()
    std::unique_ptr<SplitClosure> self_guard(this);
    // Repsond this RPC.
    brpc::ClosureGuard done_guard(done_);
    if (status().ok()) return ;
    // Try redirect if this request failed.
    this->master_->Redirect(this->response_);
}
}
