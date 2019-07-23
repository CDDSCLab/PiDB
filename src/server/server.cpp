// node server for controlling raft node
#include "server.h"
#include <sstream>
#include <leveldb/db.h>   //leveldb
#include <leveldb/write_batch.h>
#include <butil/logging.h>
#include <butil/time.h>
#include <brpc/channel.h>
#include <braft/raft.h>
#include <braft/util.h>
#include <braft/route_table.h>
#include "master.pb.h"
#include "raftnode.h"
#include <gflags/gflags.h>
//DEFINE_string(config, "127.0.1.1:8300:0,127.0.1.1:8301:0,127.0.1.1:8302:0", "Carry this along with requests");


namespace pidb {

    Server::Server(const ServerOption &serveroption) : port_(serveroption.port),
                                                       data_path_(std::move(serveroption.data_path))
                                                       {
        option_ = serveroption;
        //TO-DO recover from log
        //启动本地的raftnode，如果没有则初始化一个      
        //raft 和 server同属一个端口 
        //这只是一个实例，后面可能需调整优化
 //       auto option = RaftOption();
        //raft 和server共享一个rpc
//        option.port = serveroption.port;
//        option.group = "group1";
//        option.conf="127.0.1.1:8100:0,127.0.1.1:8101:0,127.0.1.1:8102:0";
//        option.conf = "127.0.1.1:8100:0";
//        option.data_path = serveroption.data_path+"/group1";
//        auto s = registerRaftNode(option,Range("",""));
//        if (!s.ok()) {
//            LOG(INFO) << "Fail to add raft node";
//        }
    }

    Status Server::registerRaftNode(const RaftOption &option,const Range &range) {
        if (nodes_.find(option.group) != nodes_.end()) {
            std::ostringstream s;
            s << "There is alreay existing raftnode in" << option.group;
            return Status::Corruption(option.group, s.str());
        }

        auto raftnode = std::make_shared<RaftNode>(option, range);
        raftnode->SetDB(db_);

        auto status = raftnode->start();
        if (!status.ok()){
            LOG(ERROR)<<"Fail to start raftnode group:"<<option.group;
            return status;
        }
        //成功开启加入nodes
        nodes_[option.group] = raftnode;
        //判断一下是否当前map里面没有同样id的raft

        return status;
    }

    Status Server::registerRaftNode(const pidb::RaftOption &option, const pidb::Range &range,
                                    ::pidb::PiDBRaftManageResponse *response, ::google::protobuf::Closure *done) {

        if (nodes_.find(option.group) != nodes_.end()) {
            std::ostringstream s;
            s << "There is alreay existing raftnode in" << option.group;
            return Status::Corruption(option.group, s.str());
        }

        auto raftnode = std::make_shared<RaftNode>(option, range);
        raftnode->SetDB(db_);

        google::protobuf::Closure* raft_done = brpc::NewCallback(
                StartRaftCallback,response,done,raftnode);
        //raftnode->done = raft_done;

        auto status = raftnode->start();

        if (!status.ok()){
            LOG(ERROR)<<"Fail to start raftnode group:"<<option.group;
            return status;
        }
        //成功开启加入nodes
        nodes_[option.group] = raftnode;
        //判断一下是否当前map里面没有同样id的raft

        return status;

    }

    //打开leveldb
    //加载配置，遍历nodes_里的raft 把他们全部启动起来
    Status Server::Start() {

        //开启leveldb
        std::string db_path = data_path_ + "/database";
        leveldb::DB *db;
        leveldb::Options options;
        options.create_if_missing = true;
        auto status = leveldb::DB::Open(options, db_path, &db);

        if (!status.ok()) {
            LOG(ERROR) << "Fail to open db";
            return Status::Corruption(db_path, "Fail to open db");
        }
        db_ = new SharedDB(db);

        //可能存在部分节点启动失败，暂时返回OK
        //LoadNdes (从当前的配置中加载Nodes,因为Server可能从宕机中重启，需要恢复raft)
//        for (auto const n:nodes_) {
//            //在启动前设置共享的db,而不是在初始化的时候
//            n.second->SetDB(db_);
//            if (!n.second->start().ok()) {
//                LOG(ERROR) << "Fail to start" << n.first << "node";
//                //TO-DO 是否记录失败信息，重试？
//                return Status::Corruption(n.first, "Fail to start");
//            }
//        }

        //start hearbeat timer
       // auto self(shared_from_this());
        hearbeat_timer_.init(shared_from_this(),option_.heartbeat_timeout_ms);
        hearbeat_timer_.start();
        return Status::OK();
    }

    Status Server::Stop() {
        for (const auto n:nodes_) {
            //TO-DO 判断节点信息
            n.second->shutdown();
        }
        //server_.Stop(0);
        for (const auto n:nodes_) {
            n.second->join();
        }
        //server_.Join();
        hearbeat_timer_.destroy();
        nodes_.clear();
        return Status::OK();
    }


    void Server::Put(const ::pidb::PiDBRequest *request,
                     ::pidb::PiDBResponse *response,
                     ::google::protobuf::Closure *done) {
        //put 操作需要更新到raft上，
        //选择正确的node，更新
        //TO-DO使用路由表去查找
        auto key = request->key();
        LOG(INFO) << key;

        //TODO auto node = SelectNodde(key) 并判断是否合法
        auto node = nodes_.begin();

        //为了测试取第一个
        assert(node != nodes_.end());

        //TO-DO 异步的方式
        assert(node->second.get()!= nullptr);
        node->second.get()->Put(request, response, done);

    }

    Status Server::Get(const ::pidb::PiDBRequest *request,
                       ::pidb::PiDBResponse *response,
                       ::google::protobuf::Closure *done) {
        //因为是共享db，所以直接得到结果
        scoped_db db = db_;
        std::string value;
        leveldb::ReadOptions read_options;
        LOG(INFO)<<request->has_snapshot();
        LOG(INFO)<<"GET";
        if (request->has_snapshot()){
            LOG(INFO)<<"GET FROM SNAPSHOT";
            auto snapshot_ptr = snapshots_.Get(request->snapshot().id());
            if (snapshot_ptr== nullptr){
                response->set_success(false);
                return Status::InvalidArgument("Get","Snapshot is not existed");
            }
            read_options.snapshot = snapshot_ptr->Get();
        }

        auto s = db_->db()->Get(read_options, request->key(), &value);
        if (!s.ok()) {
            LOG(ERROR) << "Fail to get key from db"<<s.ToString();
            return Status::Corruption(request->key(), "Fail to get key from db");
        }
        response->set_new_value(std::move(value));
        response->set_success(true);
        return Status::OK();
    }

    void Server::Write(const ::pidb::PiDBWriteBatch *request,
                       ::pidb::PiDBResponse *response,
                       ::google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        //根据batch 的内容进行转发到不同的region上面写
        assert(request->writebatch_size() > 0);
        //std::unordered_map<std::string,std::unique_ptr<leveldb::WriteBatch>> batchs;
        std::unordered_map<std::string, std::unique_ptr<PiDBWriteBatch>> batchs;

        //遍历write_batch ,分发到不同的region,传给每个region一个writebatch的请求
        //因为需要序列化，所以直接使用PiDBWritebatch
        std::vector<std::string> groups;
        for (int i = 0; i < request->writebatch_size(); i++) {
            auto batch = request->writebatch(i);
            //为了测试该region为group1
            //auto group = FindGroup(batch.key());
            auto group = "group1";
            //当前group还没有batch
            if (batchs.find(group) == batchs.end()) {
                batchs[group] = std::unique_ptr<PiDBWriteBatch>(new PiDBWriteBatch());
                groups.push_back(group);
            }

            auto op = batchs[group]->add_writebatch();
            op->set_op(batch.op());
            op->set_key(std::move(batch.key()));
            op->set_value(std::move(batch.value()));
//            if(batch.op()==RaftNode::kPutOp)
//                op->set_value(std::move(batch.value()));
//
//             switch(batch.op()){
//                 case RaftNode::kPutOp:
//                     //TODO 获得key值,找到region 的id,并放入其batch中
//                     batchs[group]->P
//                     break;
//                 case RaftNode::kDeleteOP:
//                     batchs[group]->Delete(batch.key());
//                     break;
//                 //不属于操作范围，直接break,其他的操作不受影响
//                 default:
//                     LOG(INFO)<<"Write batch unknown operation";
//                     break;
//            }// switch
        } // for write_batch

        ServerClosure *closure = new ServerClosure(response, done_guard.release(), std::move(groups));
        //TODO 异步调用
        for (auto iter = batchs.begin(); iter != batchs.end(); iter++) {

            auto node = nodes_.begin();
            assert(node != nullptr);
            //因为write 操作可能涉及多个rfat的操作,所以不能直接将response交给raft执行，需要
            //server端 收集所有涉及操作的region的信息。
            //TODO 可否采用并发执行，不考虑其执行顺序？

            node->second.get()->Write(leveldb::WriteOptions(), std::move(iter->second), closure);
        }
        response->set_success(true);
    }

    int64_t Server::GetSnapshot() {
        scoped_db db = db_;
        assert(db->db() != nullptr);
//         if(db.get()== nullptr)
//         {
//             LOG(INFO)<<"DB is not open";
//             return 0;
//         }
        std::unique_ptr<pidb::SnapshotContext> s(new pidb::SnapshotContext(db->db()->GetSnapshot()));
        auto id = snapshots_.Put(std::move(s));

        return id;
    }


    Status Server::ReleaseSnapshot(int64_t id) {
        auto snapshot = snapshots_.Get(id);
        //从数据库真实释放snapshot
        auto db = db_->db();
        db->ReleaseSnapshot(snapshot.get()->Get());
        bool success = snapshots_.Erase(id);
        if (!success) {
            std::ostringstream stream;
            stream << "There is not snapshot indicated at " << id;
            return Status::InvalidArgument("Snapshot", stream.str());
        }
        LOG(INFO)<<"RELEASE OK";
        return Status::OK();

    }

    int64_t Server::GetIterator(const std::string &start, const std::string &end) {
        //不需要使用mutex,因为通过id来保证了iterator唯一，且leveldb的getiterator也保证了并发
        auto db = db_->db();
        assert(db != nullptr);
        //TODO 优化iterator的使用.
        // 如当前server有多个region不相连,start属于group1,此时iterator属于group1
        // 当iterator移动到group1且还没到end,需要切换iterator到下一个group,
        //并且此时需要通知用户当前已经到此region结尾,让用户切换到其他的region（此时的region可能属于其他server）
        auto it = std::unique_ptr<leveldb::Iterator>(db->NewIterator(leveldb::ReadOptions()));
        //auto group = selectGroup(start)
        auto group = "group1";

        it->Seek(start);
        assert(it->Valid());
        //是不是需要优化以下阿
        auto id = iterators_.Put(std::unique_ptr<IteratorContext>(
                new IteratorContext(std::move(it), group)));

        return id;

    }

    Status Server::Next(int64_t id, std::string *value) {
        auto it = iterators_.Get(id);
        if(it == nullptr){
            std::ostringstream s;
            s<<"There is not iterator indicated at"<<id;
            return Status::InvalidArgument("Iterator", s.str());
        }
        auto iterator = it->Get();
        if(iterator->Valid()){
            *value = iterator->value().ToString();
            //TODO 判断当前的value是否为当前region的最后
        }else{
            return Status::Corruption("Iterate","Iterator is invalid");
        }
        iterator->Next();
        return Status::OK();
    }

    Status Server::ReleaseIterator(int64_t id) {
        auto it = iterators_.Get(id);
        if(it == nullptr) {
            std::ostringstream s;
            s<<"There is not iterator indicated at"<<id;

            return Status::InvalidArgument("Iterator", s.str());
        }
        if(!iterators_.Erase(id)){
            return Status:: Corruption("Iterator","Fail to erase");
        }
        return Status::OK();
    }
    void Server::HandleHeartbeat() {
        //TO
//        for (const auto & node:nodes_){
//            LOG(INFO)<<node.first;
//        }
        brpc::Channel channel;
        braft::PeerId leader;


        if (braft::rtb::update_configuration("master", "127.0.1.1:8300:0,127.0.1.1:8301:0,127.0.1.1:8302:0") != 0) {
            LOG(ERROR) << "Fail to register configuration " << "127.0.1.1:8300:0,127.0.1.1:8301:0,127.0.1.1:8302:0"
                       << " of group " << "master";
        }
        if (braft::rtb::select_leader("master", &leader) != 0) {
            butil::Status st = braft::rtb::refresh_leader(
                    "master", 500);
            if (!st.ok()) {
                LOG(WARNING) << "Fail to refresh_leader : " << st;
                bthread_usleep(500 * 1000L);
            }
        }

        if (braft::rtb::select_leader("master", &leader) != 0) {
            butil::Status st = braft::rtb::refresh_leader(
                    "master", 500);
            if (!st.ok()) {

                LOG(WARNING) << "Fail to refresh_leader : " << st;
                bthread_usleep(500 * 1000L);
            }
        }

        // Initialize the channel, NULL means using default options.
        brpc::ChannelOptions options;
        options.protocol = "baidu_std";
        options.connection_type = "";
        options.timeout_ms = 1000/*milliseconds*/;
        options.max_retry = 3;
        if (channel.Init(leader.addr, &options) != 0) {
            LOG(ERROR) << "Fail to initialize channel";
        }
        pidb::MasterService_Stub stub(&channel);
        int log_id = 0;
        brpc::Controller cntl;
        pidb::PiDBStoreResponse response;
        pidb::PiDBStoreRequest request;
        request.set_leader_num(0);
        std::string address= "127.0.1.1:"+std::to_string(port_);
        request.set_store_addr(address);
        request.set_region_num(0);
        stub.StoreHeartbeat(&cntl,&request,&response,NULL);
        if(!cntl.Failed()){
            LOG(INFO)<<"Get Key:"<<response.success();
        } else{
            LOG(INFO)<<cntl.ErrorText();
        }

        LOG(INFO)<<"HEARBET";
    }


    ServerClosure::ServerClosure(pidb::PiDBResponse *response, google::protobuf::Closure *done,
                                 std::vector<std::string> groups)
            : response_(response),
              done_(done) {
        for (const auto &g:groups) {
            batchs[g] = false;
        }

    }

    void ServerClosure::SetDone(const std::string &group) {
        if (batchs.find(group) == batchs.end())
            return;

        batchs[group] = true;
        count_.fetch_add(1, std::memory_order_release);

    }

    void ServerClosure::Run() {
        std::unique_ptr<ServerClosure> self_guard(this);
        brpc::ClosureGuard done_guard(done_);
        auto res = response();
        if (!status().ok() || s_.ok() || !IsDone()) {

            res->set_success(false);
        } else {
            res->set_success(true);
            LOG(INFO)<<"WRITE SUCCESS";
        }
    }

    bool ServerClosure::IsDone() {
        auto c = count_.load(std::memory_order_acquire);
        return c >= batchs.size();
    }


    int ServerTimer::init(std::shared_ptr<pidb::Server> server, int timeout_ms) {
        //调用父类的init函数进行初始化
        BRAFT_RETURN_IF(RepeatedTimerTask::init(timeout_ms) != 0, -1);
        server_ = server;
        return 0;
    }
    void ServerTimer::on_destroy() {
        if(server_!= nullptr){
            server_.reset();
            server_= nullptr;
        }
    }

    void HeartbeatTimer::run() {
        server_->HandleHeartbeat();
    }

    void Server::StartRaftCallback(::pidb::PiDBRaftManageResponse *response, ::google::protobuf::Closure *done,std::shared_ptr<RaftNode> raft) {
        LOG(INFO)<<"Raft Callback is leader"<<raft->is_leader();
        brpc::ClosureGuard self_guard(done);
        response->set_is_leader(raft->is_leader());
    }
    void Server::HandleRaftManage(const ::pidb::PiDBRaftManageRequest *request, ::pidb::PiDBRaftManageResponse *response,
                                  ::google::protobuf::Closure *done) {
        pidb::RaftOption option;
        option.port = port_;
        option.group = request->raft_group();
        option.conf = request->raft_conf();
        option.data_path ="./"+option.group;
        //brpc::ClosureGuard guard_release(done);

        auto status= registerRaftNode(option,pidb::Range(request->min_key(),request->max_key()));

        LOG(INFO)<<"register RaftNode :"<<status.ToString()<<option.conf<<" "<<request->raft_group();

    }


} //  pidb
