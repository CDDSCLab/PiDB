#include <memory>
#include "raftnode.h"
#include "server.h"
#include "pidb/options.h"
#include "leveldb/write_batch.h"
#include <bthread/bthread.h>
#include <brpc/channel.h>
#include <braft/route_table.h>
namespace pidb {

    RaftNode::RaftNode(const RaftOption &option, const Range &range)
            : group_(std::move(option.group)),
            port_(option.port),
            conf_(std::move(option.conf)),
            data_path_(std::move(option.data_path)),
            leader_term_(-1)
            {

        SetRange(range.start, range.limit);

    }

    Status RaftNode::start() {
        butil::EndPoint addr(butil::my_ip(), port_);
        braft::NodeOptions node_options;
        if (node_options.initial_conf.parse_from(conf_) != 0) {
            LOG(ERROR) << "Fail to parse configuration"<<conf_;
            return Status::OK();
        }
        LOG(INFO)<<node_options.initial_conf;
        node_options.election_timeout_ms = 5000;
        node_options.fsm = this;
        node_options.node_owns_fsm = false;
        node_options.snapshot_interval_s = 30; //30*60s
        std::string prefix = "local://" + group_;
        node_options.log_uri = prefix + "/log";
        node_options.raft_meta_uri = prefix + "/raft_meta";
        node_options.snapshot_uri = prefix + "/snapshot";
        node_options.disable_cli = false;
        auto idx =atoi(&(*(group_.end()-1)));

        braft::Node *node = new braft::Node(group_, braft::PeerId(addr,idx));
        if (node->init(node_options) != 0) {
            LOG(ERROR) << "Fail to init raft node";
            delete node;
            node = nullptr;
            return Status::InvalidArgument(group_, "Fail to init node");
        }
        node_ = node;

        //backup data
        backup_data();
        //start timer
//        raft_timer_.init(shared_from_this(),2000);

        return Status::OK();
    }

    void RaftNode::redirect(PiDBResponse *response) {
        response->set_success(false);
        if (node_) {
            braft::PeerId leader = node_->leader_id();
            if (!leader.is_empty()) {
                response->set_redirect(leader.to_string());
            }
        }
    }

    Status RaftNode::do_put_or_del(uint8_t type, const butil::IOBuf &data, braft::Closure *done) {
        assert(type == kPutOp || type == kDeleteOP);
        PiDBResponse *response = NULL;
        //This task is applied by this node
        std::string key, value;
        if (done) {
            RaftNodeClosure *c = dynamic_cast<RaftNodeClosure *>(done);
            response = c->response();
            key = c->request()->key();
            if (type == kPutOp)
                value = c->request()->value();
        } else {
            butil::IOBufAsZeroCopyInputStream wrapper(data);
            //put or delete的request
            PiDBRequest request;
            CHECK(request.ParseFromZeroCopyStream(&wrapper));
            key = request.key();
            if (type == kPutOp)
                value = request.value();
        }
        auto db = db_->db();
        assert(db != nullptr);
        leveldb::Status s;
        if (type == kPutOp)
            s = db->Put(leveldb::WriteOptions(), key, value);
        else
            s = db->Delete(leveldb::WriteOptions(), key);
        LOG(INFO) << s.ToString();
        //fail to put value
        if (!s.ok()) {
            if (response) {
                response->set_success(false);
            }
            //closure_guard.release();
            // TO-DO which error happened and what should be done.
            return Status::IOError("DB", "Fail to put value into db");
        }

        if (response) {
            response->set_success(true);
        }
        return Status::OK();

    }

    Status RaftNode::do_write(uint8_t type, const butil::IOBuf &data, braft::Closure *done) {
        braft::AsyncClosureGuard closure_guard(done);
        assert(type == kWriteOp);
        leveldb::WriteBatch batch;
        ServerClosure *s = done ? dynamic_cast<ServerClosure *>(done) : nullptr;

        PiDBWriteBatch writeBatch;
        butil::IOBufAsZeroCopyInputStream wrapper(data);
        CHECK(writeBatch.ParseFromZeroCopyStream(&wrapper));
        for (int i = 0; i < writeBatch.writebatch_size(); i++) {
            auto b = writeBatch.writebatch(i);
            //判断operator类型，并放入相应的batch里面
            switch (b.op()) {
                case kPutOp: {
                    batch.Put(b.key(), b.value());
                    break;
                }
                case kDeleteOP: {
                    batch.Delete(b.key());
                    break;
                }
                default:
                    LOG(ERROR) << "Unknown operation";
                    break;
            }
        }

        auto db = db_->db();
        assert(db != nullptr);

        auto status = db->Write(leveldb::WriteOptions(), &batch);
        // TODO 这里涉及多个raft的操作,存在并发等问题
        if (status.ok()) {
            if (done) {
                s->SetDone(group_);
                s->s_ = Status::OK();

                if (!s->IsDone()) {
                    closure_guard.release();
                }
            }
        } else {
            s->s_ = Status::Corruption("raft", "Fail to Write");
        }

        return Status::OK();
    }

    void RaftNode::on_apply(braft::Iterator &iter) {
//TO-DO
        for (; iter.valid(); iter.next()) {
            // This guard helps invoke iter.done()->Run() asynchronously to
            // avoid that callback blocks the StateMachine
            braft::AsyncClosureGuard closure_guard(iter.done());
            butil::IOBuf data = iter.data();
            uint8_t type = kUnknownOp;
            data.cutn(&type, sizeof(uint8_t));
            switch (type) {
                case kDeleteOP:
                case kPutOp: {
                    auto s = do_put_or_del(type, data, iter.done());
                    if (!s.ok())
                        LOG(ERROR) << "Fail to apply put operation" << s.ToString();
                    break;
                }
                case kWriteOp: {
                    closure_guard.release();
                    auto s = do_write(type, data, iter.done());
                    if (!s.ok())
                        LOG(ERROR) << "Fail to apply put operation" << s.ToString();
                    break;
                }
                default:
                    LOG(ERROR) << "Unknown operation typpe";
                    break;
            }
        }
    }

    int RaftNode::link_overwrite(const char* old_path, const char* new_path) {
        if (::unlink(new_path) < 0 && errno != ENOENT) {
            PLOG(ERROR) << "Fail to unlink " << new_path;
            return -1;
        }
        return ::link(old_path, new_path);
    }

    void *RaftNode::save_snapshot(void *arg) {
        //获得handle
        SnapshotHandle *sh = static_cast<SnapshotHandle *> (arg);
        std::unique_ptr<SnapshotHandle> arg_guard(sh);
        brpc::ClosureGuard done_guard(sh->done);
       // auto db = sh->db->db();
        //auto start = node_->ra
        std::string data_path = sh->data_path;
//        std::string snapshot_path = sh->writer->get_path() + "/region_data";
//        LOG(INFO)<<data_path;
//       //auto status = db->DumpRange(data_path,"","");
//       status = Status::OK();
//       assert(db!= nullptr);
//       // butil::WriteFile(butil::FilePath(data_path),data_path.c_str(),3);
//        //auto status = Status::OK();
//        LOG(INFO)<<status.ok();
//        if(!status.ok()){
//            LOG(ERROR)<<status.ToString();
//            sh->done->status().set_error(EIO,"Fail to dump snapshot range %m");
//            return NULL;
//        }
//        if(link_overwrite(data_path.c_str(),snapshot_path.c_str())!=0) {
//            sh->done->status().set_error(EIO,"Fail to link snapshot to %s  %m",data_path.c_str());
//            return NULL;
//        }
//
//        if (sh->writer->add_file("region_data") != 0) {
//            sh->done->status().set_error(EIO, "Fail to add file to writer");
//            return NULL;
//        }
//        LOG(INFO)<<"SUCCESS";
        return NULL;
    }

    void RaftNode::on_snapshot_save(braft::SnapshotWriter *writer, braft::Closure *done) {
        //SnapshotHandle *arg = new SnapshotHandle;
        brpc::ClosureGuard done_guard(done);
        LOG(INFO)<<"SAVE SNAPSHOT ----------------------------";
//        arg->db = db_;
//        arg->writer = writer;
//        arg->done = done;
//        arg->range = &range_;
//        arg->data_path = data_path_;
//        //bthread_t tid;
        //放到后台线程里面执行,urgent保证当前线程会被马上调度
        //bthread 文档：Use this function when the new thread is more urgent.
       // bthread_start_urgent(&tid, NULL, save_snapshot, arg);
    }

    int RaftNode::on_snapshot_load(braft::SnapshotReader *reader) {
        //TO-DO
        //Load snopashot, 如果我们操作的文件（通过link过去的文件）被其他占用,会怎么杨？
//        LOG(INFO)<<"load SNAPSHOT ----------------------------";
//        CHECK(!is_leader())<<"Leader is not supposed to load snapshot";
//        if(reader->get_file_meta("region_data",NULL)!=0){
//            LOG(ERROR)<<"Fail to find data on "<<reader->get_path();
//            return  -1;
//        }
//        std::string snapshot_path = reader->get_path() + "/region_data";
//        std::string data_path =  data_path_;
//
//        if (link_overwrite(snapshot_path.c_str(), data_path.c_str()) != 0) {
//            PLOG(ERROR) << "Fail to link data";
//            return -1;
//        }
//
//        std::string old_data_path = data_path_+".old";
//        auto db = db_->db();
//        assert(db!=nullptr);
//        std::string start,end;
//        leveldb::Status s;
//        if(butil::PathExists(butil::FilePath(old_data_path))){
//            s = db->IngestRanges(old_data_path,data_path);
//        }else{
//           // s = db->LoadRange(data_path,&start,&end);
//        }
//
//        LOG(ERROR)<<"Load snapshot";
//        if(!s.ok()){
//            LOG(ERROR)<<s.ToString();
//            return  -1;
//        }
//
//        LOG(INFO) << "ON_SNAPSHOT_LOAD";
        return 0;
    }

    void RaftNode::on_leader_start(int64_t term) {
//        if(done!= nullptr && leader_term_<0)
//            brpc::ClosureGuard self_guard(done);
        LOG(INFO)<<"Become Leader";
        leader_term_.store(term, std::memory_order_release);
        raft_timer_.init(this,2000);
        raft_timer_.start();
        bthread_t tid;
        std::string key = "n";
//        bthread_start_urgent(&tid,NULL,RaftNode::RequestSplit,&key);


    }

    void RaftNode::on_shutdown() {
        //TO-DO
        LOG(INFO) << "on_shutdown";
    }

    void RaftNode::on_error(const ::braft::Error &e) {
        LOG(ERROR) << "raft error";
    }

    void RaftNode::on_configuration_committed(const ::braft::Configuration &conf) {
        LOG(INFO) << "Configuration of this group is " << conf;
    }

    void RaftNode::on_stop_following(const ::braft::LeaderChangeContext &ctx) {
        LOG(INFO) << "Node stops following " << ctx;

    }
    void RaftNode::on_start_following(const ::braft::LeaderChangeContext &ctx) {
//        if(done != nullptr && leader_term_<0)
//            brpc::ClosureGuard self_guard(done);

        //在变为follower之前需要将之前的snapshot备份，为了后面的增量存储
        LOG(INFO) << "Node start following " << ctx;
        backup_data();
    }

    //当raft成为leader或者follower后需要回调server传过来的方法
    void RaftNode::on_role_change() {
        if(role_change == nullptr) return;

    }

    void RaftNode::backup_data() {

        //TODO 加入对比
        LOG(INFO)<<"back up data";
        std::string data_path = data_path_;
        std::string old_data_path = data_path_+".old";

        if (butil::PathExists(butil::FilePath(data_path))){
            LOG(INFO)<<"Find data at "<<data_path;
            if(butil::CopyFile(butil::FilePath(data_path),butil::FilePath(old_data_path))){
                LOG(INFO)<<"COPY SUCCESS";
            } else{
                LOG(INFO)<<"COPY FAILED";
            }

        }
    }
    // end of @braft::StateMachine
//TO-DO check option

    void RaftNodeClosure::Run() {
        std::unique_ptr<RaftNodeClosure> self_guard(this);
        //用户closure
        brpc::ClosureGuard done_guard(done_);
        if (status().ok()) {
            LOG(INFO)<<"DDD";
            return;
        }
        node_->redirect(response_);
    }

    Status RaftNode::Get(const PiDBRequest *request, PiDBResponse *response,
                         ::google::protobuf::Closure *done) const {
        if (!IsLeader()) {
            // return redirect(response);
            //当前不是leader 不能直接转发请求，需要提示用户刷新本地缓存。
            return Status::Corruption(group_, "not leader");
        }
        //获得db_
        auto db = db_;
        std::string value;
        auto s = db_->db()->Get(leveldb::ReadOptions(), request->key(), &value);
        if (!s.ok()) {
            LOG(ERROR) << "Fail to read from db";
            response->set_success(false);
            return Status::Corruption(group_, "Fail to read from db");
        }
        response->set_new_value(value);
        response->set_success(true);
        return Status::OK();
    }

    void RaftNode::Put(const PiDBRequest *request, PiDBResponse *response,
                       google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        const int64_t term = leader_term_.load(std::memory_order_relaxed);
        if (term < 0) {
            redirect(response);
        }
        butil::IOBuf log;
        log.push_back((uint8_t) kPutOp);
        butil::IOBufAsZeroCopyOutputStream wrapper(&log);
        if (!request->SerializeToZeroCopyStream(&wrapper)) {
            LOG(ERROR) << "Fail to serialize request";
            response->set_success(false);
        }

        // Apply this log as a braft::Task

        braft::Task task;
        task.data = &log;
        // This callback would be iovoked when the task actually excuted or
        // fail
        task.done = new RaftNodeClosure(this, request, response, done_guard.release());

        // ABA problem can be avoid if expected_term is set
        task.expected_term = term;

        // Now the task is applied to the group, waiting for the result.
        return node_->apply(task);
    }

//Write 操作
    void RaftNode::Write(const leveldb::WriteOptions &options, std::unique_ptr<PiDBWriteBatch> batch,
                         braft::Closure *done) {

        auto term = leader_term_.load(std::memory_order_relaxed);
        //TODO 分不同的region处理，需要记录
        if (term < 0) {
            //TODO
        }
        butil::IOBuf log;
        log.push_back((uint8_t) kWriteOp);
        butil::IOBufAsZeroCopyOutputStream wrapper(&log);
        if (!batch->SerializeToZeroCopyStream(&wrapper)) {
            LOG(ERROR) << "Fail to seralize batch request";
            return;
        }
        braft::Task task;
        task.data = &log;
        task.done = done;
        task.expected_term = term;
        return node_->apply(task);

    }

    void RaftNode::HandleHeartbeat() {
        //TODO 放入节点角色变化去取消timer
        if(!is_leader()) return;

        #
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
        pidb::PiDBRegionRequest request;
        pidb::PiDBRegionResponse response;
        std::string address= "127.0.1.1:"+std::to_string(port_);
        request.set_leader_addr(address);
//        auto s = request.add_peer_addr();
//         s = new std::string(address);
       // request.add_peer_addr(address);
        request.set_raft_group(group_);
        //request.set_peer_addr(0,address);
        LOG(INFO)<<group_;
        stub.RegionHeartbeat(&cntl,&request,&response,NULL);
        if(!cntl.Failed()){
            LOG(INFO)<<"response"<<response.success();
        } else{
            LOG(INFO)<<cntl.ErrorText();
        }

        LOG(INFO)<<"RAFT HEARBET";
        if(count_==5 && group_=="group")
            RequestSplit(this);
        count_++;
    }

    void* RaftNode::RequestSplit(void *arg) {

        LOG(INFO)<<"Request Split";

        auto raft = (reinterpret_cast<RaftNode *>(arg));
        if(!raft->is_leader()) return nullptr;
        usleep(1000*1000);
        //init channel
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

        pidb::PiDBSplitRequest request;
        pidb::PiDBSplitResponse response;


        std::string address= "127.0.1.1:"+std::to_string(raft->port_);
        request.set_leader_addr(address);
        LOG(INFO)<<address;
        request.set_raft_group(raft->group_);
        request.set_split_key("b");

        LOG(INFO)<<raft->group_;
        stub.RegionSplit(&cntl,&request,&response,NULL);
        if(!cntl.Failed()){
            LOG(INFO)<<"response"<<response.success();
        } else{
            LOG(INFO)<<cntl.ErrorText();
        }
        return nullptr;
    }

    int RaftTimer::init(RaftNode* raft, int timeout_ms) {
        //调用父类的init函数进行初始化

        BRAFT_RETURN_IF(RepeatedTimerTask::init(timeout_ms) != 0, -1);
        raft_ = raft;
        return 0;
    }
    void RaftTimer::on_destroy() {
//        if(raft_!= nullptr){
//            raft_.reset();
//            raft_= nullptr;
//        }
    }

    void RaftHeartbeatTimer::run() {
        raft_->HandleHeartbeat();
    }
} // namespace pidb