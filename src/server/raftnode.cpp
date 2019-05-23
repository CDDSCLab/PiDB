#include <memory>
#include "raftnode.h"
#include "server.h"
#include "pidb/options.h"
#include "leveldb/write_batch.h"

namespace pidb {

    RaftNode::RaftNode(const RaftOption &option, const Range &range)
            : group_(std::move(option.group)),
            port_(option.port),
            conf_(std::move(option.conf)),
            leader_term_(-1),
            data_path_(std::move(option.data_path))
            {

        SetRange(range.start, range.limit);

    }

    Status RaftNode::start() {
        butil::EndPoint addr(butil::my_ip(), port_);
        braft::NodeOptions node_options;
        if (node_options.initial_conf.parse_from(conf_) != 0) {
            LOG(ERROR) << "Fail to parse configuration";
            return Status::OK();
        }
        node_options.election_timeout_ms = 5000;
        node_options.fsm = this;
        node_options.snapshot_interval_s = 30;
        std::string prefix = "local://./" + group_;
        node_options.log_uri = prefix + "/log";
        node_options.raft_meta_uri = prefix + "/raft_meta";
        node_options.snapshot_uri = prefix + "/snapshot";
        node_options.disable_cli = false;
        braft::Node *node = new braft::Node(group_, braft::PeerId(addr));
        if (node->init(node_options) != 0) {
            LOG(ERROR) << "Fail to init raft node";
            delete node;
            node = nullptr;
            return Status::InvalidArgument(group_, "Fail to init node");
        }
        node_ = node;
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
            PiDBResponse *response = NULL;
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
        auto db = sh->db->db();
        //auto start = node_->ra
        std::string data_path = sh->data_path;
        std::string snapshot_path = sh->writer->get_path() + "/region_data";

        auto status = db->DumpRange(data_path,sh->range->start,sh->range->limit);
       // butil::WriteFile(butil::FilePath(data_path),data_path.c_str(),3);
        //auto status = Status::OK();
        if(!status.ok()){
            LOG(ERROR)<<status.ToString();
            sh->done->status().set_error(EIO,"Fail to dump snapshot range %m");
            return NULL;
        }
        if(link_overwrite(data_path.c_str(),snapshot_path.c_str())!=0) {
            sh->done->status().set_error(EIO,"Fail to link snapshot to %s  %m",data_path.c_str());
            return NULL;
        }

        if (sh->writer->add_file("region_data") != 0) {
            sh->done->status().set_error(EIO, "Fail to add file to writer");
            return NULL;
        }
        return NULL;
    }

    void RaftNode::on_snapshot_save(braft::SnapshotWriter *writer, braft::Closure *done) {
        SnapshotHandle *arg = new SnapshotHandle;
        arg->db = db_;
        arg->writer = writer;
        arg->done = done;
        arg->range = &range_;
        arg->data_path = data_path_;
        bthread_t tid;
        //放到后台线程里面执行,urgent保证当前线程会被马上调度
        //bthread 文档：Use this function when the new thread is more urgent.
        bthread_start_urgent(&tid, NULL, save_snapshot, arg);
    }

    int RaftNode::on_snapshot_load(braft::SnapshotReader *reader) {
        //TO-DO
        //Load snopashot, 如果我们操作的文件（通过link过去的文件）被其他占用,会怎么杨？
        CHECK(!is_leader())<<"Leader is not supposed to load snapshot";
        if(reader->get_file_meta("region_data",NULL)!=0){
            LOG(ERROR)<<"Fail to find data on "<<reader->get_path();
            return  -1;
        }
        std::string snapshot_path = reader->get_path() + "/region_data";
        std::string data_path =  data_path;
        if (link_overwrite(snapshot_path.c_str(), data_path.c_str()) != 0) {
            PLOG(ERROR) << "Fail to link data";
            return -1;
        }

        auto db = db_->db();
        std::string start,end;
        auto s = db->LoadRange(data_path,&start,&end);
        LOG(ERROR)<<"Load snapshot";
        if(!s.ok()){
            LOG(ERROR)<<s.ToString();
            return  -1;
        }

        LOG(INFO) << "ON_SNAPSHOT_LOAD";
        return 0;
    }

    void RaftNode::on_leader_start(int64_t term) {

        leader_term_.store(term, std::memory_order_release);
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
        LOG(INFO) << "Node start following " << ctx;
    }
    // end of @braft::StateMachine
//TO-DO check option

    void RaftNodeClosure::Run() {
        std::unique_ptr<RaftNodeClosure> self_guard(this);
        brpc::ClosureGuard done_guard(done_);
        if (status().ok()) {
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

} // namespace pidb