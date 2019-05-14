#include <memory>
#include "raftnode.h"
#include "pidb/options.h"

namespace pidb{

RaftNode::RaftNode(const RaftOption &option,const Range &range)
                    :group_(std::move(option.group))
                    ,port_(option.port)
                    ,conf_(std::move(option.conf))
                    ,learder_term_(-1){

    SetRange(range.start,range.limit);

}

Status RaftNode::start(){
    butil::EndPoint addr(butil::my_ip(),port_);
    braft::NodeOptions node_options;
    if(node_options.initial_conf.parse_from(conf_)!=0){
        LOG(ERROR)<<"Fail to parse configuration";
        return Status::OK();
    }
    node_options.election_timeout_ms = 5000;
    node_options.fsm = this;
    node_options.snapshot_interval_s = 30;
    std::string prefix = "local://./"+group_;
    node_options.log_uri=prefix+"/log";
    node_options.raft_meta_uri = prefix+"/raft_meta";
    node_options.snapshot_uri = prefix+"/snapshot";
    node_options.disable_cli = false;
    braft::Node* node = new braft::Node(group_,braft::PeerId(addr));
    if(node->init(node_options)!=0){
        LOG(ERROR)<<"Fail to init raft node";
        delete node;
        node = nullptr;
        return Status::InvalidArgument(group_,"Fail to init node");
    }
    node_ = node;
    return Status::OK();
}
void RaftNode::redirect(PiDBResponse *response){
    response->set_success(false);
    if(node_){
        braft::PeerId leader = node_->leader_id();
        if(!leader.is_empty()){
            response->set_redirect(leader.to_string());
        }
    }
}

void RaftNode::on_apply(braft::Iterator& iter){
//TO-DO
   for (; iter.valid(); iter.next()) {
            PiDBResponse* response = NULL;

            // This guard helps invoke iter.done()->Run() asynchronously to
            // avoid that callback blocks the StateMachine
            braft::AsyncClosureGuard closure_guard(iter.done());
            butil::IOBuf data;
            std::string key,value;
            if (iter.done()) {
                // This task is applied by this node, get value from this
                // closure to avoid additional parsing.
                RaftNodeClosure* c = dynamic_cast<RaftNodeClosure*>(iter.done());
                //data.swap(*(c->data()));
                response = c->response();
                key = c->request()->key();
                value = c->request()->value();
            } else {
                butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
                PiDBRequest request;
                CHECK(request.ParseFromZeroCopyStream(&wrapper));
                key = request.key();
                value = request.value();
            }
            leveldb::Status s = db_->db()->Put(leveldb::WriteOptions(),key,value);

            //fail to put value
            if(!s.ok()){
            	if(response){
            		response->set_success(false);
            	}
            	closure_guard.release();
            	// TO-DO which error happened and what should be done.
           		return;
            }

            if (response) {
                response->set_success(true);
            }

        }
}

void* RaftNode::save_snapshot(void* arg){
    //TO-DO
    SnapshotHandle* sh =static_cast<SnapshotHandle*> (arg);
    brpc::ClosureGuard done_guard(sh->done);
    return nullptr;
}

void RaftNode::on_snapshot_save(braft::SnapshotWriter* writer,braft::Closure* done) {
    SnapshotHandle * arg = new SnapshotHandle;
    arg->db = db_;
    arg->writer = writer;
    arg->done = done;
    bthread_t tid;
    bthread_start_urgent(&tid,NULL,save_snapshot,arg);
}

int RaftNode::on_snapshot_load(braft::SnapshotReader* reader){
    //TO-DO
    LOG(INFO)<<"ON_SNAPSHOT_LOAD";
    return 1;
}

void RaftNode::on_leader_start(int64_t term){
    
    learder_term_.store(term,std::memory_order_release);
}
void RaftNode::on_shutdown(){
    //TO-DO
     LOG(INFO)<<"on_shutdown";
}
void RaftNode::on_error(const ::braft::Error &e){
    LOG(ERROR)<<"raft error";
}

void RaftNode::on_configuration_committed(const ::braft::Configuration& conf) {
    LOG(INFO) << "Configuration of this group is " << conf;
}
void RaftNode::on_stop_following(const ::braft::LeaderChangeContext& ctx) {
    LOG(INFO) << "Node stops following " << ctx;
}
void RaftNode::on_start_following(const ::braft::LeaderChangeContext& ctx) {
    LOG(INFO) << "Node start following " << ctx;
}
    // end of @braft::StateMachine
//TO-DO check option

void RaftNodeClosure::Run(){
    std::unique_ptr<RaftNodeClosure> self_guard(this);
    brpc::ClosureGuard done_guard(done_);
    if(status().ok()){
        return;
    }
    node_->redirect(response_);
}

Status RaftNode::Get(const PiDBRequest *request,PiDBResponse* response,
                     ::google::protobuf::Closure* done) const{
    if(!IsLeader()){
        // return redirect(response);
        //当前不是leader 不能直接转发请求，需要提示用户刷新本地缓存。
        return Status::Corruption(group_,"not leader");
    }
    //获得db_
    auto db = db_;
    std::string value;
    auto s = db_->db()->Get(leveldb::ReadOptions(),request->key(),&value);
    if(!s.ok()){
        LOG(ERROR)<<"Fail to read from db";
        response->set_success(false);
        return Status::Corruption(group_,"Fail to read from db");
    }
    response->set_new_value(value);
    response->set_success(true);
    return Status::OK();
}
Status RaftNode::Put(const PiDBRequest *request,PiDBResponse* response,
               google::protobuf::Closure* done){
    brpc::ClosureGuard done_guard(done);
    const int64_t term = learder_term_.load(std::memory_order_relaxed);
    if(term<0){
            redirect(response);
            return Status::InvalidArgument("Leader","Not leader");
    }
      butil::IOBuf log;
        butil::IOBufAsZeroCopyOutputStream wrapper(&log);
        if (!request->SerializeToZeroCopyStream(&wrapper)) {
            LOG(ERROR) << "Fail to serialize request";
            response->set_success(false);
            return Status::Corruption("put","Fail to serialize request");
        }

        // Apply this log as a braft::Task
        braft::Task task;
        task.data = &log;
        // This callback would be iovoked when the task actually excuted or
        // fail
        task.done = new RaftNodeClosure(this, request,response,done_guard.release());

        if (true) {
            // ABA problem can be avoid if expected_term is set
            task.expected_term = term;
        }
        // Now the task is applied to the group, waiting for the result.
         node_->apply(task);
        return Status::OK();
}

} // namespace pidb