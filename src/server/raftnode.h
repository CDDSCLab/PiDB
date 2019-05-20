
#ifndef PIDB_RAFTNODE_H_
#define PIDB_RAFTNODE_H_
#include <memory>
#include <atomic>
#include <brpc/controller.h>
#include <braft/raft.h>
#include <braft/util.h> //braft::AsyncClosureGuard
#include <braft/storage.h> //braf::SnapshotWriter
#include <pidb/status.h>
#include <leveldb/db.h>
#include "shareddb.h"
#include <pidb.pb.h>

namespace pidb
{
class RaftNode;
struct RaftOption;
class Status;
class RaftNodeClosure: public braft::Closure{
public:
	RaftNodeClosure(RaftNode* node,
            const PiDBRequest *request,
            PiDBResponse* response,
            google::protobuf::Closure* done)
		:node_(node),
		request_(request),
 		response_(response),
		done_(done){}
	~RaftNodeClosure(){}
	const PiDBRequest* request() const {return request_;}
	PiDBResponse* response() const {return response_;}
	void Run();
private:
	RaftNode* node_;
	const PiDBRequest* request_;
	PiDBResponse* response_;
    google::protobuf::Closure* done_;
};

struct Range{
    Slice start;
    Slice limit;
    Range(){}
    Range(const Slice& s,const Slice& l):start(s),limit(l){}
};

using leveldb::Snapshot;
using leveldb::Iterator;


class RaftNode : public braft::StateMachine {
public:
    //RaftNode 的Rnage初始化不用option传入，因为range可能在运行中是经常变化的
    //我认为不要写入option中可能好一点
    //使用option设置node的端口和group(启动之后不会变化)
    RaftNode(const RaftOption &option,const Range &range);
    ~RaftNode() {
        delete node_;
    }
    Status start();

    virtual Status Get(const PiDBRequest *request,PiDBResponse* response,
            ::google::protobuf::Closure* done) const;
    virtual void Put(const PiDBRequest *request,PiDBResponse* response,
            ::google::protobuf::Closure* done);

    virtual void Write(const leveldb::WriteOptions& options,std::unique_ptr<PiDBWriteBatch> batchs,
            ::google::protobuf::Closure *done);

    /*待实现
    virtual Status Write(const PiDBRequest *request,PiDBResponse* response,
            ::google::protobuf::Closure* done);
    virtual Status Delete(const PiDBRequest *request,PiDBResponse* response,
    ::google::protobuf::Closure* done);
   virtual Iterator * NewIterator(const PiDBRequest *request,PiDBResponse* response,
            ::google::protobuf::Closure* done);
    virtual Snapshot* GetSnapshot();
    virtual void ReleaseSnapshot(const Snapshot* snapshot);
*/
    void on_apply(braft::Iterator &iter) override;
    void on_snapshot_save(braft::SnapshotWriter* writer,braft::Closure* done) override;
    int on_snapshot_load(braft::SnapshotReader* reader) override;
    void on_leader_start(int64_t term) override;
    void on_shutdown() override;
    void on_error(const ::braft::Error &e) override;
    void on_configuration_committed(const ::braft::Configuration& conf) override ;
    void on_stop_following(const ::braft::LeaderChangeContext& ctx) override;
    void on_start_following(const ::braft::LeaderChangeContext& ctx) override;
    Status SetRange(const Slice &s,const Slice &l){
        //TO-DO 判断s和l是否合法，例如s<=l 等等
        range_ = Range(s,l);
        return Status::OK();
    }
    // Starts this node
    scoped_db GetDB() const{
        BAIDU_SCOPED_LOCK(_db_mutex);
        return db_;
    }
    void SetDB(scoped_db db){
        //也可以使用std::lock_guard
        BAIDU_SCOPED_LOCK(_db_mutex);
        db_ = db;

    }
    bool IsLeader() const{
        return leader_term_.load(std::memory_order_acquire) >0;
    }
    void redirect(PiDBResponse * response);

    void shutdown(){
        if(node_) node_->shutdown(NULL);
    }

    void join(){
        if(node_) node_->join();
    }

    static void *save_snapshot(void* arg);
private:
    friend class RaftNodeClosure;
    mutable butil::Mutex _db_mutex;
    braft::Node* volatile node_;

    scoped_db db_;
    Range range_;

    //暂时用string 代表group,暂时让他们不可更改
    const std::string group_;
    const int32_t port_;
    const std::string conf_;

    std::atomic<int64_t> leader_term_;

    struct SnapshotHandle{
        scoped_db db;
        braft::SnapshotWriter* writer;
        braft::Closure* done;
    };

};

} // pidb

#endif
