
#include <sys/types.h>                  // O_CREAT
#include <fcntl.h>                      // open
#include <gflags/gflags.h>              // DEFINE_*
#include <butil/sys_byteorder.h>        // butil::NetToHost32
#include <brpc/controller.h>            // brpc::Controller
#include <brpc/server.h>                // brpc::Server
#include <braft/raft.h>                 // braft::Node braft::StateMachine
#include <braft/storage.h>              // braft::SnapshotWriter
#include <braft/util.h>                 // braft::AsyncClosureGuard
#include <leveldb/db.h>
#include "pidb.pb.h"

DEFINE_bool(check_term,true,"Check if the leader changed to another term");
DEFINE_bool(disable_cli,false,"Don't allow raft cli access this node");
DEFINE_bool(log_applied_task,false,"Print notice log when a task is applied");
DEFINE_int32(election_timeout_ms,5000,
			 "Start election in such milliseconds if disconnect with the leader");
DEFINE_int32(port, 8200, "Listen port of this peer");
DEFINE_int32(snapshot_interval, 30, "Interval between each snapshot");
DEFINE_string(conf, "", "Initial configuration of the replication group");
DEFINE_string(data_path, "./data", "Path of data stored on");
DEFINE_string(group, "Block", "Id of the replication group");
DEFINE_string(db_path,"./db","Path of leveldb");

namespace pidb{
class Data;

class DataClosure: public braft::Closure{
public:
	DataClosure(Data* data,
				const PiDBRequest *request,
				PiDBResponse* response,
				google::protobuf::Closure* done)
		: _data(data),
		_request(request),
 		_response(response),
		_done(done){}
	~DataClosure(){}
	const PiDBRequest* request() const {return _request;}
	PiDBResponse* response() const {return _response;}
	void Run();
private:
	Data* _data;
	const PiDBRequest* _request;
	PiDBResponse* _response;
    google::protobuf::Closure* _done;
};

class Data : public braft::StateMachine {
public:
    Data()
        : _node(NULL)
        , _leader_term(-1)
        , _db(NULL)
    {}
    ~Data() {
        delete _node;
    }

    // Starts this node
    int start() {
    	//set the db path of leveldb
    	std::string db_path = FLAGS_db_path+"/database";
    	// set options
    	leveldb::DB *db;
    	leveldb::Options options;
    	options.create_if_missing = true;
        if (!(leveldb::DB::Open(options,db_path,&db)).ok()) {
            LOG(ERROR) << "Fail to open db " << db_path;
            return -1;
        }
        LOG(INFO)<<"DB PATH IS "<<db_path;
        _db = new SharedDB(db);

        butil::EndPoint addr(butil::my_ip(), FLAGS_port);
        braft::NodeOptions node_options;
        if (node_options.initial_conf.parse_from(FLAGS_conf) != 0) {
            LOG(ERROR) << "Fail to parse configuration `" << FLAGS_conf << '\'';
            return -1;
        }
        node_options.election_timeout_ms = FLAGS_election_timeout_ms;
        node_options.fsm = this;
        node_options.node_owns_fsm = false;
        node_options.snapshot_interval_s = FLAGS_snapshot_interval;
        std::string prefix = "local://" + FLAGS_data_path;
        node_options.log_uri = prefix + "/log";
        node_options.raft_meta_uri = prefix + "/raft_meta";
        node_options.snapshot_uri = prefix + "/snapshot";
        node_options.disable_cli = FLAGS_disable_cli;
        braft::Node* node = new braft::Node(FLAGS_group, braft::PeerId(addr));
        if (node->init(node_options) != 0) {
            LOG(ERROR) << "Fail to init raft node";
            delete node;
            return -1;
        }
        _node = node;
        return 0;
    }

    // Impelements Service methods
    void write(const PiDBRequest* request,
               PiDBResponse* response,
               butil::IOBuf* data,
               google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        // Serialize request to the replicated write-ahead-log so that all the
        // peers in the group receive this request as well.
        // Notice that _value can't be modified in this routine otherwise it
        // will be inconsistent with others in this group.

        // Serialize request to IOBuf
        const int64_t term = _leader_term.load(butil::memory_order_relaxed);
        if (term < 0) {
            return redirect(response);
        }
        butil::IOBuf log;
        butil::IOBufAsZeroCopyOutputStream wrapper(&log);
        if (!request->SerializeToZeroCopyStream(&wrapper)) {
            LOG(ERROR) << "Fail to serialize request";
            response->set_success(false);
            return;
        }

        // Apply this log as a braft::Task
        braft::Task task;
        task.data = &log;
        // This callback would be iovoked when the task actually excuted or
        // fail
        task.done = new DataClosure(this, request,response,done_guard.release());
        if (FLAGS_check_term) {
            // ABA problem can be avoid if expected_term is set
            task.expected_term = term;
        }
        // Now the task is applied to the group, waiting for the result.
        return _node->apply(task);
    }

    void read(const PiDBRequest *request, PiDBResponse* response,
              butil::IOBuf* buf) {
        // In consideration of consistency. GetRequest to follower should be
        // rejected.
        if (!is_leader()) {
            // This node is a follower or it's not up-to-date. Redirect to
            // the leader if possible.
            return redirect(response);
        }

        // This is the leader and is up-to-date. It's safe to respond client
        scoped_db db = get_db();
        std::string value;
        leveldb::Status s = _db->db()->Get(leveldb::ReadOptions(),request->key(),&value);
        if(!s.ok()){
        	PLOG(ERROR) << "Fail to read from db=";
        	response->set_success(false);
        	return;
        }
      	response->set_new_value(value);
        response->set_success(true);
    }

    bool is_leader() const
    { return _leader_term.load(butil::memory_order_acquire) > 0; }

    // Shut this node down.
    void shutdown() {
        if (_node) {
            _node->shutdown(NULL);
        }
    }

    // Blocking this thread until the node is eventually down.
    void join() {
        if (_node) {
            _node->join();
        }
    }

private:
    class SharedDB : public butil::RefCountedThreadSafe<SharedDB> {
    public:
        explicit SharedDB(leveldb::DB* db) : _db(db) {}
        leveldb::DB* db() const { return _db; }
    private:
    friend class butil::RefCountedThreadSafe<SharedDB>;
        ~SharedDB() {
            if (_db!=nullptr) {
               delete _db;
            }
        }
        leveldb::DB* _db;
    };

    typedef scoped_refptr<SharedDB> scoped_db;

    scoped_db get_db() const {
        BAIDU_SCOPED_LOCK(_db_mutex);
        return _db;
    }
friend class DataClosure;

    void redirect(PiDBResponse* response) {
        response->set_success(false);
        if (_node) {
            braft::PeerId leader = _node->leader_id();
            if (!leader.is_empty()) {
                response->set_redirect(leader.to_string());
            }
        }
    }

    // @braft::StateMachine
    void on_apply(braft::Iterator& iter) {
        // A batch of tasks are committed, which must be processed through
        // |iter|
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
                DataClosure* c = dynamic_cast<DataClosure*>(iter.done());
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
            leveldb::Status s = _db->db()->Put(leveldb::WriteOptions(),key,value);

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

            // The purpose of following logs is to help you understand the way
            // this StateMachine works.
            // Remove these logs in performance-sensitive servers.
            LOG_IF(INFO, FLAGS_log_applied_task)
                    << "PUT KEY=" << key<<" VALUE IS"<<value;

        }
    }

    struct SnapshotArg {
        scoped_db db;
        braft::SnapshotWriter* writer;
        braft::Closure* done;
    };

    static int link_overwrite(const char* old_path, const char* new_path) {
        if (::unlink(new_path) < 0 && errno != ENOENT) {
            PLOG(ERROR) << "Fail to unlink " << new_path;
            return -1;
        }
        return ::link(old_path, new_path);
    }

    static void *save_snapshot(void* arg) {
        SnapshotArg* sa = (SnapshotArg*) arg;
        std::unique_ptr<SnapshotArg> arg_guard(sa);
        // Serialize StateMachine to the snapshot
        brpc::ClosureGuard done_guard(sa->done);
        std::string snapshot_path = sa->writer->get_path() + "/database";
        // Sync buffered data before
        LOG(INFO) << "Saving snapshot to " << snapshot_path;

        std::string db_path = FLAGS_db_path + "/database";

        if (link_overwrite(db_path.c_str(), snapshot_path.c_str()) != 0) {
            sa->done->status().set_error(EIO, "Fail to link data : %m");
            return NULL;
        }

        // Snapshot is a set of files in raft. Add the only file into the
        // writer here.
        if (sa->writer->add_file("database") != 0) {
            sa->done->status().set_error(EIO, "Fail to add file to writer");
            return NULL;
        }
        return NULL;
    }

    void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
        // Save current StateMachine in memory and starts a new bthread to avoid
        // blocking StateMachine since it's a bit slow to write data to disk
        // file.
        SnapshotArg* arg = new SnapshotArg;
        arg->db = _db;
        arg->writer = writer;
        arg->done = done;
        bthread_t tid;
        bthread_start_urgent(&tid, NULL, save_snapshot, arg);
    }

    int on_snapshot_load(braft::SnapshotReader* reader) {
        // Load snasphot from reader, replacing the running StateMachine
        CHECK(!is_leader()) << "Leader is not supposed to load snapshot";
        if (reader->get_file_meta("data", NULL) != 0) {
            LOG(ERROR) << "Fail to find `data' on " << reader->get_path();
            return -1;
        }
        // reset fd
        _db = NULL;
        std::string snapshot_path = reader->get_path() + "/database";
        std::string db_path = FLAGS_db_path + "/database";
        if (link_overwrite(snapshot_path.c_str(), db_path.c_str()) != 0) {
            PLOG(ERROR) << "Fail to link data";
            return -1;
        }
        // Reopen this database
		leveldb::DB *db;
    	leveldb::Options options;
    	options.create_if_missing = true;
        if (!(leveldb::DB::Open(options,db_path,&db)).ok()) {
            LOG(ERROR) << "Fail to open db " << db_path;
            return -1;
        }
        _db = new SharedDB(db);
        return 0;
    }

    void on_leader_start(int64_t term) {
        _leader_term.store(term, butil::memory_order_release);
        LOG(INFO) << "Node becomes leader";
    }
    void on_leader_stop(const butil::Status& status) {
        _leader_term.store(-1, butil::memory_order_release);
        LOG(INFO) << "Node stepped down : " << status;
    }

    void on_shutdown() {
        LOG(INFO) << "This node is down";
    }
    void on_error(const ::braft::Error& e) {
        LOG(ERROR) << "Met raft error " << e;
    }
    void on_configuration_committed(const ::braft::Configuration& conf) {
        LOG(INFO) << "Configuration of this group is " << conf;
    }
    void on_stop_following(const ::braft::LeaderChangeContext& ctx) {
        LOG(INFO) << "Node stops following " << ctx;
    }
    void on_start_following(const ::braft::LeaderChangeContext& ctx) {
        LOG(INFO) << "Node start following " << ctx;
    }
    // end of @braft::StateMachine

private:
    mutable butil::Mutex _db_mutex;
    braft::Node* volatile _node;
    butil::atomic<int64_t> _leader_term;
    scoped_db _db;
};

void DataClosure::Run() {
    // Auto delete this after Run()
    std::unique_ptr<DataClosure> self_guard(this);
    // Repsond this RPC.
    brpc::ClosureGuard done_guard(_done);
    if (status().ok()) {
        return;
    }
    // Try redirect if this request failed.
    _data->redirect(_response);
}

// Implements pidb::BlockService if you are using brpc.
class PiDBServiceImpl : public PiDBService {
public:
    explicit PiDBServiceImpl(Data* data) : _data(data) {}
    void write(::google::protobuf::RpcController* controller,
               const ::pidb::PiDBRequest* request,
               ::pidb::PiDBResponse* response,
               ::google::protobuf::Closure* done) {
        brpc::Controller* cntl = (brpc::Controller*)controller;
        return _data->write(request, response,
                             &cntl->request_attachment(), done);
    }
    void read(::google::protobuf::RpcController* controller,
              const ::pidb::PiDBRequest* request,
              ::pidb::PiDBResponse* response,
              ::google::protobuf::Closure* done) {
        brpc::Controller* cntl = (brpc::Controller*)controller;
        brpc::ClosureGuard done_guard(done);
        return _data->read(request, response, &cntl->response_attachment());
    }
private:
    Data* _data;
};
}

int main(int argc, char* argv[]) {
    //GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);
    butil::AtExitManager exit_manager;

    // Generally you only need one Server.
    brpc::Server server;
    pidb::Data data;
    pidb::PiDBServiceImpl service(&data);

    // Add your service into RPC rerver
    if (server.AddService(&service,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }
    // raft can share the same RPC server. Notice the second parameter, because
    // adding services into a running server is not allowed and the listen
    // address of this server is impossible to get before the server starts. You
    // have to specify the address of the server.
    if (braft::add_service(&server, FLAGS_port) != 0) {
        LOG(ERROR) << "Fail to add raft service";
        return -1;
    }

    // It's recommended to start the server before Block is started to avoid
    // the case that it becomes the leader while the service is unreacheable by
    // clients.
    // Notice that default options of server are used here. Check out details
    // from the doc of brpc if you would like change some options
    if (server.Start(FLAGS_port, NULL) != 0) {
        LOG(ERROR) << "Fail to start Server";
        return -1;
    }

    // It's ok to start Block
    if (data.start() != 0) {
        LOG(ERROR) << "Fail to start Block";
        return -1;
    }

    LOG(INFO) << "Block service is running on " << server.listen_address();
    // Wait until 'CTRL-C' is pressed. then Stop() and Join() the service
    while (!brpc::IsAskedToQuit()) {
        sleep(1);
    }
    LOG(INFO) << "Block service is going to quit";

    // Stop block before server
    data.shutdown();
    server.Stop(0);

    // Wait until all the processing tasks are over.
    data.join();
    server.Join();
    return 0;
}
