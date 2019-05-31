//控制raftnode，并且回报情况给master
#ifndef PIDB_SERVER_H
#define PIDB_SERVER_H
#include <map>
#include <memory>
#include <unordered_map>
#include <brpc/server.h>  //srever
#include "context_cache.h"
#include "shareddb.h"
#include "braft/repeated_timer_task.h"
#include "pidb/status.h"
#include "pidb_service_impl.h"
#include "pidb/options.h"

namespace pidb{
class RaftNode;
struct Range;
class Server;
class ServerClosure: public braft::Closure{
public:
    ServerClosure(PiDBResponse* response,
                 google::protobuf::Closure* done,std::vector<std::string> groups);

    ~ServerClosure(){}
    PiDBResponse* response() const {return response_;}
    void SetDone(const std::string &group);
    bool IsDone();

    //Run用于判断batchs的操作是否已经全部完成
    void Run();
    Status s_;
private:
    //TODO 更一般化的形式
    std::mutex mutex_;
    std::atomic_uint64_t count_;
    std::unordered_map<std::string,bool> batchs;
    PiDBResponse* response_;
    google::protobuf::Closure* done_;
};


class ServerTimer:public braft::RepeatedTimerTask{
public:
    ServerTimer():server_(nullptr){}
    int init(std::shared_ptr<Server> server, int timeout_ms);
    virtual void run() = 0;

protected:
    void on_destroy();
    std::shared_ptr<Server> server_;
};

class HeartbeatTimer:public ServerTimer{
protected:
    void run();
};

class Server:public std::enable_shared_from_this<Server>{
public:
    //operation for write

	explicit Server(const ServerOption &serveroption);
	//no copy and =
	Server(const Server&) = delete;
	Server& operator = (const Server&) = delete;

    Status Start();
    Status Stop();

	//TO-DO 获得server的信息 要先定义一个获得信息的handler
	class InfoHandler;
	void getServerInfo(InfoHandler * handler) const;

	Status registerRaftNode(const RaftOption &option,const Range &range);
	Status removeRaftNode(const RaftOption &option);

	//处理Put请求，需要转发到raft节点
	void Put(const ::pidb::PiDBRequest* request,
                       ::pidb::PiDBResponse* response,
                       ::google::protobuf::Closure* done);


    //处理用户的Get请求,server可以直接处理
	Status Get(const ::pidb::PiDBRequest* request,
                       ::pidb::PiDBResponse* response,
                       ::google::protobuf::Closure* done);

	//处理用户发过来的write的请求
	void Write(const ::pidb::PiDBWriteBatch* request,
                   ::pidb::PiDBResponse* response,
                   ::google::protobuf::Closure* done);


    //Get Snapshot
    //return unique id 用于标识snapshot的id
    int64_t GetSnapshot();
    //当使用完snapshot的时候需要释放snapshot
    Status  ReleaseSnapshot(int64_t id);


    int64_t GetIterator(const std::string &start, const std::string &end);
    Status ReleaseIterator(int64_t id);
    Status Next(int64_t id,std::string* value);

    void HandleHearbeat();

	void Recover();
	void DestroyServer();

	// 给master发送心跳信息
	void Hearbet();
    ~Server(){}

private:
    //用于存储用户存储的snapshotContext
    pidb::ContextCache<pidb::SnapshotContext> snapshots_;
    pidb::ContextCache<pidb::IteratorContext> iterators_;
    ServerOption option_;
	int32_t port_;
	scoped_db db_;
	//可能需要换一种数据结构,暂时用map代替
	//TODO 一个Server里面的group只能一个raft节点？？？
	std::map<std::string,std::shared_ptr<RaftNode>> nodes_;
	std::string data_path_;

	HeartbeatTimer hearbeat_timer_;
};
}//namespace pidb

#endif // STORAGE_LEVELDB_DB_FILENAME_H_