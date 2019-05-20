//控制raftnode，并且回报情况给master
#ifndef PIDB_SERVER_H
#define PIDB_SERVER_H
#include <map>
#include <memory>
#include <unordered_map>
#include <brpc/server.h>  //srever
#include "shareddb.h"
#include "pidb/status.h"
#include "pidb_service_impl.h"
#include "pidb/options.h"

namespace pidb{
class RaftNode;

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

class Server{
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

	Status registerRaftNode(const RaftOption &option);
	Status removeRaftNode(const RaftOption &option);

	//暂时实现两个
	void Put(const ::pidb::PiDBRequest* request,
                       ::pidb::PiDBResponse* response,
                       ::google::protobuf::Closure* done);

	Status Get(const ::pidb::PiDBRequest* request,
                       ::pidb::PiDBResponse* response,
                       ::google::protobuf::Closure* done);

    void Write(const ::pidb::PiDBWriteBatch* request,
                   ::pidb::PiDBResponse* response,
                   ::google::protobuf::Closure* done);
	void Recover();
	void DestroyServer();

	// 给master发送心跳信息
	void Hearbet();
    ~Server(){}
private:
	int32_t port_;
	scoped_db db_;
	//可能需要换一种数据结构,暂时用map代替
	std::map<std::string,std::shared_ptr<RaftNode>> nodes_;
	std::string data_path_;
};
}//namespace pidb

#endif // STORAGE_LEVELDB_DB_FILENAME_H_