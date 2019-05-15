//控制raftnode，并且回报情况给master
#ifndef PIDB_SERVER_H
#define PIDB_SERVER_H
#include <map>
#include <memory>
#include <brpc/server.h>  //srever
#include "shareddb.h"
#include "pidb/status.h"
#include "pidb_service_impl.h"
#include "pidb/options.h"

namespace pidb{
class RaftNode;
class Server{
public:
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
	Status Put(const ::pidb::PiDBRequest* request,
                       ::pidb::PiDBResponse* response,
                       ::google::protobuf::Closure* done);

	Status Read(const ::pidb::PiDBRequest* request,
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