// node server for controlling raft node
#include "server.h"
#include <sstream>
#include <leveldb/db.h>   //leveldb
#include "raftnode.h"

namespace  pidb
{
     Server::Server(const ServerOption &serveroption):port_(serveroption.port)
                    ,data_path_(std::move(serveroption.data_path)){
        //TO-DO recover from log
        //启动本地的raftnode，如果没有则初始化一个      
        //raft 和 server同属一个端口 
        //这只是一个实例，后面可能需调整优化
        auto option = RaftOption();
        //raft 和server共享一个rpc
        option.port = serveroption.port;
        option.group = "1";
        option.conf="127.0.1.1:8100:0 127.0.1.1:8101:0 127.0.1.1:8102:0";
        auto s = registerRaftNode(option);
        if(!s.ok()){
            LOG(INFO)<<"Fail to add raft node";
        }
    }

    Status Server::registerRaftNode(const RaftOption &option){
        if(nodes_.find(option.group)!=nodes_.end()) {
            std::ostringstream s;
            s << "There is alreay existing raftnode in" << option.group;
            return Status::Corruption(option.group, s.str());
        }
        Range range("","");
        auto raftnode = std::make_shared<RaftNode>(option,range);
        nodes_[option.group]=raftnode;
        //判断一下是否当前map里面没有同样id的raft
        return Status::OK();
    }

    //打开leveldb
    //加载配置，遍历nodes_里的raft 把他们全部启动起来
    Status Server::Start(){

        //开启leveldb
        std::string db_path = data_path_+"/database";
        leveldb::DB *db;
        leveldb::Options options;
        options.create_if_missing = true;
        auto status = leveldb::DB::Open(options,db_path,&db);
        if(!status.ok()){
            LOG(ERROR)<<"Fail to open db";
            return Status::Corruption(db_path,"Fail to open db");
        }
        db_ = new SharedDB(db);

        //可能存在部分节点启动失败，暂时返回OK


        for(auto const n:nodes_){
            if(!n.second->start().ok()){
                LOG(ERROR)<<"Fail to start"<<n.first<<"node";
                //TO-DO 是否记录失败信息，重试？
                return Status::Corruption(n.first,"Fail to start");
            }
        }
        return Status::OK();
    }

    Status Server::Stop(){
        for(const auto n:nodes_){
            //TO-DO 判断节点信息
            n.second->shutdown();
        }
        //server_.Stop(0);
        for(const auto n:nodes_){
            n.second->join();
        }
        //server_.Join();
        nodes_.clear();
    return Status::OK();
    }


    Status Server::Put(const ::pidb::PiDBRequest* request,
                       ::pidb::PiDBResponse* response,
                       ::google::protobuf::Closure* done){
        //put 操作需要更新到raft上，
        //选择正确的node，更新
        //TO-DO使用路由表去查找
        auto key = request->key();
        //TO-DO auto node = SelectNodde(key)
        auto node = nodes_.begin();
        //TO-DO 异步的方式
        auto s = node->second->Put(request,response,done);
        if(!s.ok()){
            LOG(ERROR)<<"Fail to put key into db";
            return Status::Corruption(key,"Fail to put key into db");
        }
        return Status::OK();
        
    }

    Status Server::Read(const ::pidb::PiDBRequest* request,
                       ::pidb::PiDBResponse* response,
                       ::google::protobuf::Closure* done){
        //因为是共享db，所以直接得到结果
        scoped_db db = db_;
        std::string value;
        auto s = db_->db()->Get(leveldb::ReadOptions(),request->key(),&value);
        if(!s.ok()){
            LOG(ERROR)<<"Fail to get key from db";
            return Status::Corruption(request->key(),"Fail to get key from db");
        }
        response->set_new_value(std::move(value));
        response->set_success(true);
        return Status::OK();

    }
} //  pidb
