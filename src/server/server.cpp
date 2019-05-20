// node server for controlling raft node
#include "server.h"
#include <sstream>
#include <leveldb/db.h>   //leveldb
#include <leveldb/write_batch.h>
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
//        option.conf="127.0.1.1:8100:0,127.0.1.1:8101:0,127.0.1.1:8102:0";
        option.conf="127.0.1.1:8100:0";
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
        //raftnode->SetDB(db_);
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
            //在启动前设置共享的db,而不是在初始化的时候
            n.second->SetDB(db_);
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


    void Server::Put(const ::pidb::PiDBRequest* request,
                       ::pidb::PiDBResponse* response,
                       ::google::protobuf::Closure* done){
        //put 操作需要更新到raft上，
        //选择正确的node，更新
        //TO-DO使用路由表去查找
        auto key = request->key();
        LOG(INFO)<<key;

        //TODO auto node = SelectNodde(key) 并判断是否合法
        auto node = nodes_.begin();

        //为了测试取第一个
        assert(node!=nodes_.end());

        //TO-DO 异步的方式
        node->second.get()->Put(request,response,done);
        
    }

    Status Server::Get(const ::pidb::PiDBRequest* request,
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

    void Server::Write(const ::pidb::PiDBWriteBatch *request,
                       ::pidb::PiDBResponse *response,
                       ::google::protobuf::Closure *done) {
         brpc::ClosureGuard done_guard(done);
         //根据batch 的内容进行转发到不同的region上面写
         assert(request->writebatch_size()>0);
         //std::unordered_map<std::string,std::unique_ptr<leveldb::WriteBatch>> batchs;
         std::unordered_map<std::string,std::unique_ptr<PiDBWriteBatch>> batchs;

         //遍历write_batch ,分发到不同的region,传给每个region一个writebatch的请求
         //因为需要序列化，所以直接使用PiDBWritebatch
         for(int i = 0;i<request->writebatch_size();i++){
            auto batch = request->writebatch(i);
            //为了测试该region为group1
            //auto group = FindGroup(batch.key());
            auto group = "group1";
            //当前group还没有batch

            if(batchs.find(group)==batchs.end()){
                batchs[group] = std::unique_ptr<PiDBWriteBatch>(new PiDBWriteBatch);
            }
            auto op = batchs[group]->add_writebatch();
            op->set_op(batch.op());
            op->set_key(std::move(batch.key()));
            if(batch.op()==kPutOp)
                op->set_value(std::move(batch.value()));
//
//             switch(batch.op()){
//                 case kPutOp:
//                     //TODO 获得key值,找到region 的id,并放入其batch中
//
//                     batchs[group]->set
//                     break;
//                 case kDeleteOp:
//                     batchs[group]->Delete(batch.key());
//                     break;
//                 //不属于操作范围，直接break,其他的操作不受影响
//                 default:
//                     LOG(INFO)<<"Write batch unknown operation";
//                     break;
//             }// switch
         } // for write_batch
         ServerClosure * closure = new ServerClosure(response,done_guard.release());
         //TODO 异步调用
         for( auto &item:batchs){
             auto node = nodes_[item.first];
             assert(node!=nullptr);
             //因为write 操作可能涉及多个rfat的操作,所以不能直接将response交给raft执行，需要
             //server端 收集所有涉及操作的region的信息。
             //TODO 可否采用并发执行，不考虑其执行顺序？
             node->Write(leveldb::WriteOptions(),std::move(item.second),closure);
         }
     }
     void ServerClosure::Run() {
         return;
     }
} //  pidb
