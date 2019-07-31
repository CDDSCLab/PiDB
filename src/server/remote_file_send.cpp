//
// Created by ehds on 7/24/19.
//

#include "remote_file_send.h"
#include "pidb.pb.h"
//
//DEFINE_string(server, "0.0.0.0:8001", "IP Address of server");
//DEFINE_int32(timeout_ms, 100, "RPC timeout in milliseconds");
//DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)");
//

namespace pidb{

    RemoteFileSend::RemoteFileSend(const std::string &addr, const std::string &source_path)
                            :remote_addr_(addr),source_path_(source_path)
    {
        brpc::ChannelOptions options;
        options.protocol = brpc::PROTOCOL_BAIDU_STD;
        options.connection_type = "";
        options.timeout_ms = 100/*milliseconds*/;
        options.max_retry = 3;

        //初始化channel_
        if (channel_.Init(remote_addr_.c_str(), NULL) != 0) {
            LOG(ERROR) << "Fail to initialize channel";
        }

        //初始化stream
        if (brpc::StreamCreate(&stream_, cntl_, NULL) != 0) {
            LOG(ERROR) << "Fail to create stream";
        }
        env_ = leveldb::Env::Default();
        st_ = leveldb::Status::OK();

    }

    //开始推送数据
    void RemoteFileSend::start() {

        pidb::PiDBService_Stub stub(&channel_);
        pidb::PiDBFileRequest request;
        pidb::PiDBFileResponse response;
        request.set_filename(source_path_);

        stub.PushFile(&cntl_,&request,&response,NULL);
        if(cntl_.Failed()){
            LOG(INFO)<<"建立连接失败"<<cntl_.ErrorText();
            //TODO 重试
        }

        if(env_->FileExists(source_path_)){
            st_.Corruption(source_path_+" File not exists");
            return;
        }

        leveldb::SequentialFile *file;
        env_->NewSequentialFile(source_path_,&file);

        if (brpc::StreamCreate(&stream_, cntl_, NULL) != 0) {
            LOG(ERROR) << "Fail to create stream";
        }

        //写数据
        char *buffer = new char[128*1024];
        leveldb::Slice res;
        butil::IOBuf msg;
        int retry = 5;
        //每次读写128k数据
        while ((st_=file->Read(128*1024,&res,buffer)).ok() && res.size()>0){
            msg.append(buffer,res.size());
            //需要重试
            auto code = brpc::StreamWrite(stream_, msg);
            LOG(INFO)<<code;
            if(code==EAGAIN){
                //重试五次
                LOG(INFO)<<"RETRY";
                for (retry =0;retry<5 && code==EAGAIN;retry++){
                    code = brpc::StreamWrite(stream_, msg);
                    usleep(10000);
                }
                if(retry>=5){
                    st_ = leveldb::Status::Corruption("Failt to send stream to remote server");
                    return;
                }

            }
            LOG(INFO)<<res.size();
            res.clear();
            msg.clear();
        }

        delete buffer;
        delete file;

    }


}//namespace pidb
