//
// Created by ehds on 7/24/19.
//

#ifndef PIDB_REMOTE_FILE_SEND_H
#define PIDB_REMOTE_FILE_SEND_H

#include <string>
#include <gflags/gflags.h>
#include <butil/logging.h>
#include <brpc/channel.h>
#include <brpc/stream.h>
#include <butil/file_util.h>

#include <leveldb/env.h>
#include <leveldb/status.h>


namespace pidb{
class RemoteFileSend {
public:
    RemoteFileSend(const std::string& addr,const std::string &source_path);

    void start();

    //待实现
   // void cancle();
    const leveldb::Status &status() const {return st_;}


private:
    //发送数据到指定group的region
    std::string group_;
    brpc::Controller cntl_;
    brpc::Channel channel_;
    leveldb::Env *env_;
    std::string source_path_;

    //远程braft的conf
    std::string conf_;
    std::string remote_addr_;
    brpc::StreamId stream_;

    //记录在传输过程中的状态
    leveldb::Status st_;


};
}//namespace pidb

#endif //PIDB_REMOTE_FILE_SEND_H
