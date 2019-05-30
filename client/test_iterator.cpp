//
// Created by ehds on 19-5-23.
//
#include "client.h"

int main(int argc, char* argv[]) {
    // Parse gflags. We recommend you to use gflags as well.
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

    butil::CreateDirectory(butil::FilePath("./data"));
    // A Channel represents a communication line to a Server. Notice that
    // Channel is thread-safe and can be shared by all threads in your program.
    brpc::Channel channel;

    // Initialize the channel, NULL means using default options.
    brpc::ChannelOptions options;
    options.protocol = FLAGS_protocol;
    options.connection_type = FLAGS_connection_type;
    options.timeout_ms = FLAGS_timeout_ms/*milliseconds*/;
    options.max_retry = FLAGS_max_retry;
    if (channel.Init(FLAGS_server.c_str(), FLAGS_load_balancer.c_str(), &options) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        return -1;
    }

    //出使花stub
    pidb::PiDBService_Stub stub(&channel);

    // Send a request and wait for the response every 1 second.
    int log_id = 0;


    brpc::Controller cntl;
    pidb::PiDBResponse response;

    pidb::PiDBIterator iterator;
    //先给一个-1，待优化
    iterator.set_id(-1);
    iterator.set_start("1");
    iterator.set_stop("2");
    stub.GetIterator(&cntl, &iterator, &iterator, NULL);
    if (!cntl.Failed()) {
        LOG(INFO) << "Iterator id:"<<iterator.id();
    } else{
        LOG(INFO)<<"fAILE";
    }
    iterator.set_id(iterator.id());
    cntl.Reset();

    stub.Iterate(&cntl, &iterator, &response, NULL);

    if (!cntl.Failed()) {
        LOG(INFO) <<  "遍历1："<<response.new_value();
    } else {
        LOG(INFO) << cntl.ErrorText();
    }
    cntl.Reset();
    stub.Iterate(&cntl, &iterator, &response, NULL);
    if (!cntl.Failed()) {
        LOG(INFO) << "遍历2："<<response.new_value();
    } else {
        LOG(INFO) << cntl.ErrorText();
    }
    return 0;
    cntl.Reset();

}