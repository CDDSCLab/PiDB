//
// Created by ehds on 19-5-23.
//

//
// Created by ehds on 19-5-23.
//
#include "client.h"

int main(int argc, char* argv[]) {
// Parse gflags. We recommend you to use gflags as well.
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

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



    pidb::PiDBWriteBatch batch;
    pidb::PiDBResponse response;

    pidb::PiDBOperator *oper;


    oper = batch.add_writebatch();
    //设置一个batch的操作 put key1->value1
    oper->set_op(2);
    oper->set_key("key4");
    oper->set_value("value4");

    oper=batch.add_writebatch();
    oper->set_op(2);
    oper->set_key("key5");
    oper->set_value("value_new");
    //oper->set_value("value2"); //删除操作不需要设置alue

    cntl.set_log_id(log_id ++);  // set by user
    stub.Write(&cntl, &batch, &response, NULL);

    if (!cntl.Failed()) {
        LOG(INFO) << "Received response from " << cntl.remote_side()
                  << " to " << cntl.local_side()
                  << ": " << response.success() << " (attached="
                  << cntl.response_attachment() << ")"
                  << " latency=" << cntl.latency_us() << "us";
        LOG(INFO)<<"Write batch success";
    } else {
        LOG(WARNING) << cntl.ErrorText();
    }
}