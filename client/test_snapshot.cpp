//
// Created by ehds on 19-5-23.
//

#include "client.h"
#include <memory>
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

    //pidb::PiDBSnapshot *snapshot = new pidb::PiDBSnapshot();
    // pidb::PiDBSnapshot snapshot ;
    std::unique_ptr<pidb::PiDBSnapshot> snapshot(new pidb::PiDBSnapshot());
    cntl.set_log_id(log_id++);
    pidb::Empty empty;
    stub.GetSnapshot(&cntl,&empty,snapshot.get(),NULL);
    if(!cntl.Failed()){
        LOG(INFO)<<"Get Snapshopt success id :"<<snapshot->id();
    } else{
        LOG(ERROR)<<cntl.ErrorText();
    }
    cntl.Reset();
    pidb::Success success;
    stub.ReleaseSnapshot(&cntl,snapshot.get(),&success,NULL);
    if (!cntl.Failed()){
        LOG(INFO)<<"Release Snapshopt id:"<<snapshot->id()<<" Success";
    }else{
        LOG(INFO)<<"Release Snapshopt id:"<<snapshot->id()<<" Failed";
    }


    pidb::PiDBRequest request;

    request.set_key("name");
    request.set_value("uestc");
    cntl.Reset();
    stub.Put(&cntl,&request,&response,NULL);
    if (!cntl.Failed()) {
        LOG(INFO) << "Received response from " << cntl.remote_side()
                  << " to " << cntl.local_side()
                  << ": " << response.success() << " (attached="
                  << cntl.response_attachment() << ")"
                  << " latency=" << cntl.latency_us() << "us";
        LOG(INFO)<<"Put Key"<<request.key()<<"value"<<request.value()<< "success";
    } else {
        LOG(WARNING) << cntl.ErrorText();
    }
    cntl.Reset();

    stub.GetSnapshot(&cntl,&empty,snapshot.get(),NULL);
    LOG(INFO)<<"Get Snapshopt success id :"<<snapshot->id();

    request.set_key("name");
    request.set_value("dscl");
    cntl.Reset();
    stub.Put(&cntl,&request,&response,NULL);
    if (!cntl.Failed()) {
        LOG(INFO) << "Received response from " << cntl.remote_side()
                  << " to " << cntl.local_side()
                  << ": " << response.success() << " (attached="
                  << cntl.response_attachment() << ")"
                  << " latency=" << cntl.latency_us() << "us";
        LOG(INFO)<<"Put Key"<<request.key()<<"value"<<request.value()<< "success";
    } else {
        LOG(WARNING) << cntl.ErrorText();
    }

    request.set_key("name");
    cntl.Reset();
    stub.Get(&cntl,&request,&response,NULL);
    if (!cntl.Failed())
        LOG(INFO)<<response.new_value();

    request.clear_snapshot();

    cntl.Reset();
    stub.ReleaseSnapshot(&cntl,snapshot.get(),&success,NULL);

}