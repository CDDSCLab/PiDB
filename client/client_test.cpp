//
// Created by ehds on 19-5-18.
//

#include <gflags/gflags.h>
#include <butil/logging.h>
#include <butil/time.h>
#include <brpc/channel.h>
#include <pidb.pb.h>
#include "pidb.pb.h"

DEFINE_string(attachment, "", "Carry this along with requests");
DEFINE_string(protocol, "baidu_std", "Protocol type. Defined in src/brpc/options.proto");
DEFINE_string(connection_type, "", "Connection type. Available values: single, pooled, short");
DEFINE_string(server, "127.0.1.1:8100", "IP Address of server");
DEFINE_string(load_balancer, "", "The algorithm for load balancing");
DEFINE_int32(timeout_ms, 100, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)");
DEFINE_int32(interval_ms, 1000, "Milliseconds between consecutive requests");

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
   // while (!brpc::IsAskedToQuit()) {

        brpc::Controller cntl;

        pidb::PiDBResponse response;
        /*
        //-----BATCH OPERATION
        pidb::PiDBWriteBatch batch;
        pidb::PiDBOperator *oper;


        oper = batch.add_writebatch();
        //设置一个batch的操作 put key1->value1
        oper->set_op(2);
        oper->set_key("key4");
        oper->set_value("value4");

        oper=batch.add_writebatch();
        oper->set_op(2);
        oper->set_key("key5");
        oper->set_value("value5");
        //oper->set_value("value2"); //删除操作不需要设置alue

        cntl.set_log_id(log_id ++);  // set by user
        // Set attachment which is wired to network directly instead of
        // being serialized into protobuf messages.
        cntl.request_attachment().append(FLAGS_attachment);

        // Because `done'(last parameter) is NULL, this function waits until
        // the response comes back or error occurs(including timedout).

        stub.Write(&cntl, &batch, &response, NULL);
        if (!cntl.Failed()) {
            LOG(INFO) << "Received response from " << cntl.remote_side()
                      << " to " << cntl.local_side()
                      << ": " << response.success() << " (attached="
                      << cntl.response_attachment() << ")"
                      << " latency=" << cntl.latency_us() << "us";
        } else {
            LOG(WARNING) << cntl.ErrorText();
        }
        usleep(FLAGS_interval_ms * 1000L);
  //  }

*/

        //GET OPERATION

        /*
            cntl.Reset();
            pidb::PiDBRequest request;

            request.set_key("key5");
            //request.set_value("value");
            stub.Get(&cntl,&request,&response,NULL);
            if(!cntl.Failed()){
                LOG(INFO)<<response.new_value();
            }

            */

        cntl.set_log_id(log_id++);  // set by user

        pidb::PiDBSnapshot snapshot;
        pidb::Empty empty;
        stub.GetSnapshot(&cntl,&empty,&snapshot,NULL);
        if(!cntl.Failed()){
            LOG(INFO)<<snapshot.id();
        } else{
            LOG(ERROR)<<cntl.ErrorText();
        }
        cntl.Reset();
        pidb::Success s;
        stub.ReleaseSnapshot(&cntl,&snapshot,&s,NULL);
        if(!cntl.Failed()){
            LOG(INFO)<<s.success();
        }else{
            LOG(INFO)<<cntl.ErrorText();
        }

    LOG(INFO) << "EchoClient is going to quit";
    return 0;
}