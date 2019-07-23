//
// Created by ehds on 19-5-18.
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

    //初始化stub
    pidb::PiDBService_Stub stub(&channel);

    // Send a request and wait for the response every 1 second.
    int log_id = 0;


        brpc::Controller cntl;
        pidb::PiDBResponse response;

        //-----BATCH OPERATION
//        pidb::PiDBWriteBatch batch;
//        pidb::PiDBOperator *oper;
//
//
//        oper = batch.add_writebatch();
//        //设置一个batch的操作 put key1->value1
//        oper->set_op(2);
//        oper->set_key("key4");
//        oper->set_value("value4");
//
//        oper=batch.add_writebatch();
//        oper->set_op(2);
//        oper->set_key("key5");
//        oper->set_value("value5");
//        //oper->set_value("value2"); //删除操作不需要设置alue
//
//        cntl.set_log_id(log_id ++);  // set by user
//        // Set attachment which is wired to network directly instead of
//        // being serialized into protobuf messages.
//        //cntl.request_attachment().append(FLAGS_attachment);
//
//        // Because `done'(last parameter) is NULL, this function waits until
//        // the response comes back or error occurs(including timedout).
//
//        stub.Write(&cntl, &batch, &response, NULL);
//        if (!cntl.Failed()) {
//            LOG(INFO) << "Received response from " << cntl.remote_side()
//                      << " to " << cntl.local_side()
//                      << ": " << response.success() << " (attached="
//                      << cntl.response_attachment() << ")"
//                      << " latency=" << cntl.latency_us() << "us";
//            if(response.success())
//                LOG(INFO)<<"Put operation success";
//        } else {
//            LOG(WARNING) << cntl.ErrorText();
//        }
//        cntl.Reset();
//        //获得snapshot
//        pidb::PiDBSnapshot *snapshot = new pidb::PiDBSnapshot();
//      // pidb::PiDBSnapshot snapshot ;
//       pidb::Empty empty;
//        stub.GetSnapshot(&cntl,&empty,snapshot,NULL);
//        if(!cntl.Failed()){
//            LOG(INFO)<<snapshot->id();
//        } else{
//            LOG(ERROR)<<cntl.ErrorText();
//        }
//
//        usleep(FLAGS_interval_ms * 1000L);
//
//        cntl.Reset();
//
//        pidb::PiDBRequest request;
//
//        request.set_key("key5");
//        request.set_value("value_new");
//        stub.Put(&cntl,&request,&response,NULL);
////
//        cntl.Reset();
//        request.set_key("key5");
//        request.set_allocated_snapshot(snapshot);
//
//
//        //request.set_allocated_snapshot(&snapshot);
//
//        LOG(INFO)<<request.has_snapshot();
//
//
//        stub.Get(&cntl,&request,&response,NULL);
//        if (!cntl.Failed())
//            LOG(INFO)<<response.new_value();
//
//        request.clear_snapshot();



        //delete snapshot;
//        cntl.Reset();
//        pidb::Success s;
//        stub.ReleaseSnapshot(&cntl,&snapshot,&s,NULL);
//        if (!cntl.Failed()){
//            LOG(INFO)<<s.message();
//        }
//        request.clear_snapshot();

        //GET OPERATION


            pidb::PiDBRequest request;

            request.set_key("key5");
            //request.set_value("value");
            stub.Get(&cntl,&request,&response,NULL);
            if(!cntl.Failed()){
                LOG(INFO)<<"Get Key:"<<request.key()<<" Value: "<<response.new_value();
            }


        // set by user
        //Snapshot
        /*
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

        cntl.Reset();
        pidb::PiDBIterator iterator;
        iterator.set_id(-1);
        iterator.set_start("a");
        stub.GetIterator(&cntl,&iterator,&iterator,NULL);
        if(!cntl.Failed()){
            LOG(INFO)<<iterator.id();
        }
    iterator.set_id(iterator.id());
        cntl.Reset();

     stub.Iterate(&cntl,&iterator, &response,NULL);

    if(!cntl.Failed()){
        LOG(INFO)<<response.new_value();
    } else{
        LOG(INFO)<<cntl.ErrorText();
    }
    */

}