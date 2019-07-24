#include "pidb_service_impl.h"
#include "server.h"
#include "brpc/controller.h"
#include <brpc/stream.h>
#include <leveldb/env.h>
#include <leveldb/db.h>

namespace pidb {

    class StreamReceiver : public brpc::StreamInputHandler {
    public:
        StreamReceiver(leveldb::WritableFile *file):file_(file){
            LOG(INFO)<<"INIT FILE";
        }
        virtual int on_received_messages(brpc::StreamId id,
                                         butil::IOBuf *const messages[],
                                         size_t size) {
            //std::ostringstream os;
            assert(file!= nullptr);
            for (size_t i = 0; i < size; ++i) {
                // os << "msg[" << i << "]=" << *messages[i];
                //LOG(INFO)<<*messages[i];
                file_->Append(messages[i]->to_string());
            }
            //LOG(INFO) << "Received from Stream=" << id << ": " << os.str();
            return 0;
        }

        virtual void on_idle_timeout(brpc::StreamId id) {
            LOG(INFO) << "Stream=" << id << " has no data transmission for a while";
        }

        virtual void on_closed(brpc::StreamId id) {
            LOG(INFO) << "Stream=" << id << " is closed";
            file_->Flush();
            delete file_;
            delete this;
            //file_->Close();
        }

        ~StreamReceiver(){
            LOG(INFO)<<"HANDLER IS DONE";
        }

    private:
        leveldb::WritableFile *file_;
    };


    void PiDBServiceImpl::Put(::google::protobuf::RpcController *controller,
                                const ::pidb::PiDBRequest *request,
                                ::pidb::PiDBResponse *response,
                                ::google::protobuf::Closure *done) {
        //brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);
        server_->Put(request, response, done);

    }

    void PiDBServiceImpl::Get(::google::protobuf::RpcController *controller,
                               const ::pidb::PiDBRequest *request,
                               ::pidb::PiDBResponse *response,
                               ::google::protobuf::Closure *done) {
        //auto cntl = static_cast<brpc::Controller *>(controller);
        brpc::ClosureGuard done_guard(done);
        server_->Get(request, response, done);
    }

    void PiDBServiceImpl::Write(::google::protobuf::RpcController *controller,
                                const ::pidb::PiDBWriteBatch *request,
                                ::pidb::PiDBResponse *response,
                                ::google::protobuf::Closure *done) {

        //brpc是使用static_cast转化，这是不安全的，https://docs.microsoft.com/en-us/cpp/cpp/static-cast-operator?view=vs-2019
        //应该使用dynamic_cast
       // auto cntl = static_cast<brpc::Controller *>(controller);
        auto req = request;
        auto batch_size = req->writebatch_size();
        if(batch_size<1){
            response->set_success(false);
            return;
        }
        return server_->Write(request,response,done);

    }

    void PiDBServiceImpl::GetSnapshot(::google::protobuf::RpcController *controller,
                                     const ::pidb::Empty *request,
                                      ::pidb::PiDBSnapshot *response,
                                      ::google::protobuf::Closure *done){
        brpc::ClosureGuard done_guard(done);

        auto id =  server_->GetSnapshot();
        response->set_id(id);

    }

    void PiDBServiceImpl::ReleaseSnapshot(::google::protobuf::RpcController *controller,
                                          const ::pidb::PiDBSnapshot *request,
                                          ::pidb::Success *response,
                                          ::google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        auto id = request->id();
        auto s = server_->ReleaseSnapshot(id);
        if(s.ok()){
            response->set_success(true);
            LOG(INFO)<<"RELEASE SUCCESS";
        }else{
            response->set_success(false);
            response->set_message(s.ToString());
        }

    }

    void PiDBServiceImpl::GetIterator(::google::protobuf::RpcController *controller,
                                      const ::pidb::PiDBIterator *request,
                                      ::pidb::PiDBIterator *response,
                                      ::google::protobuf::Closure *done) {

        brpc::ClosureGuard done_guard(done);
        auto id = server_->GetIterator(request->start(),request->stop());
        LOG(INFO)<<id;
        response->set_id(id);

    }
    void PiDBServiceImpl::Iterate(::google::protobuf::RpcController *controller,
                                const ::pidb::PiDBIterator *request,
                                  ::pidb::PiDBResponse *response,
                                  ::google::protobuf::Closure *done) {
        brpc::ClosureGuard self_guard(done);
        auto id = request->id();
        std::string value;
        auto s = server_->Next(id,&value);
        if (s.ok()) {
            response->set_success(true);
            response->set_new_value(value);
        }else{
            LOG(INFO)<<s.ToString();
            response->set_success(false);
        }
    }
    void PiDBServiceImpl::RequestFile(::google::protobuf::RpcController *controller,
                                      const ::pidb::PiDBFileRequest *request, ::pidb::PiDBFileResponse *response,
                                      ::google::protobuf::Closure *done) {
        brpc::ClosureGuard self_gurad(done);
        LOG(INFO)<<"Reques file"<<request->filename();

        //TODO 判断是否有文件
        response->set_success(true);
    }


    void PiDBServiceImpl::PushFile(::google::protobuf::RpcController *controller,
                                   const ::pidb::PiDBFileRequest *request, ::pidb::PiDBFileResponse *response,
                                   ::google::protobuf::Closure *done) {

        brpc::ClosureGuard done_guard(done);

        brpc::Controller* cntl =
                static_cast<brpc::Controller*>(controller);
        brpc::StreamOptions stream_options;
        leveldb::WritableFile *file;

        std::string path = "./receive/test.txt";

        leveldb::Env *env = leveldb::Env::Default();

        env->NewWritableFile(path,&file);

        StreamReceiver* r=new StreamReceiver(file);
        stream_options.handler = r;
        brpc::StreamId _sd;
        if (brpc::StreamAccept(&_sd, *cntl, &stream_options) != 0) {
            cntl->SetFailed("Fail to accept stream");
            LOG(INFO)<<"STREAM ID IS DONE"<<_sd;
            return;
        }
        response->set_success(true);

    }

}
//TO-DO 加入新的功能。