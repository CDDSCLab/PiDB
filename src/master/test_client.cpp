#include <gflags/gflags.h>
#include <bthread/bthread.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <braft/raft.h>
#include <braft/util.h>
#include <braft/route_table.h>
#include <iostream>
#include "master.pb.h"

DEFINE_int32(timeout_ms, 1000, "请求超时时间");
DEFINE_string(conf, "", "raft组的配置信息（地址）");
DEFINE_string(group, "master", "raft组的ID");

using namespace pidb;

bvar::LatencyRecorder g_latency_recorder("block_client");

static void* sender(void* arg) {
    braft::PeerId leader;
    while (!brpc::IsAskedToQuit()) {
        // 从路由表选择leader
        if (braft::rtb::select_leader(FLAGS_group, &leader) != 0) {
            // 如果没有通过发rpc选举leader
            butil::Status st = braft::rtb::refresh_leader(
                    FLAGS_group, FLAGS_timeout_ms);
            if (!st.ok()) {
                // 若没选出来，等会儿再选
                LOG(WARNING) << "选举失败：" << st;
                bthread_usleep(FLAGS_timeout_ms * 1000L);
            }
            continue;
        }
    }
    // 现在有leader了,构建stub然后发rpc
    brpc::Channel channel;
    if (channel.Init(leader.addr, NULL) != 0) {
        LOG(ERROR) << "初始化通道失败：" << leader;
        bthread_usleep(FLAGS_timeout_ms * 1000L);
        return NULL;
    }
    MasterService_Stub stub(&channel);
    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_timeout_ms);

    // 发送请求
    PiDBClientResponse response;
    PiDBClientRequest request;
    request.set_key("bb");
    stub.QueryRoute(&cntl, &request, &response, NULL);

    // 出错处理
    if (cntl.Failed()) {
        LOG(WARNING) << "Fail to send request to " 
            << leader << " : " << cntl.ErrorText();
        // 更新leader
        // braft::rtb::update_leader(FLAGS_group, braft::PeerId());
        return NULL;
    }
    if (!response.success()) {
        if(response.has_redirect()){
            LOG(WARNING) << "Fail to send request to " 
            << leader << ", redirecting to " << response.redirect();
        // 更新leader
        // braft::rtb::update_leader(FLAGS_group, response.redirect());
        }
        else{
            LOG(WARNING) << "send success but hehehehehe";
        }
       
        return NULL;
    }
    LOG(INFO) << response.leader_addr();

    return NULL;
}

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    butil::AtExitManager exit_manager;

    // Register configuration of target group to RouteTable
    if (braft::rtb::update_configuration(FLAGS_group, FLAGS_conf) != 0) {
        LOG(ERROR) << "Fail to register configuration " << FLAGS_conf
                   << " of group " << FLAGS_group;
        return -1;
    }

    bthread_t t;
    if (bthread_start_background(&t, NULL, sender, NULL) != 0) {
        LOG(ERROR) << "Fail to create bthread";
        return -1;
    }

    while (!brpc::IsAskedToQuit()) {
        sleep(3);
        LOG(INFO) << "正常工作中……";
    }

    LOG(INFO) << "客户端已退出";
    bthread_join(t, NULL);

    return 0;
}