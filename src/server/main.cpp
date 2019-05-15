#include <iostream>
#include <gflags/gflags.h>
#include "server.h"
#include "route_table.h"

//DEFINE_string(data_path,"./data","Path of data stored on");
//DEFINE_int32(port,8100,"port of server to listen on");

int main(int argc,char *argv[]){
    GFLAGS_NS::ParseCommandLineFlags(&argc,&argv,true);
    butil::AtExitManager exit_manager;
    //默认参数


    pidb::RouteTable r;
    r.AddRecord("","a","group1");
    r.AddRecord("c","d","group2");
    r.AddRecord("d","f","group3");
    std::cout<<r.FindRegion("abc");
    LOG(INFO)<<r.FindRegion("abc");
    return 0;

}