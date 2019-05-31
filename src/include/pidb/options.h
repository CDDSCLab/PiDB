//
// Created by ehds on 19-5-4.
//

#ifndef PIDB_OPTIONS_H
#define PIDB_OPTIONS_H

#include <stddef.h>
#include <string>

namespace pidb {

    struct RaftOption
    {
        std::string group;
        std::string conf;
        std::string data_path;
        int32_t port;
        RaftOption():data_path("./data"){}

    };

    struct ServerOption{
        std::string data_path;
        int32_t port;
        int heartbeat_timeout_ms;
        //默认的配置
        ServerOption(const std::string &path,int32_t p)
            :data_path(path),port(p){};
        ServerOption():data_path("./data"),port(8100),heartbeat_timeout_ms(5000){}
    };

}


#endif //PIDB_OPTIONS_H
