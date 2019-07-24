//
// Created by ehds on 19-5-23.
//
#include "client.h"
#include "remote_file_send.h"

int main(int argc, char* argv[]) {
    pidb::RemoteFileSend a("127.0.0.1:8200","./datatest/test.txt");
    a.start();
    LOG(INFO)<<a.status().ToString();
    return 0;
}