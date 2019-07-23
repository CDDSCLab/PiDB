# 此文件应该放在源文件目录下
cmake_minimum_required (VERSION 2.8.10)
project (db)

# 头文件目录
include_directories("../include")

# 源文件目录放进工程
file(GLOB SRCS "./head/*")

# 库目录（前者本地，后者系统）
link_directories("../libs")
link_directories("/usr/lib")

# 可执行文件
add_executable(client test_client.cpp ${SRCS})
add_executable(server master_main.cpp ${SRCS})

# 连接库(注意顺序)
target_link_libraries(client libbraft.so libbrpc.so libgflags.so libprotobuf.so libleveldb.a libpthread.so libsnappy.so)
target_link_libraries(server libbraft.so libbrpc.so libgflags.so libprotobuf.so libleveldb.a libpthread.so libsnappy.so)

