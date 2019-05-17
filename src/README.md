# server目录

完成功能
## 一 server端
1. server的心跳（收集信息发给PD)
2. server对raft的管理
3. routetable

## levedb功能补充
1. 增加range的dumpfile功能

## raft
1. leveldb的写操作
2. iterator
3. snapshot

# 关于测试
1. 请将所有的单元测试写在根目录的test文件夹下
2. 使用gtest的测试框架，具体请参考gtest的用法
3. 注意一些包的依赖，比如我要测试pidb server的一些功能，
要链接pidb的库文件

