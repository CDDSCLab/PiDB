# PiDB-distributed leveldb 
[![Build Status](https://travis-ci.com/CDDSCLab/PiDB.svg?branch=master)](https://travis-ci.com/github/CDDSCLab/PiDB)
## 所依赖的库
1. [brpc](https://github.com/apache/incubator-brpc)
2. [braft](https://github.com/brpc/braft)

## 关于测试
1. 请将所有的单元测试写在根目录的test文件夹下
2. 使用gtest的测试框架，具体请参考gtest的用法
3. 注意一些包的依赖，比如我要测试pidb server的一些功能，
要链接pidb的库文件


## 关于client
1. client最为和pidb的一个独立的库的存在
 所以编译文件和测试都是单独的

## TODO
1. Iterator 的操作
    1. 依照目前已经实现的Snapshot形式，返回id的形式标识不同用户的iterator
    2. 具体功能与leveldb的功能一样（next，pre，seek）
    3. 客户端需要根据当前遍历的结果做出调整，因为数据库是分region的，每个Server的region的key是不连续的
    所以每个region会有一个iterator(smallest_key,largest_key),客户端需要根据当前region是否已经到头，选择下一个
    region等
    4. 优化：每次next的时候可以获得一个batch的大小，减少访问次数



## 第一个实例
实现了一个极简的demo，能够实现leveldb的Put和Write

demo演示
在demo文件夹下
```
mkdir bld && cd bld && cmake .. && make
```
然后运行
```
run_server.sh 和 run_client.sh
```
