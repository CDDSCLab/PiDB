# db-project
# 第一个实例
实现了一个极简的demo，能够实现leveldb的Put和Write
## 所依赖的库
1. [brpc](https://github.com/apache/incubator-brpc)
    注意安装brpc的依赖的时候 非ubuntu系统的protobuf版本要保持在3.0.0~3.6.1以内
2. [braft](https://github.com/brpc/braft)
## demo演示
在demo文件夹下
```
mkdir bld && cd bld && cmake .. && make
```
然后运行
```
run_server.sh 和 run_client.sh
```



