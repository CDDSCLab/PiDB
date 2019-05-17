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
将运行脚本cp到文件夹下：
```
cp *.sh ./bld
```
然后运行
```
run_server.sh 和 run_client.sh
```


# 关于测试
1. 请将所有的单元测试写在根目录的test文件夹下
2. 使用gtest的测试框架，具体请参考gtest的用法
3. 注意一些包的依赖，比如我要测试pidb server的一些功能，
要链接pidb的库文件

