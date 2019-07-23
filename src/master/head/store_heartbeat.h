#ifndef PIDB_STORE_HEARTBEAT_H
#define PIDB_STORE_HEARTBEAT_H

#include <iostream>
#include <vector>
#include <map>
#include <string>
#include <bthread/bthread.h>
#include "./json11.hpp"

using std::string;

namespace  pidb{
class StoreHeartbeat {
private:
	// 一个store心跳的内容结构体
	struct StoreInfo{
		int region_num_;      // store上的region数量
		int leader_num_;      // store上的leader数量
	    bool is_alive_;       // 用于master确定store是否活着
		StoreInfo(){};
		StoreInfo(int r, int l):region_num_(r), leader_num_(l), 
			is_alive_(false) {}
		~StoreInfo() {}

		// 自动把此结构体转成json格式（必须以to_json命名）
		json11::Json to_json() const {
			return json11::Json::object{{"region_num",region_num_},
										{"leader_num",leader_num_}};
		}
	};

	std::map<string, StoreInfo> store_table_; // 存放所有store信息，string是store的addr
	bthread_mutex_t mutex_;     // 一把互斥锁
	bthread_mutex_t mutex1_;     // 一把互斥锁

    std::vector<string> least_region_; // 维护一个region数目递增的store表

	StoreHeartbeat();
	static StoreHeartbeat* s;

	// 往least_region_里增加、删除一条记录
	void AddLeast(const string& addr, int region_num);
	void DeleteLeast(const string& addr);

public:
	static StoreHeartbeat* Initial() {
		return s;
	}

	// 根据addr更新store的相关信息以及least表，(addr存在就修改，否则新增)
	// 另外若region_num 或者 leader_num为-1则代表不需要修改。
	// 返回值0表示新增，1表示修改
	int UpdateStoreInfo(const string& addr, int region_num, 
		int leader_num);
	
	// 获取store表条目数
	int GetSize();

	// 获取store信息用于调度(主要是store的addr)
	// size是需要的store的数量，is_new区分是新增还是删除
	// vin是传入的store地址，vout是传出的store地址
	// 返回值表示是否成功获取了完整信息(0成功)
	int GetStoreInfo(int size, const std::vector<string>* vin,
		std::vector<string> *vout, bool is_new);

    // 根据addr检查store是否活着（根据is_alive_标志）
	// 若store还活着则设置标志为假（每个心跳会设置为真）
	// 若store已死，清理它的信息
    static bool CheckIsAlive(void* addr);
	
	// Store信息写入磁盘持久化
	bool WriteToFile(const string& file_name);

	// 尝试从磁盘文件读取Store信息（一般用于启动时）
	int ReadFromFile(const string& file_name);

	// 清空store信息
	void Clear();
};
}// namespace pidb

#endif //PIDB_STORE_HEARTBEAT_H
