// 路由表即存放region的路由信息，有两种（分成两张表）
// 一种正常的，既含leader又有完整数据
// 一种不正常的，可能没有leader，或者有leader但无完整数据

#ifndef PIDB_ROUTE_TABLE_H
#define PIDB_ROUTE_TABLE_H

#include <iostream>
#include <vector>
#include <string>
#include <bthread/bthread.h>
#include "./json11.hpp"

using std::string;

namespace  pidb{
class RouteTable;

// 用于测活的参数封装，因为看门狗那里只有一个参数
struct CheckRAlive{
	RouteTable* r_;
	string group_;
	CheckRAlive(RouteTable* r, const string& group):
		r_(r),group_(group){};
};

class RouteTable {
private:
	// 路由表存放的内容结构体
	struct RouteInfo {
		string min_key_;      // 最小key值
		string max_key_;      // 最大key值
		string raft_group_;   // 所属raft组名
		string leader_addr_;  // 所属raft的leader地址
		string conf_;         // 所属raft组配置
		// 路由记录状态
		// 0完全正常，-4为彻底凉凉
		// 1普通正常（要分裂的region的新raft刚刚选出leader，
		// 老region的范围要强制缩小，需要标识以防止心跳改变
		// -1为无leader(单纯新增region)
		// -2为无leader（分裂新增的region）
		// -3为有leader但数据不完整（分类新增的leader）
		int state_;  
		bool is_alive;        // 某个region是否活着 
	    
		RouteInfo(const string& min, const string& max, 
			const string& r, const string& l, const string& c, int s):
			min_key_(min), max_key_(max), raft_group_(r),
			leader_addr_(l), conf_(c), state_(s), is_alive(false) {}
		~RouteInfo() {}

		// 自动把此结构体转成json格式（必须以to_json命名）
		json11::Json to_json() const {
			return json11::Json::object{{"min_key",min_key_},
										{"max_key",max_key_},
										{"raft_group",raft_group_},
										{"leader_addr",leader_addr_},
										{"conf", conf_},
										{"state", state_}};
		}
	};
	std::vector<RouteInfo> route_table_;  // 存放整个路由表
	bthread_mutex_t mutex_;  // 一把互斥锁

public:
	RouteTable();
	~RouteTable();

	// 更新路由信息，state意义详见类成员定义
	// 若已存在就修改并返回1，若新增返回0
	int UpdateRouteInfo(const string& min_key, const string& max_key, 
		const string& raft_group, const string& leader_addr,
		const string& conf, int state);

	// 根据group删除某条路由记录
	void RemoveRouteInfo(const string& raft_group);

    // 根据group获取state情况
	int GetState(const string& raft_group); 
	// 根据group获取leader_addr
	bool GetAddr(const string& raft_group, string& addr);
	// 根据group获取conf
	bool GetConf(const string& raft_group, string& conf);
	// 返回路由表条目数
    int GetSize();
	// 根据group名获取对应region的范围
	bool GetRange(const string& raft_group, string& min_key,
		string& max_key);
	
	// 从路由表查询记录，state意义详见类成员定义
	bool GetRouteInfo(const string& key, string& raft_group, 
		string& leader_addr, string& conf, int& state);

    // 根据group检查store是否活着（根据is_alive_标志）
	// 若store还活着则设置标志为假（每个心跳会置真）,并返回true
	// 若store已死，清理它的信息，并返回false
    static bool CheckIsAlive(void* check_alive);

	// 整个路由表写入磁盘持久化
	// 传入参数 file_name 写入文件的路径+文件名
	bool WriteToFile(const string& file_name);

	// 尝试从磁盘文件读入路由表（一般用于启动时）
	// 传入参数 file_name   读取文件的路径+文件名
	// 返回值 -1表示打开文件失败，-2表示解析文件失败，0表示成功
	int ReadFromFile(const string& file_name);

	// 清空路由
	void Clear();
};
}// namespace pidb

#endif //PIDB_ROUTE_TABLE_H
