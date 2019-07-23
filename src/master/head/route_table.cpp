#include <algorithm>
#include <fstream>
#include <iterator>
#include "route_table.h"

namespace pidb{
RouteTable::RouteTable() {
	bthread_mutex_init(&this->mutex_, NULL);
}
RouteTable::~RouteTable() {
}

int RouteTable::UpdateRouteInfo(const string& min_key, 
		const string& max_key, const string& raft_group,
		const string& leader_addr, const string& conf, int state) {
	bthread_mutex_lock(&this->mutex_);
	// 找到第一个min_key_≥min_key的记录位置
	auto iter = std::lower_bound(route_table_.begin(), 
		route_table_.end(), min_key, [](const RouteInfo& r, 
		const string& key)->bool{return r.min_key_ < key;});
	// 确认找到
	if(iter != route_table_.end() && iter->min_key_ == min_key){
		// 特殊情况
		if(iter->state_ == 1) iter->conf_ = conf;
		else{
			iter->max_key_ = max_key;
			iter->raft_group_ = raft_group;
			iter->leader_addr_ = leader_addr;
			iter->conf_ = conf;
			iter->state_ = state;
			iter->is_alive = true;
		}
		bthread_mutex_unlock(&this->mutex_);
		return 1;
	}
	// 否则新增
	else{
		RouteInfo tmp(min_key, max_key, raft_group, 
			leader_addr, conf, state);
		route_table_.insert(iter, tmp);
		bthread_mutex_unlock(&this->mutex_);
		return 0;
	}
	
}

void RouteTable::RemoveRouteInfo(const string& raft_group){
	bthread_mutex_lock(&this->mutex_);
	for(auto iter = route_table_.begin(); 
			iter != route_table_.end(); ++iter)
		if(iter->raft_group_ == raft_group){
			route_table_.erase(iter);
			break;
		}
	bthread_mutex_unlock(&this->mutex_);
}

int RouteTable::GetState(const string& raft_group){
	bthread_mutex_lock(&this->mutex_);
	for(auto iter = route_table_.begin(); 
			iter != route_table_.end(); ++iter)
		if(iter->raft_group_ == raft_group) {
			bthread_mutex_unlock(&this->mutex_);
			return iter->state_;
		}
	bthread_mutex_unlock(&this->mutex_);
	return -4; //找不到就凉凉
}

bool RouteTable::GetAddr(const string& group, string& addr){
	bthread_mutex_lock(&this->mutex_);
	for(auto iter = route_table_.begin(); 
			iter != route_table_.end(); ++iter)
		if(iter->raft_group_ == group && iter->leader_addr_!="") {
			addr = iter->leader_addr_;
			bthread_mutex_unlock(&this->mutex_);
			return true;
		}
	bthread_mutex_unlock(&this->mutex_);
	return false;
}

bool RouteTable::GetConf(const string& group, string& conf){
	bthread_mutex_lock(&this->mutex_);
	for(auto iter = route_table_.begin(); 
			iter != route_table_.end(); ++iter)
		if(iter->raft_group_ == group) {
			conf = iter->conf_;
			bthread_mutex_unlock(&this->mutex_);
			return true;
		}
	bthread_mutex_unlock(&this->mutex_);
	return false;
}

int RouteTable::GetSize() {
	return route_table_.size();
}

void  RouteTable::Clear() {
	this->route_table_.clear();
}

bool RouteTable::GetRouteInfo(const string& key, string& raft_group,
		string& leader_addr, string& conf, int& state) {
	bthread_mutex_lock(&this->mutex_);
	// 排除路由表为空的情况
	if(route_table_.size() == 0){
		bthread_mutex_unlock(&this->mutex_);
	    return false;
	}
	// 找到第一个min_key_>key的记录位置
	auto iter = std::upper_bound(route_table_.begin(), 
		route_table_.end(), key, [](const string& key, 
		const RouteInfo& r)->bool{return key < r.min_key_;});
	// 没找到但存在（所有min_key都≤key但最后一条max_key>key）
	if(iter == route_table_.end()){
		if((iter - 1)->max_key_ > key || (iter - 1)->max_key_ == ""){
			raft_group = iter->raft_group_;
			leader_addr = iter->leader_addr_;
			conf = iter->conf_;
			state = iter->state_;
			bthread_mutex_unlock(&this->mutex_);
	        return true;
		}
	}
	// 找到了且存在（排除路由表断层的情况）
	else if(iter != route_table_.begin() && (iter - 1)->max_key_ > key){
		raft_group = iter->raft_group_;
		leader_addr = iter->leader_addr_;
		conf = iter->conf_;
		state = iter->state_;
		bthread_mutex_unlock(&this->mutex_);
	    return true;
	}
	// 到这的都是不存在的情况
	bthread_mutex_unlock(&this->mutex_);
	return false;
}

bool RouteTable::GetRange(const string& group,string& min_key,
		string& max_key){
	// 遍历寻找
	for(auto& r : route_table_)
		if(r.raft_group_ == group){
			min_key = r.min_key_;
			max_key = r.max_key_;
			return true;
		}
	return false;
}

bool RouteTable::CheckIsAlive(void* check_alive){
    RouteTable* r = ((CheckRAlive*)check_alive)->r_;
	string group = ((CheckRAlive*)check_alive)->group_;
	bthread_mutex_lock(&r->mutex_);
	for(auto iter = r->route_table_.begin(); 
			iter != r->route_table_.end(); ++iter){
		if(iter->raft_group_ == group){
			if(iter->is_alive == true){
				iter->is_alive = false;
				bthread_mutex_unlock(&r->mutex_);
				return true;
			}
			else {
				r->route_table_.erase(iter);
				bthread_mutex_unlock(&r->mutex_);
				return false;
			}
		}
	}
}

bool RouteTable::WriteToFile(const string& file_name) {
	std::ofstream out(file_name, std::fstream::out | std::fstream::binary);
	if (!out) return false;

	string json_str = json11::Json(this->route_table_).dump();
	out.write(json_str.c_str(), json_str.size());
	return true;
}

int RouteTable::ReadFromFile(const string& file_name) {
	std::ifstream in(file_name, std::fstream::in | std::fstream::binary);
	int times = 3;   // 尝试3次打开文件（若失败）
	while(!in && times != 0){
		sleep(1);
		times-- ;
		in.clear();
		in.open(file_name, std::fstream::in | std::fstream::binary);
	}
	if (!in) return -1;

	// 从文件中读取数据并解析成json格式
	std::istreambuf_iterator<char> start(in), end;
	string data(start, end);
	in.close();
	string error;
	auto json = json11::Json::parse(data, error);
	if (!error.empty()) return -2;

	// 应该是数组（vector）格式
	auto array = json.array_items();
	for (auto& object : array) {
		RouteInfo route_info(object["min_key"].string_value(), 
			object["max_key"].string_value(), object["raft_group"].string_value(),
			object["leader_addr"].string_value(),object["conf"].string_value(),
			object["state"].int_value());
		this->route_table_.push_back(route_info);
	}
	return 0;
}
}//namespace pidb