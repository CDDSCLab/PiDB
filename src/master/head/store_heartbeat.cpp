#include <algorithm>
#include <fstream>
#include <iterator>
#include <unistd.h>    // sleep.h
#include "store_heartbeat.h"

namespace pidb{

StoreHeartbeat* StoreHeartbeat::s = new StoreHeartbeat;

StoreHeartbeat::StoreHeartbeat() {
	bthread_mutex_init(&this->mutex_, NULL);
	bthread_mutex_init(&this->mutex1_, NULL);
}

void StoreHeartbeat::AddLeast(const string& addr, int region_num){
	bthread_mutex_lock(&this->mutex1_);
	for(auto iter = least_region_.begin(); 
			iter != least_region_.end(); ++iter){
		int tmp = store_table_[*iter].region_num_;
		if(region_num < tmp){
			least_region_.insert(iter, addr);
			bthread_mutex_lock(&this->mutex1_);
			return;
		}
	}
	// 没找到合适的位置
	least_region_.insert(least_region_.end(), addr);
	bthread_mutex_unlock(&this->mutex1_);
}
void StoreHeartbeat::DeleteLeast(const string& addr){
	bthread_mutex_lock(&this->mutex1_);
	auto iter = std::find(least_region_.begin(),
			least_region_.end(), addr);
	if(iter != least_region_.end()) least_region_.erase(iter);
	bthread_mutex_unlock(&this->mutex1_);
}

int StoreHeartbeat::UpdateStoreInfo(const string& addr, 
		int region_num, int leader_num) {
	int return_value = 1; // 返回值
	bthread_mutex_lock(&this->mutex_);
	// 新store
	if(store_table_.find(addr) == store_table_.end()){
		StoreInfo s(region_num, leader_num);
		this->store_table_.insert({addr, s});
		return_value = 0;
		// 然后更新least表
		AddLeast(addr, region_num);
	}
	// 旧store
	else {
		if(region_num != -1) store_table_[addr].region_num_ = region_num;
		if(leader_num != -1) store_table_[addr].leader_num_ = leader_num;
		store_table_[addr].is_alive_ = true;
		// 然后更新least表
		if(region_num != -1){
			DeleteLeast(addr);
			AddLeast(addr, region_num);
		}
	}
	bthread_mutex_unlock(&this->mutex_);
	return return_value;
}

void  StoreHeartbeat::Clear() {
	this->store_table_.clear();
}

int  StoreHeartbeat::GetSize() {
	return this->store_table_.size();
}

int StoreHeartbeat::GetStoreInfo(int size, const std::vector<string>* vin,
		std::vector<string> *vout, bool is_new){
	// 如果新增，要找不在vin里面的store(region最少的)
	bthread_mutex_lock(&this->mutex_);
	if(is_new){
		if(least_region_.size() < size) return -1;
		auto iter = least_region_.begin();
		if(vin == nullptr){
			for(int i = 0; i<size, iter != least_region_.end();
					++iter, ++i)
				vout->push_back(*iter);
		}
		else{
			for(int i = 0; i<size, iter != least_region_.end();++iter){
				auto iter1 = std::find(vin->begin(), vin->end(), *iter);
				if(iter1 == vin->end()){
					vout->push_back(*iter);
					++i;
				}
			}
		}
		bthread_mutex_unlock(&this->mutex_);
		if(vout->size() == size) return 0;
		else return -1;
	}
    // 如果删除,要找在vin里面的store(region最多的)
	else{
		// 从后往前找，找的就是region最多的
		auto iter = least_region_.end()-1;
		for(int i = 0; i<size, iter != least_region_.begin();--iter){
			auto iter1 = std::find(vin->begin(), vin->end(), *iter);
			if(iter1 != vin->end()){
				vout->push_back(*iter);
				++i;
			}
		}
		bthread_mutex_unlock(&this->mutex_);
		if(vout->size() == size) return 0;
		else return -1;
	}
}

bool StoreHeartbeat::CheckIsAlive(void* addr){
	StoreHeartbeat* s = StoreHeartbeat::Initial();
	string* addr_ = (string*)addr;
	bthread_mutex_lock(&s->mutex_);
	if(s->store_table_.find(*addr_) != s->store_table_.end()){
		if(s->store_table_[*addr_].is_alive_ == true){
			s->store_table_[*addr_].is_alive_ = false;
			bthread_mutex_unlock(&s->mutex_);
			return true;
		}
		else {
			s->store_table_.erase(*addr_);
			delete addr_; // 释放内存
			bthread_mutex_unlock(&s->mutex_);
			return false;
		}
	}
}

bool StoreHeartbeat::WriteToFile(const string& file_name) {
	std::ofstream out(file_name, std::fstream::out | std::fstream::binary);
	if (!out) return false;

	string json_str = json11::Json(this->store_table_).dump();
	out.write(json_str.c_str(), json_str.size());
	return true;
}

int StoreHeartbeat::ReadFromFile(const string& file_name) {
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

	// 应该是map格式
	auto map_object = json.object_items();
	for (auto& object : map_object) {
		StoreInfo store_info( object.second["region_num"].int_value(), 
			object.second["leader_num"].int_value());
		this->store_table_.insert({object.first, store_info});
	}
	return 0;
}
}//namespace pidb