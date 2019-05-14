//
// Created by ehds on 19-5-4.

#include "route_table.h"
#include "json11.hpp"
#include <leveldb/env.h>
#include <algorithm>
namespace pidb{

void RouteTable::BuildRouteTable(const std::string &name) const{
    if(router_.empty()){
        //内容为空 //log
        return;
    }
    leveldb::Env *env = leveldb::Env::Default();
    //先将map中的record转化为ector
    std::string jsonstr = json11::Json(router_).dump();
    auto s = leveldb::WriteStringToFile(env,jsonstr,name);
    if(!s.ok()){
        //TO-DO log
    }

}

void RouteTable::AddRecord(const std::string &s, const std::string &l, const std::string group) {
    //暂时先这样，后面需要加入一些判断
    assert(s.compare(l)<=0);
    //还需检查是否有重叠部分
    router_[l] = Record(s,l,group);

}

void RouteTable::ReadRouter(const std::string &name) {
    leveldb::Env *env = leveldb::Env::Default();
    std::string content;
    auto s = leveldb::ReadFileToString(env,name,&content);
    if(!s.ok()){
        //LOG
        return;
    }
    std::string error;
    auto parse = json11::Json::parse(content,error,json11::JsonParse::STANDARD);
    if(!error.empty()){
        //LOG ERROR
        return;
    }
    router_.clear();
    for(const auto & v:parse.object_items()){
        router_[v.first] = {v.first,v.second["limit"].string_value(),v.second["group"].string_value()};
    }

}

const std::string & RouteTable::FindRegion(const std::string &key) const {
    if (router_.size()==0)
        return NULL ;
    //("“, a),[a,c),[c,e),[e,"")
    //因为我们使用limit作为标识，所以直接通过upper_bound来找到和是的region
    //假设key为b,查找发现位于第二个region，因为b<c
    auto res = router_.upper_bound(key);
    //如果没有找到，检查最后一个的region的limit是否为无穷大
    if(res!=router_.end())
        return res->second.group;

    //没有找到合适的上界，检查是否有无穷的limit
    if(router_.find("")!=router_.end()) {
        //有limit为无穷大的region
        assert(key>router_[""].smallest);
        return router_.find(key)->second.group;
        //需要判断
    }
    return NULL;
}
const std::string & RouteTable::operator[](const std::string &key) const {

    return router_.find(key) == router_.end()?NULL:router_.find(key)->second.group;
}

}//namespace pidb
