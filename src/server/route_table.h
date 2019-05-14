//
// Created by ehds on 19-5-4.
//

#ifndef PIDB_ROUTE_TABLE_H
#define PIDB_ROUTE_TABLE_H
#include <map>
#include <string>
#include <json11/json11.hpp>

//用与存储 region的路由信息
// 对于每一个region,由最大的key来标识，对于该region所存储的值必须不小于smallest
//这样的设计方便查找key所处的范围
//如果smallest = “” 代表最小的key ，largest=“” 代表最大的值


namespace  pidb{

class RouteTable {
public:
    RouteTable() = default;

    RouteTable(const RouteTable&) = delete;
    RouteTable& operator =(const RouteTable &)= delete;

    ~RouteTable() = default;

    void BuildRouteTable(const std::string &name) const;
    void AddRecord(const std::string &s,const std::string &l,std::string group);
    void SplitRecord(const std::string &a,const std::string &newKey);
    void ReadRouter(const std::string &name);
    const std::string &FindRegion(const std::string &key) const;
    const std::string &operator [](const std::string &key) const;


private:
    //使用maps 来代表routetable的取值范围
    struct Record{
        std::string smallest;
        std::string limit;
        std::string group;
        Record(){}
        Record(const std::string &s,const std::string &l,const std::string &g)
        :smallest(std::move(s)),limit(std::move(l)),group(std::move(g)){}
        json11::Json to_json() const{
            return json11::Json::object{{"smallest",smallest},{"limit",limit},{"group",group}};
        }

    };
    std::map<std::string,Record> router_; // limitkey -> smallest,regionGrou
    std::vector<Record> routers_;
};
}// namespace pidb

#endif //PIDB_ROUTE_TABLE_H
