#include "gtest/gtest.h"
#include "route_table.h"

//简单测试
TEST(FooTest, Test1){
    pidb::RouteTable r;
    r.AddRecord("","a","group1");
    r.AddRecord("c","d","group2");
    r.AddRecord("d","f","group3");
    std::string a = r.FindRegion("abc");
    EXPECT_TRUE(a=="group2");
    auto b = r.FindRegion("1");
    EXPECT_TRUE(b=="group1");
}
