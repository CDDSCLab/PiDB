#include "gtest/gtest.h"
#include "route_table.h"

TEST(FooTest, Test1){
    EXPECT_EQ(1,1);
    EXPECT_TRUE(3>0);
    EXPECT_TRUE(3<0);
}

TEST(RouteTest, AddTest){
    pidb::RouteTable r;
    r.AddRecord("","a","group1");
    r.AddRecord("b","d","group2");
    EXPECT_TRUE(r[""]=="group1");
    EXPECT_TRUE(r["b"]=="group2");

    EXPECT_TRUE(r.FindRegion("1") == "group1");
    EXPECT_TRUE(r.FindRegion("c")=="group2");
    EXPECT_TRUE(r.FindRegion("e")=="");
}

int main(int argc,char *argv[]){
    ::testing::InitGoogleTest(&argc,argv);
    return  RUN_ALL_TESTS();
}