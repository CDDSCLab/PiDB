#include "gtest/gtest.h"
#include "route_table.h"

TEST(FooTest, Test1){
    EXPECT_EQ(1,1);
    EXPECT_TRUE(3>0);
    EXPECT_TRUE(3<0);
}

Test(RouteTest AddTest){
    pidb::RouteTable r;
}
int main(int argc,char *argv[]){
    ::testing::InitGoogleTest(&argc,argv);
    return  RUN_ALL_TESTS();
}