
// 此部分目的是维护一个定时器，到点了执行某件事
// 具体实现是维持一个周期，在此周期范围内每隔一定的时间间隔
// 执行某件事情（若有事）.例如周期3s,时间间隔10ms,那么每
// 个周期就有300次去处理对应时刻可能存在的事情

#ifndef PIDB_GUARD_DOG_H
#define PIDB_GUARD_DOG_H

#include <bthread/bthread.h>
#include <vector>
#include <string>

using std::vector;
using std::pair;
using std::string;

namespace  pidb{
class GuardDog{
private:
    int current_time_;  // 当前时间，用计数器表示
    int cycle_;         // 周期(s)
    int interval_;      // 时间间隔(ms)
    bthread_mutex_t mutex_; // 一把互斥锁

    // 要定期处理的事情以及参数表
    // 函数固定类型bool(*)(void*)
    // void×是函数参数，传指针(若有多个，放在一个结构体里)
    vector<vector<pair<bool(*)(void*), void*>>> things_;

public:
    GuardDog(int cycle, int interval);
    ~GuardDog();

    // 新增一个事情及参数(根据当前时间放入对应的时刻)
    void AddThing(bool(*func)(void*), void* arg);

    // 具体定期处理事情（后台线程，循环处理）
    void HandleThings();
    static void* DoHandleThings(void* guard_dog);
};
}// namespace pidb

#endif //PIDB_GUARD_DOG_H
