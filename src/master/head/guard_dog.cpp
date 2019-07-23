#include <unistd.h>
#include "guard_dog.h"

namespace pidb{
    GuardDog::GuardDog(int cycle, int interval){
        this->cycle_ = cycle;
        this->interval_ = interval;
        this->current_time_ = 0;
        this->things_.resize(cycle * 1000 / interval);
        bthread_mutex_init(&this->mutex_, NULL);
    }

    GuardDog::~GuardDog(){
        this->things_.clear();
    }

    void GuardDog::AddThing(bool(*func)(void*), void* arg){
        bthread_mutex_lock(&this->mutex_);
        // 一个简单的插入vector操作
        // 这里-1的原因是留给下一周期检查，新来的不用检查
        int current_time = (current_time_ - 1 + things_.size()) 
            %things_.size();
        things_[current_time].push_back({func, arg});
        bthread_mutex_unlock(&this->mutex_);
    }

    void GuardDog::HandleThings(){
        bthread_t t;
        if (bthread_start_background(&t, NULL, 
            GuardDog::DoHandleThings, this) != 0) {
            LOG(ERROR) << "create bthread guard_dog fail";
        }
        else LOG(INFO) << "create bthread guard_dog success";
    }

    void* GuardDog::DoHandleThings(void* guard_dog){
        GuardDog* dog = (GuardDog*)guard_dog;
        int time = dog->cycle_ * 1000 / dog->interval_;
        // 一直执行，一个周期接一个周期
        while(true){
            // 时间计数器归0（周期初始）
            bthread_mutex_lock(&dog->mutex_);
            dog->current_time_ = 0;
            bthread_mutex_unlock(&dog->mutex_);
            
            // 执行对应节点的事情，若执行“不成功”就删除该事情
            for(int i = 0; i < time; ++i){
                usleep(dog->interval_ * 1000);
                bthread_mutex_lock(&dog->mutex_);
                for(auto iter = dog->things_[i].begin();
                        iter != dog->things_[i].end();){
                    if(!iter->first(iter->second))
                        iter = dog->things_[i].erase(iter);
                    else ++iter;
                }
                
                // 时间计数器加1
                dog->current_time_ += 1;
                bthread_mutex_unlock(&dog->mutex_);
            }
        }
    }
}