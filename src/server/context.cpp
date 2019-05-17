//
// Created by ehds on 19-5-17.
//

#include "context.h"
namespace pidb{

    template <typename T>
    uint64_t ContextCache<T>::Put(std::unique_ptr<T> &&context) {
        auto s = std::shared_ptr<T>(reinterpret_cast<T*>(context.release()));
        auto res = context_id_.fetch_add(1,std::memory_order_relaxed);
        map_[res]=s;
        return res;
    }

    template <typename T>
    std::shared_ptr<T> ContextCache<T>::Get(int64_t context_id) {
        if(map_.find(context_id) == map_.end()){
            return nullptr;
        }
        return map_[context_id];
    }

    template <typename T>
    int ContextCache<T>::Erase(int64_t context_id) {
        if(map_.find(context_id) == map_.end()){
            return 0;
        }
        map_.erase(context_id);
        return 0;
    }


}// namespace pidb的次