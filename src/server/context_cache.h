//
// Created by ehds on 19-5-17.
//
#pragma once

#ifndef PIDB_CONTEXT_CACHE_H
#define PIDB_CONTEXT_CACHE_H

#include "leveldb/db.h"
#include <map>
#include <memory>
#include <atomic>
#include <unordered_map>
#include <iostream>

namespace pidb {
    class Context {
    public:
        Context() = default;

        Context(const Context &) = default;

        Context &operator=(const Context &) = default;

        virtual ~Context() = default;

        void SetID(int64_t id)noexcept {
            id_=id;
        }
        int64_t GetID() const{
            return id_;
        }
    private:
        int64_t id_;
    };


    class SnapshotContext : public Context {
    public:
        SnapshotContext(const leveldb::Snapshot *snapshot){
            snapshot_ = snapshot;
        };

        SnapshotContext(const SnapshotContext &) = delete;

        SnapshotContext &operator=(const SnapshotContext &) = delete;

        ~SnapshotContext() {
            //在这里我们需要释放snapshot,但是snapshot是存储在db里面的snapshotlist
            //需要调用db的release_snapshot.
            //测试
            std::cout<<"release snapshot"<<std::endl;

        }
        const leveldb::Snapshot * Get() const{ return snapshot_;}

    private:
        const leveldb::Snapshot* snapshot_;
    };


    class IteratorContext:public Context{
    public:
        IteratorContext(std::unique_ptr<leveldb::Iterator> iterator,const std::string &group)
        :iterator_(std::move(iterator)),group_(group){};

        IteratorContext(const IteratorContext &) = delete;

        IteratorContext &operator=(const IteratorContext &) = delete;
        ~IteratorContext(){
            //shoud we do somthing?
        }
    private:
        std::unique_ptr<leveldb::Iterator> iterator_;
//        std::string start_;     //当前iterator起始key
//        std::string end_;       //当前iterator的结束key
        const std::string group_; //用于标识当前iterator属于哪个group


    };

    //用于缓存类型为T的指针，以ID为name,用完需要调用Erase释放资源
    template <typename T>
    class ContextCache {
    public:
        ContextCache() noexcept{
            context_id_.store(0,std::memory_order_relaxed);
        };

        //no copy
        ContextCache(const ContextCache &) = delete;
        ContextCache &operator=(const ContextCache &) = delete;


        virtual ~ContextCache() { map_.clear(); }
        //放如一个对象，并且返回一个全局id
        uint64_t Put(std::unique_ptr<T> context);
        //通过全局ID获得对象
        std::shared_ptr<T> Get(int64_t context_id);
        //从缓存中删除这个对象
        virtual bool Erase(int64_t context_id);
        //清空缓存
        virtual void Clear(){map_.clear();}
        //获得cache的大小
        virtual int Size() const noexcept{ return map_.size();}
    private:
        std::unordered_map<uint64_t, std::shared_ptr<T>> map_;
        std::atomic_uint64_t context_id_;
    };


    template <typename T>
    uint64_t ContextCache<T>::Put(std::unique_ptr<T> context) {
        //TODO 测试多线程一致性, s是否是当前+1前的值
        //https://stackoverflow.com/questions/56185373/how-to-guarantee-data-dependence-with-atomic

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
    bool ContextCache<T>::Erase(int64_t context_id) {
        if(map_.find(context_id) == map_.end()){
            return 0;
        }

        map_.erase(context_id);
        return 0;
    }


}//namespace pidb
#endif //PIDB_CONTEXT_CACHE_H
