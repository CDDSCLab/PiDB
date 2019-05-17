//
// Created by ehds on 19-5-17.
//
#pragma once

#ifndef PIDB_CONTEXT_H
#define PIDB_CONTEXT_H

#include "leveldb/db.h"
#include <map>
#include <memory>
#include <atomic>
#include <unordered_map>
namespace pidb {
    class Context {
    public:
        Context() = default;

        Context(const Context &) = delete;

        Context &operator=(const Context &) = delete;

        virtual ~Context() = default;
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
        }

    private:
        const leveldb::Snapshot* snapshot_;
    };


    //用于缓存类型为T的指针，以ID为name,用完需要调用Erase释放资源
    template<typename T>
    class ContextCache {
    public:
        ContextCache() = default;

        ContextCache(const ContextCache &) = delete;

        ContextCache &operator=(const ContextCache &) = delete;


        virtual ~ContextCache() { map_.clear(); }

        virtual uint64_t Put(std::unique_ptr<T> &&context);
        virtual std::shared_ptr<T> Get(int64_t context_id);
        virtual int Erase(int64_t context_id);
        virtual void Clear(){map_.clear();}
        virtual int Size(){ return map_.size();}

    private:
        std::unordered_map<uint64_t, std::shared_ptr<T>> map_;
        std::atomic_uint64_t context_id_;
    };
}//namespace pidb
#endif //PIDB_CONTEXT_H
