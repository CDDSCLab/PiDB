//
// Created by ehds on 19-5-17.
//
#include "gtest/gtest.h"
#include "leveldb/db.h"
#include "context_cache.h"

TEST(ContextTest,put_and_get_test){
    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);
    pidb::ContextCache<pidb::SnapshotContext> a;

    std::unique_ptr<pidb::SnapshotContext> s (new pidb::SnapshotContext (db->GetSnapshot()));
    auto id = a.Put(std::move(s));
    EXPECT_TRUE(id==0);
    auto snapshot = a.Get(id);
    EXPECT_TRUE(snapshot!= nullptr);
    db->ReleaseSnapshot(snapshot.get()->Get());
    a.Erase(id);
    EXPECT_TRUE(a.Get(id)== nullptr);
}
