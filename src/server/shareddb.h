#ifndef PIDB_SHARED_DB_H_
#define PIDB_SHARED_DB_H_
#include <braft/storage.h>              // braft::SnapshotWriter
#include <leveldb/db.h>
//使用自带的引用计数，将其独立成文件，因为其他地方也可能用到
//考虑后面能否直接用shared_ptr代替
namespace  pidb
{
class SharedDB : public butil::RefCountedThreadSafe<SharedDB> {
public:
    explicit SharedDB(leveldb::DB* db) : _db(db) {}
    leveldb::DB* db() const { return _db; }
private:
friend class butil::RefCountedThreadSafe<SharedDB>;
    ~SharedDB() {
        if (_db!=nullptr) {
            delete _db;
        }
    }
    leveldb::DB* _db;
};
typedef scoped_refptr<SharedDB> scoped_db;
    
} //  pidb

#endif