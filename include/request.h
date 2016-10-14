#ifndef __REQUEST_H_INCLUDED__
#define __REQUEST_H_INCLUDED__

#include <mutex>
#include <condition_variable>
#include <algorithm>

#include "status.h"
#include "rocksdb_wrapper.h"

class Counter {
public:
    Counter(size_t max_counter, const std::string & db_path): max_counter(max_counter) {
        Status status = rocksdb.init(db_path, OpenMode::READ_ONLY);
        if (status != Status::SUCCESS) {
            LOG(ERROR) << "failed to open the db" << std::endl;
        }
    }

    ~Counter() {
        rocksdb.de_init();
    }

    void increment() {
        std::unique_lock<std::mutex> mlock(_mutex);
        std::string value;
        Status status = rocksdb.get(std::string(reinterpret_cast<const char *>(&counter), sizeof(counter)),
                                   value);
        if (status != Status::SUCCESS) {
            LOG(ERROR) << "failed to read the key value" << std::endl;
        }
        ++counter;
        if (counter == max_counter) {
            LOG(INFO) << "notify the waiter that it is done" << std::endl;
            _cond.notify_one();
        }
    }

    uint32_t getCount() const {
        return counter;
    }

    void wait() {
        std::unique_lock<std::mutex> mlock(_mutex);
        while (counter < max_counter) {
            _cond.wait(mlock);
        }
        LOG(INFO) << "exiting from loop" << std::endl;
    }

private:
    uint32_t counter {0};
    uint32_t max_counter;
    std::mutex _mutex;
    std::condition_variable _cond;
    RocksDBWrapper rocksdb;
};

struct Request {
    int64_t req_id;
    Counter & counter;
    std::vector<int64_t> prices;

    Request(int64_t req_id, const size_t num_entries, Counter & counter):
            req_id(req_id), counter(counter) {
        for (size_t idx = 0; idx < num_entries; ++idx) {
            prices.push_back(num_entries-idx);
        }
    }

    void do_work() {
        LOG(INFO) << "op=work, status=start, req_id=" << req_id << std::endl;
        std::sort(prices.begin(), prices.end());
        counter.increment();
        LOG(INFO) << "op=work, status=end, req_id=" << req_id << std::endl;
    }
};

#endif /* __REQUEST_H_INCLUDED__ */
