#ifndef __EXECUTOR_SERVICE_FOLLY_H_INCLUDED__
#define __EXECUTOR_SERVICE_FOLLY_H_INCLUDED__

#include <thread>
#include <algorithm>
#include <vector>
#include <glog/logging.h>
#include <folly/MPMCQueue.h>

#include "status.h"
#include "request.h"

namespace utils {

/*
 * This service spawns threads equivalent to number of processors
 * available in the machine and stick each thread to a core.
 * Single io service is run across all threads 
 */

class ExecutorServiceFolly {

const size_t MAX_QUEUE_SIZE = 100 * 1024;

public:

    explicit ExecutorServiceFolly(): req_queue(MAX_QUEUE_SIZE) {
        num_threads = std::max(static_cast<unsigned int>(1),
                               std::thread::hardware_concurrency());
    }

    /* deleting the copy and assignment constructors */
    ExecutorServiceFolly(const ExecutorServiceFolly &) = delete;
    const ExecutorServiceFolly operator=(const ExecutorServiceFolly &) = delete;

    Status start() {
        for (size_t idx = 0; idx < num_threads; ++idx) {
            int err = 0;
            std::thread core_thread([this, idx, &err]() {
                err = stickThreadToCore(static_cast<int>(idx));
                if (err < 0) {
                    LOG(ERROR) << "op=thread, status=failed_to_stick_to_core"
                               << std::endl;
                    return;
                }
                thread_func();
            });
            if (err == 0) {
                threads.push_back(std::move(core_thread));
            }
        }
        if (threads.size() == 0) {
            LOG(ERROR) << "op=ExecutorServiceFolly, status=failed_to_start_thread"
                       << std::endl;
            return Status::THREADS_CREATION_FAILED;
        }
        LOG(INFO) << "op=ExecutorServiceFolly, status=started, num_threads=" << threads.size() << std::endl;
        return Status::SUCCESS;
    }

    void submit(const std::shared_ptr<Request> & req) {
        req_queue.write(req);
    }

    Status stop() {
        for (auto & e: threads) {
            e.join();
        }
        return Status::SUCCESS;
    }

private:

    void thread_func() {
        LOG(INFO) << "op=thread_func, started" << std::endl;
        while (true) {
            std::shared_ptr<Request> req;
            req_queue.blockingRead(req);
            req->do_work();
        }
        LOG(INFO) << "op=thread_func, exit" << std::endl;
    }

    int stickThreadToCore(int core_id) {
#ifdef __linux
        int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
        if (core_id < 0 || core_id >= num_cores) {
            return EINVAL;
        }
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(core_id, &cpuset);
        pthread_t current_thread = pthread_self();
        return pthread_setaffinity_np(current_thread, sizeof(cpu_set_t),
                                      &cpuset);
#else
        LOG(ERROR) << "op=stickThreadtoCore, status=cpu_affinity_disabled" << std::endl;
#endif
    }
    size_t num_threads {1};
    std::vector<std::thread> threads;
    folly::MPMCQueue<std::shared_ptr<Request>> req_queue;
};

}

#endif  /*__EXECUTOR_SERVICE_FOLLY_H_INCLUDED__*/
