#ifndef __EXECUTOR_SERVICE_IO_H_INCLUDED__
#define __EXECUTOR_SERVICE_IO_H_INCLUDED__

#include <boost/asio/io_service.hpp>
#include <thread>
#include <algorithm>
#include <vector>
#include <glog/logging.h>

#include <status.h>

namespace utils {

/*
 * This service spawns threads equivalent to number of processors
 * available in the machine and stick each thread to a core.
 * Single io service is run across all threads 
 */

class ExecutorServiceIO {

public:

    explicit ExecutorServiceIO(): work(io_service) {
        num_threads = std::max(static_cast<unsigned int>(1),
                               std::thread::hardware_concurrency());
    }

    /* deleting the copy and assignment constructors */
    ExecutorServiceIO(const ExecutorServiceIO &) = delete;
    const ExecutorServiceIO operator=(const ExecutorServiceIO &) = delete;

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
                io_service.run(); 
            });
            if (err == 0) {
                threads.push_back(std::move(core_thread));
            }
        }
        if (threads.size() == 0) {
            LOG(ERROR) << "op=ExecutorServiceIO, status=failed_to_start_thread"
                       << std::endl;
            return Status::THREADS_CREATION_FAILED;
        }
        LOG(INFO) << "op=ExecutorServiceIO, status=started, num_threads=" << threads.size() << std::endl;
        return Status::SUCCESS;
    }

    template<typename Fn, typename ... Params>
    void submit(Fn && fn, Params&& ... args) {
        io_service.post(std::bind(std::forward<Fn>(fn), std::forward<Params>(args)...));
    }

    Status stop() {
        if (!io_service.stopped()) {
            io_service.stop();
        }
        for (auto & e: threads) {
            e.join();
        }
        return Status::SUCCESS;
    }

private:

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

    boost::asio::io_service io_service;
    boost::asio::io_service::work work;
    size_t num_threads {1};
    std::vector<std::thread> threads;
};

}

#endif  /*__EXECUTOR_SERVICE_IO_H_INCLUDED__*/
