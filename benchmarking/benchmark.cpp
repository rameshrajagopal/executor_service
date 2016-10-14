#include "executor_service_folly.h"
#include "executor_service_io.h"
#include "request.h"

#include <memory>
#include <chrono>
#include <iostream>

static const std::string FOLLY_SERVICE = "folly";
static const std::string IO_SERVICE = "io";
using namespace utils;

int main(int argc, char * argv[]) {
    if (argc != 5) {
        std::cout << argv[0] << " num_times num_entries folly/io" << std::endl;
        return 0;
    }
    int num_times = atoi(argv[1]);
    int num_entries = atoi(argv[2]);
    std::string service_name = argv[3];
    std::string db_path = argv[4];

    int64_t duration = 0;

    Counter counter(num_times, db_path);

    if (service_name == FOLLY_SERVICE) {
        ExecutorServiceFolly service; 
        service.start();
        std::this_thread::sleep_for(std::chrono::seconds(1));

        auto begin = std::chrono::high_resolution_clock::now();
        for (size_t idx = 0; idx < num_times; ++idx) {
            std::shared_ptr<Request> req = std::make_shared<Request>(static_cast<int64_t>(idx),
                                                                     static_cast<int64_t>(num_entries),
                                                                     counter);
            service.submit(req);
        }
        counter.wait();
        auto end = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count();
        LOG(INFO) << "op=benchmarking, service=" << service_name << ", num_times="
              << num_times << ", num_entries=" << num_entries << ", duration=" << duration << std::endl;

        service.stop();
    } else {
        ExecutorServiceIO service;
        service.start();
        std::this_thread::sleep_for(std::chrono::seconds(1));

        auto begin = std::chrono::high_resolution_clock::now();
        for (size_t idx = 0; idx < num_times; ++idx) {
            std::shared_ptr<Request> req = std::make_shared<Request>(static_cast<int64_t>(idx),
                                                                     static_cast<int64_t>(num_entries),
                                                                     counter);
            service.submit(&Request::do_work, req);
        }
        counter.wait();
        auto end = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count();
        LOG(INFO) << "op=benchmarking, service=" << service_name << ", num_times="
              << num_times << ", num_entries=" << num_entries << ", duration=" << duration << std::endl;

        service.stop();
    }

    return 0;
}
