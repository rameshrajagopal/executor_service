Simple benchmarking folly vs io_service tool

Create as many number of threads as number of cores available in the target
machine and stick each thread to each one of the core. With these threads should we use
folly for synchronizing across threads or io_service.

Prerequisite :
 - should have rocksdb 4.8 
 - should have folly
 - should boost latest

data/data_writer.cpp - this one generates rocksdb database for the benchmarking tool

include/executor_service_folly.h - executor_service + folly queue is used for distributing
the requests across threads.

include/executor_service_io.h - executor_service + io_service is used for distributing the
requests across threads.

benchmarking/benchmark.cpp - sets up the tool to calculate the timing across

include/request.h - this contains the request and work method

