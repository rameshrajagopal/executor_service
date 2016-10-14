#include "rocksdb_wrapper.h"

#include <iostream>

int main(int argc, char * argv[]) {
    
    if (argc != 3) {
        std::cout << argv[0] << " db_path num_entries" << std::endl;
        return 0;
    }
    std::string path = argv[1];
    size_t num_entries = atoi(argv[2]);
    
    RocksDBWrapper rocksdb;

    Status status = rocksdb.init(path, OpenMode::READ_WRITE);
    if (status != Status::SUCCESS) {
        LOG(INFO) << "failed to open db" << std::endl;
        return -1;
    }
    for (size_t idx = 0; idx < num_entries; ++idx) {
        status = rocksdb.put(std::string(reinterpret_cast<const char *>(&idx), sizeof(idx)), 
                    std::string(reinterpret_cast<const char *>(&idx), sizeof(idx)));
        if (status != Status::SUCCESS) {
            LOG(INFO) << "failed to write" << ", idx=" << idx << std::endl;
            return -1;
        }
    }
    rocksdb.de_init();
    return 0;
}
