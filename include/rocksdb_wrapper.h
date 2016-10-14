#ifndef __ROCKSDB_WRAPPER_H_INCLUDED__
#define __ROCKSDB_WRAPPER_H_INCLUDED__

#include <string>
#include <glog/logging.h>

#include "rocksdb/db.h"
#include "rocksdb/table.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/utilities/backupable_db.h"

#include "status.h"

enum class OpenMode {
    READ_ONLY = 1,
    READ_WRITE = 2,
};

class RocksDBWrapper {

public:

    RocksDBWrapper() {}

    Status init(const std::string & path, const OpenMode & open_mode) {
        db_path.assign(path);
        if (open_mode == OpenMode::READ_ONLY) {
            setReadOnlyOptions();
            return openReadOnly();
        } 
        setWriteOptions();
        return openReadWriteMode();
    }

    Status put(const std::string & key, const std::string & value) {
        if (rocks_db_handle == nullptr) return Status::DB_NOT_INITIALIZED;

        rocksdb::Status status = rocks_db_handle->Put(write_options, key, value);
        if (!status.ok()) {
            LOG(ERROR) << "op=put, code=" << status.code() << ", error=" << status.ToString() << std::endl;
            return Status::DB_PUT_FAILED;
        }
        return Status::SUCCESS;
    }

    Status get(const std::string & key, std::string & value) const {
        if (rocks_db_handle == nullptr) return Status::DB_NOT_INITIALIZED;

        rocksdb::Status status = rocks_db_handle->Get(read_options, key, &value);
        if (!status.ok()) {
            LOG(ERROR) << "op=get, code=" << status.code() << ", error=" << status.ToString() << std::endl;
            return Status::DB_GET_FAILED;
        }
        return Status::SUCCESS;
    }

    Status de_init() {
        if (nullptr != rocks_db_handle) {
            delete rocks_db_handle;
            rocks_db_handle = nullptr;
        }
    }

private:

    Status openReadOnly() {
        rocksdb::Status status = rocksdb::DB::OpenForReadOnly(options, db_path, &rocks_db_handle);
        if (!status.ok()) {
            LOG(ERROR) << "op=openReadOnly, status=failed" << ", error=" << status.ToString() << std::endl;
            return Status::DB_OPEN_FAILED;
        }
        return Status::SUCCESS;
    }

    Status openReadWriteMode() {
        rocksdb::Status status = rocksdb::DB::Open(options, db_path, &rocks_db_handle);
        if (!status.ok()) {
            LOG(ERROR) << "op=openReadWriteMode, status=failed" << ", error=" << status.ToString() << ", dp_path="
                       << db_path << std::endl;
            return Status::DB_OPEN_FAILED;
        }
    }

    void setReadOnlyOptions() {
        options.create_if_missing = false;
        options.info_log_level = rocksdb::InfoLogLevel::ERROR_LEVEL;
    }

    void setWriteOptions() {
        options.create_if_missing = true;

        // Enable background sync
        options.disableDataSync = false;
        options.use_fsync = true; // TODO: do we really need fsync during batch indexing???

        // Table options
        rocksdb::BlockBasedTableOptions block_based_options;
        block_based_options.block_size = 4 * 1024; // TODO: check with multiple options for below configuration
        options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(block_based_options));

        // WAL options - don't need WAL
        options.WAL_ttl_seconds = 0;
        options.WAL_size_limit_MB = 0;

        // Logging options
        options.info_log_level = rocksdb::InfoLogLevel::ERROR_LEVEL;

        // Level0 options
        options.level0_slowdown_writes_trigger = -1; // no slowdown at-all
        options.level0_stop_writes_trigger = 1; // need one file

        options.max_background_compactions = 2; // do parallel compaction
        options.env->SetBackgroundThreads(2, rocksdb::Env::Priority::LOW);

        // Write buffer size options
        options.write_buffer_size = 256 * 1024 * 1024; // TODO: check with multiple options for below configuration
        options.max_write_buffer_number = 2; // no. of memtables
        options.target_file_size_base = std::numeric_limits<uint64_t>::max(); //1073741824 / 10;
        options.max_bytes_for_level_base = std::numeric_limits<uint64_t>::max(); //1048576;
        options.source_compaction_factor = std::numeric_limits<int>::max(); //10000000;

        // Memtable options
        options.memtable_factory.reset(new rocksdb::VectorRepFactory);

        // Compression options
        options.compression = rocksdb::kLZ4Compression; // TODO: check with multiple options for below configuration

        // set write options
        write_options.disableWAL = true;
        write_options.sync = true;

        options.max_manifest_file_size = 1024;
    }

    void setReadWriteOptions() {
        options.create_if_missing = true;
        options.IncreaseParallelism();
        options.OptimizeLevelStyleCompaction();

        //options.statistics = rocksdb::CreateDBStatistics();
        options.stats_dump_period_sec = 15 * 60;
        options.max_background_compactions = 2;
        options.env->SetBackgroundThreads(2, rocksdb::Env::Priority::LOW);
        options.max_background_flushes = 1;
        options.env->SetBackgroundThreads(1, rocksdb::Env::Priority::HIGH);

        rocksdb::BlockBasedTableOptions block_based_options;
        block_based_options.block_size = 32 * 1024;
        options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(block_based_options));

        // Logging options
        options.info_log_level = rocksdb::InfoLogLevel::ERROR_LEVEL;
        options.compression = rocksdb::kLZ4Compression;
        // set write options
        write_options.disableWAL = true;
        write_options.sync = true;

        options.write_buffer_size = 64 * 1024 * 1024;
        options.max_write_buffer_number = 3;
        options.min_write_buffer_number_to_merge = 1;

        // Data sync config
        //
        options.disableDataSync = false;
        options.use_fsync = false;

        // WAL config
        //
        options.max_total_wal_size = 0; // use rocksdb default value
        options.WAL_size_limit_MB = 0; // no limit check
        options.WAL_ttl_seconds = 8 * 60 * 60; // Keeping it for 8 hours
        options.wal_dir = db_path;
        options.wal_bytes_per_sync = 0; // use rocksdb default value

        /*
         * target_file_size_base and target_file_size_multiplier -- Files in level 1 will have target_file_size_basei       * bytes. Each next level's file size will be target_file_size_multiplier bigger than previous one.
         * However, by default target_file_size_multiplier is 1, so files in all L1..Lmax levels are equal.
         * Increasing target_file_size_base will reduce total number of database files, which is generally a
         * good thing. We recommend setting target_file_size_base to be max_bytes_for_level_base / 10,
         * so that there are 10 files in level 1.
         */
        options.max_bytes_for_level_base = 512 * 1024 * 1024;
        options.target_file_size_base = 64 * 1024 * 1024;
        options.target_file_size_multiplier = 1;
        options.source_compaction_factor = 1;

        // Level0 options
        options.level0_file_num_compaction_trigger = 3;  // Once first file is written, immediately trigger compaction
        options.level0_slowdown_writes_trigger = -1;     // no slowdown at-all
        options.level0_stop_writes_trigger = 40;          // need one file

        options.max_manifest_file_size = 128 * 1024;
    }
   std::string db_path;
   rocksdb::DB * rocks_db_handle {nullptr};
   rocksdb::Options options;
   rocksdb::ReadOptions read_options;
   rocksdb::WriteOptions write_options;
   rocksdb::FlushOptions flush_options;
};

#endif /* __ROCKSDB_WRITER_H_INCLUDED__ */
