// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// +build rocksdb

package rocksdb

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/YiqinXiong/gorocksdb"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

//  properties
const (
	rocksdbDir = "rocksdb.dir"
	// DBOptions
	rocksdbAllowConcurrentMemtableWrites   = "rocksdb.allow_concurrent_memtable_writes"
	rocsdbAllowMmapReads                   = "rocksdb.allow_mmap_reads"
	rocksdbAllowMmapWrites                 = "rocksdb.allow_mmap_writes"
	rocksdbArenaBlockSize                  = "rocksdb.arena_block_size"
	rocksdbDBWriteBufferSize               = "rocksdb.db_write_buffer_size"
	rocksdbHardPendingCompactionBytesLimit = "rocksdb.hard_pending_compaction_bytes_limit"
	rocksdbLevel0FileNumCompactionTrigger  = "rocksdb.level0_file_num_compaction_trigger"
	rocksdbLevel0SlowdownWritesTrigger     = "rocksdb.level0_slowdown_writes_trigger"
	rocksdbLevel0StopWritesTrigger         = "rocksdb.level0_stop_writes_trigger"
	rocksdbMaxBytesForLevelBase            = "rocksdb.max_bytes_for_level_base"
	rocksdbMaxBytesForLevelMultiplier      = "rocksdb.max_bytes_for_level_multiplier"
	rocksdbMaxTotalWalSize                 = "rocksdb.max_total_wal_size"
	rocksdbMemtableHugePageSize            = "rocksdb.memtable_huge_page_size"
	rocksdbNumLevels                       = "rocksdb.num_levels"
	rocksdbUseDirectReads                  = "rocksdb.use_direct_reads"
	rocksdbUseFsync                        = "rocksdb.use_fsync"
	rocksdbWriteBufferSize                 = "rocksdb.write_buffer_size"
	rocksdbMaxWriteBufferNumber            = "rocksdb.max_write_buffer_number"
	// TableOptions/BlockBasedTable
	rocksdbBlockSize                        = "rocksdb.block_size"
	rocksdbBlockSizeDeviation               = "rocksdb.block_size_deviation"
	rocksdbCacheIndexAndFilterBlocks        = "rocksdb.cache_index_and_filter_blocks"
	rocksdbNoBlockCache                     = "rocksdb.no_block_cache"
	rocksdbPinL0FilterAndIndexBlocksInCache = "rocksdb.pin_l0_filter_and_index_blocks_in_cache"
	rocksdbWholeKeyFiltering                = "rocksdb.whole_key_filtering"
	rocksdbBlockRestartInterval             = "rocksdb.block_restart_interval"
	rocksdbFilterPolicy                     = "rocksdb.filter_policy"
	rocksdbIndexType                        = "rocksdb.index_type"
	// TODO: add more configurations
	// xyq add
	rocksdbBitsPerKey                              = "rocksdb.bits_per_key"
	rocksdbMinWriteBufferNumberToMerge             = "rocksdb.min_write_buffer_number_to_merge"
	rocksdbDisableAutoCompactions                  = "rocksdb.disable_auto_compactions"
	rocksdbTargetFileSizeBase                      = "rocksdb.target_file_size_base"
	rocksdbOptimizeFiltersForHits                  = "rocksdb.optimize_filters_for_hits"
	rocksdbStatsDumpPeriodSec                      = "rocksdb.stats_dump_period_sec"
	rocksdbMaxBackgroundCompactions                = "rocksdb.max_background_compactions"
	rocksdbSoftPendingCompactionBytesLimit         = "rocksdb.soft_pending_compaction_bytes_limit"
	kGB                                    float64 = 1073741824.0
	kMB                                    float64 = 1048576.0
)

type rocksDBCreator struct {
}

type rocksDB struct {
	p *properties.Properties

	db *gorocksdb.DB

	r       *util.RowCodec
	bufPool *util.BufPool

	readOpts  *gorocksdb.ReadOptions
	writeOpts *gorocksdb.WriteOptions

	internalStatsSnapshot *internalStats
}

type contextKey string

type internalStats struct {
	// Cumulative
	start_time         time.Time
	num_keys_read      uint64
	user_bytes_read    uint64
	user_bytes_written uint64
	num_keys_written   uint64
	write_other        uint64
	write_self         uint64
	wal_bytes          uint64
	wal_synced         uint64
	write_with_wal     uint64
	write_stall_micros uint64
	// about scan
	num_seek        uint64
	num_next        uint64
	num_prev        uint64
	num_seek_found  uint64
	num_next_found  uint64
	num_prev_found  uint64
	iter_read_bytes uint64
	// Interval
	interval_start_time         time.Time
	interval_num_keys_read      uint64
	interval_user_bytes_read    uint64
	interval_user_bytes_written uint64
	interval_num_keys_written   uint64
	interval_write_other        uint64
	interval_write_self         uint64
	interval_wal_bytes          uint64
	interval_wal_synced         uint64
	interval_write_with_wal     uint64
	interval_write_stall_micros uint64
	// about scan
	interval_num_seek        uint64
	interval_num_next        uint64
	interval_num_prev        uint64
	interval_num_seek_found  uint64
	interval_num_next_found  uint64
	interval_num_prev_found  uint64
	interval_iter_read_bytes uint64
}

func (c rocksDBCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	dir := p.GetString(rocksdbDir, "/tmp/rocksdb")

	if p.GetBool(prop.DropData, prop.DropDataDefault) {
		os.RemoveAll(dir)
	}

	opts := getOptions(p)

	db, err := gorocksdb.OpenDb(opts, dir)
	if err != nil {
		return nil, err
	}

	stat := newInternalStats("")

	return &rocksDB{
		p:                     p,
		db:                    db,
		r:                     util.NewRowCodec(p),
		bufPool:               util.NewBufPool(),
		readOpts:              gorocksdb.NewDefaultReadOptions(),
		writeOpts:             gorocksdb.NewDefaultWriteOptions(),
		internalStatsSnapshot: stat,
	}, nil
}

func getTableOptions(p *properties.Properties) (*gorocksdb.BlockBasedTableOptions, bool) {
	tblOpts := gorocksdb.NewDefaultBlockBasedTableOptions()

	tblOpts.SetBlockSize(p.GetInt(rocksdbBlockSize, 4<<10))
	tblOpts.SetBlockSizeDeviation(p.GetInt(rocksdbBlockSizeDeviation, 10))
	tblOpts.SetCacheIndexAndFilterBlocks(p.GetBool(rocksdbCacheIndexAndFilterBlocks, false))
	tblOpts.SetNoBlockCache(p.GetBool(rocksdbNoBlockCache, false))
	tblOpts.SetPinL0FilterAndIndexBlocksInCache(p.GetBool(rocksdbPinL0FilterAndIndexBlocksInCache, false))
	tblOpts.SetWholeKeyFiltering(p.GetBool(rocksdbWholeKeyFiltering, true))
	tblOpts.SetBlockRestartInterval(p.GetInt(rocksdbBlockRestartInterval, 16))

	if b := p.GetString(rocksdbFilterPolicy, ""); len(b) > 0 {
		if b == "rocksdb.BuiltinBloomFilter" {
			// const defaultBitsPerKey = 10
			BitsPerKey := p.GetInt(rocksdbBitsPerKey, 10)
			tblOpts.SetFilterPolicy(gorocksdb.NewBloomFilter(BitsPerKey))
		}
	}

	isHashSearch := false
	indexType := p.GetString(rocksdbIndexType, "kBinarySearch")
	if indexType == "kBinarySearch" {
		tblOpts.SetIndexType(gorocksdb.KBinarySearchIndexType)
	} else if indexType == "kHashSearch" {
		isHashSearch = true
		tblOpts.SetIndexType(gorocksdb.KHashSearchIndexType)
	} else if indexType == "kTwoLevelIndexSearch" {
		tblOpts.SetIndexType(gorocksdb.KTwoLevelIndexSearchIndexType)
	}

	return tblOpts, isHashSearch
}

func getOptions(p *properties.Properties) *gorocksdb.Options {
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)

	opts.SetAllowConcurrentMemtableWrites(p.GetBool(rocksdbAllowConcurrentMemtableWrites, true))
	opts.SetAllowMmapReads(p.GetBool(rocsdbAllowMmapReads, false))
	opts.SetAllowMmapWrites(p.GetBool(rocksdbAllowMmapWrites, false))
	opts.SetArenaBlockSize(p.GetInt(rocksdbArenaBlockSize, 0))
	opts.SetDbWriteBufferSize(p.GetInt(rocksdbDBWriteBufferSize, 0))
	opts.SetHardPendingCompactionBytesLimit(p.GetUint64(rocksdbHardPendingCompactionBytesLimit, 256<<30))
	opts.SetLevel0FileNumCompactionTrigger(p.GetInt(rocksdbLevel0FileNumCompactionTrigger, 4))
	opts.SetLevel0SlowdownWritesTrigger(p.GetInt(rocksdbLevel0SlowdownWritesTrigger, 20))
	opts.SetLevel0StopWritesTrigger(p.GetInt(rocksdbLevel0StopWritesTrigger, 36))
	opts.SetMaxBytesForLevelBase(p.GetUint64(rocksdbMaxBytesForLevelBase, 256<<20))
	opts.SetMaxBytesForLevelMultiplier(p.GetFloat64(rocksdbMaxBytesForLevelMultiplier, 10))
	opts.SetMaxTotalWalSize(p.GetUint64(rocksdbMaxTotalWalSize, 0))
	opts.SetMemtableHugePageSize(p.GetInt(rocksdbMemtableHugePageSize, 0))
	opts.SetNumLevels(p.GetInt(rocksdbNumLevels, 7))
	opts.SetUseDirectReads(p.GetBool(rocksdbUseDirectReads, false))
	opts.SetUseFsync(p.GetBool(rocksdbUseFsync, false))
	opts.SetWriteBufferSize(p.GetInt(rocksdbWriteBufferSize, 64<<20))
	opts.SetMaxWriteBufferNumber(p.GetInt(rocksdbMaxWriteBufferNumber, 2))

	opts.SetMinWriteBufferNumberToMerge(p.GetInt(rocksdbMinWriteBufferNumberToMerge, 1))
	opts.SetDisableAutoCompactions(p.GetBool(rocksdbDisableAutoCompactions, false))
	opts.SetTargetFileSizeBase(p.GetUint64(rocksdbTargetFileSizeBase, 64<<20))
	opts.SetOptimizeFiltersForHits(p.GetBool(rocksdbOptimizeFiltersForHits, false))
	opts.SetStatsDumpPeriodSec(uint(p.GetUint64(rocksdbStatsDumpPeriodSec, 600)))
	opts.EnableStatistics() // makes statistics is not nil
	opts.SetMaxBackgroundCompactions(p.GetInt(rocksdbMaxBackgroundCompactions, 1))
	opts.SetSoftPendingCompactionBytesLimit(p.GetUint64(rocksdbSoftPendingCompactionBytesLimit, 64<<30))

	tblOpts, isHashSearch := getTableOptions(p)
	if isHashSearch == true {
		opts.SetPrefixExtractor(gorocksdb.NewFixedPrefixTransform(3))
	}
	opts.SetBlockBasedTableFactory(tblOpts)

	return opts
}

func (db *rocksDB) Close() error {
	db.db.Close()
	return nil
}

func (db *rocksDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *rocksDB) CleanupThread(_ context.Context) {
}

func (db *rocksDB) getRowKey(table string, key string) []byte {
	return util.Slice(fmt.Sprintf("%s:%s", table, key))
}

func cloneValue(v *gorocksdb.Slice) []byte {
	return append([]byte(nil), v.Data()...)
}

func (db *rocksDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	value, err := db.db.Get(db.readOpts, db.getRowKey(table, key))
	if err != nil {
		return nil, err
	}
	defer value.Free()

	return db.r.Decode(cloneValue(value), fields)
}

func (db *rocksDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	res := make([]map[string][]byte, count)
	it := db.db.NewIterator(db.readOpts)
	defer it.Close()

	rowStartKey := db.getRowKey(table, startKey)

	it.Seek(rowStartKey)
	i := 0
	for it = it; it.Valid() && i < count; it.Next() {
		value := it.Value()
		m, err := db.r.Decode(cloneValue(value), fields)
		if err != nil {
			return nil, err
		}
		res[i] = m
		i++
	}

	if err := it.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

func (db *rocksDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	m, err := db.Read(ctx, table, key, nil)
	if err != nil {
		return err
	}

	for field, value := range values {
		m[field] = value
	}

	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)

	rowData, err := db.r.Encode(buf.Bytes(), m)
	if err != nil {
		return err
	}

	rowKey := db.getRowKey(table, key)

	return db.db.Put(db.writeOpts, rowKey, rowData)
}

func (db *rocksDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	rowKey := db.getRowKey(table, key)

	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)

	rowData, err := db.r.Encode(buf.Bytes(), values)
	if err != nil {
		return err
	}
	return db.db.Put(db.writeOpts, rowKey, rowData)
}

func (db *rocksDB) Delete(ctx context.Context, table string, key string) error {
	rowKey := db.getRowKey(table, key)

	return db.db.Delete(db.writeOpts, rowKey)
}

func (db *rocksDB) GetStatisticsString() string {
	opts := db.db.Opts()
	ss := opts.GetStatisticsString()
	st := db.internalStatsSnapshot
	st.intervalCount(ss)
	elapsed := time.Since(st.start_time).Seconds()
	intervalElapsed := time.Since(st.interval_start_time).Seconds()
	st.resetIntervalStartTime()

	timeStr := fmt.Sprintf("Cumulative Takes: %.1f sec, Interval Takes: %.1f sec\n", elapsed, intervalElapsed)
	cumulativeWriteStr := fmt.Sprintf("Cumulative writes: %v writes, %v keys, %v commit groups, %.1f writes per commit group, ingest: %.2f GB, %.2f MB/s\n",
		(st.write_other + st.write_self),
		st.num_keys_written,
		st.write_self,
		float64(st.write_other+st.write_self)/float64(st.write_self+1),
		float64(st.user_bytes_written)/kGB,
		float64(st.user_bytes_written)/kMB/elapsed)
	cumulativeReadStr := fmt.Sprintf("Cumulative reads: %v keys, read_bytes: %.2f GB, %.2f MB/s\n",
		st.num_keys_read,
		float64(st.user_bytes_read)/kGB,
		float64(st.user_bytes_read)/kMB/elapsed)
	cumulativeScanStr := fmt.Sprintf("Cumulative scans: %v seeks, %v seekfounds, %v nexts, %v nextfounds, %v prevs, %v prevfounds, iter_read_bytes: %.2f GB, %.2f MB/s\n",
		st.num_seek, st.num_seek_found,
		st.num_next, st.num_next_found,
		st.num_prev, st.num_prev_found,
		float64(st.iter_read_bytes)/kGB,
		float64(st.iter_read_bytes)/kMB/elapsed)
	cumulativeWALStr := fmt.Sprintf("Cumulative WAL: %v writes, %v syncs, %.2f writes per sync, written: %.2f GB, %.2f MB/s\n",
		st.write_with_wal, st.wal_synced,
		float64(st.write_with_wal)/float64(st.wal_synced+1),
		float64(st.wal_bytes)/kGB,
		float64(st.wal_bytes)/kMB/elapsed)
	cumulativeStallStr := fmt.Sprintf("Cumulative stall: %v us, %.1f percent\n",
		st.write_stall_micros,
		float64(st.write_stall_micros)/10000.0/elapsed)
	cumulativeStr := cumulativeWriteStr + cumulativeReadStr + cumulativeScanStr + cumulativeWALStr + cumulativeStallStr

	intervalWriteStr := fmt.Sprintf("Interval writes: %v writes, %v keys, %v commit groups, %.1f writes per commit group, ingest: %.2f MB, %.2f MB/s\n",
		(st.interval_write_other + st.interval_write_self),
		st.interval_num_keys_written,
		st.interval_write_self,
		float64(st.interval_write_other+st.interval_write_self)/float64(st.interval_write_self+1),
		float64(st.interval_user_bytes_written)/kMB,
		float64(st.interval_user_bytes_written)/kMB/intervalElapsed)
	intervalReadStr := fmt.Sprintf("Interval reads: %v keys, read_bytes: %.2f MB, %.2f MB/s\n",
		st.interval_num_keys_read,
		float64(st.interval_user_bytes_read)/kMB,
		float64(st.interval_user_bytes_read)/kMB/intervalElapsed)
	intervalScanStr := fmt.Sprintf("Interval scans: %v seeks, %v seekfounds, %v nexts, %v nextfounds, %v prevs, %v prevfounds, iter_read_bytes: %.2f MB, %.2f MB/s\n",
		st.interval_num_seek, st.interval_num_seek_found,
		st.interval_num_next, st.interval_num_next_found,
		st.interval_num_prev, st.interval_num_prev_found,
		float64(st.interval_iter_read_bytes)/kMB,
		float64(st.interval_iter_read_bytes)/kMB/intervalElapsed)
	intervalWALStr := fmt.Sprintf("Interval WAL: %v writes, %v syncs, %.2f writes per sync, written: %.2f MB, %.2f MB/s\n",
		st.interval_write_with_wal, st.interval_wal_synced,
		float64(st.interval_write_with_wal)/float64(st.interval_wal_synced+1),
		float64(st.interval_wal_bytes)/kMB,
		float64(st.interval_wal_bytes)/kMB/intervalElapsed)
	intervalStallStr := fmt.Sprintf("Interval stall: %v us, %.1f percent\n",
		st.interval_write_stall_micros,
		float64(st.interval_write_stall_micros)/10000.0/intervalElapsed)
	intervalStr := intervalWriteStr + intervalReadStr + intervalScanStr + intervalWALStr + intervalStallStr

	statisticsStr := timeStr + cumulativeStr + intervalStr
	return statisticsStr
}

func newInternalStats(ss string) *internalStats {
	stat := new(internalStats)
	stat.start_time = time.Now()
	stat.interval_start_time = time.Now()

	stat.num_keys_read = 0
	stat.user_bytes_read = 0
	stat.user_bytes_written = 0
	stat.num_keys_written = 0
	stat.write_other = 0
	stat.write_self = 0
	stat.wal_bytes = 0
	stat.wal_synced = 0
	stat.write_with_wal = 0
	stat.write_stall_micros = 0
	stat.num_seek = 0
	stat.num_next = 0
	stat.num_prev = 0
	stat.num_seek_found = 0
	stat.num_next_found = 0
	stat.num_prev_found = 0
	stat.iter_read_bytes = 0

	stat.interval_num_keys_read = 0
	stat.interval_user_bytes_read = 0
	stat.interval_user_bytes_written = 0
	stat.interval_num_keys_written = 0
	stat.interval_write_other = 0
	stat.interval_write_self = 0
	stat.interval_wal_bytes = 0
	stat.interval_wal_synced = 0
	stat.interval_write_with_wal = 0
	stat.interval_write_stall_micros = 0
	stat.interval_num_seek = 0
	stat.interval_num_next = 0
	stat.interval_num_prev = 0
	stat.interval_num_seek_found = 0
	stat.interval_num_next_found = 0
	stat.interval_num_prev_found = 0
	stat.interval_iter_read_bytes = 0

	return stat
}

func (stat *internalStats) intervalCount(ss string) {
	lines := strings.Split(ss, "\n")
	statMap := make(map[string]string)
	for _, s := range lines {
		line := strings.SplitN(s, " ", 2)
		if len(line) < 2 {
			continue
		}
		statMap[line[0]] = line[1]
	}

	num_keys_read, _ := strconv.ParseUint(strings.SplitN(statMap["rocksdb.number.keys.read"], " : ", 2)[1], 10, 64)
	stat.interval_num_keys_read = num_keys_read - stat.num_keys_read
	stat.num_keys_read = num_keys_read

	user_bytes_read, _ := strconv.ParseUint(strings.SplitN(statMap["rocksdb.bytes.read"], " : ", 2)[1], 10, 64)
	stat.interval_user_bytes_read = user_bytes_read - stat.user_bytes_read
	stat.user_bytes_read = user_bytes_read

	user_bytes_written, _ := strconv.ParseUint(strings.SplitN(statMap["rocksdb.bytes.written"], " : ", 2)[1], 10, 64)
	stat.interval_user_bytes_written = user_bytes_written - stat.user_bytes_written
	stat.user_bytes_written = user_bytes_written

	num_keys_written, _ := strconv.ParseUint(strings.SplitN(statMap["rocksdb.number.keys.written"], " : ", 2)[1], 10, 64)
	stat.interval_num_keys_written = num_keys_written - stat.num_keys_written
	stat.num_keys_written = num_keys_written

	write_other, _ := strconv.ParseUint(strings.SplitN(statMap["rocksdb.write.other"], " : ", 2)[1], 10, 64)
	stat.interval_write_other = write_other - stat.write_other
	stat.write_other = write_other

	write_self, _ := strconv.ParseUint(strings.SplitN(statMap["rocksdb.write.self"], " : ", 2)[1], 10, 64)
	stat.interval_write_self = write_self - stat.write_self
	stat.write_self = write_self

	wal_bytes, _ := strconv.ParseUint(strings.SplitN(statMap["rocksdb.wal.bytes"], " : ", 2)[1], 10, 64)
	stat.interval_wal_bytes = wal_bytes - stat.wal_bytes
	stat.wal_bytes = wal_bytes

	wal_synced, _ := strconv.ParseUint(strings.SplitN(statMap["rocksdb.wal.synced"], " : ", 2)[1], 10, 64)
	stat.interval_wal_synced = wal_synced - stat.wal_synced
	stat.wal_synced = wal_synced

	write_with_wal, _ := strconv.ParseUint(strings.SplitN(statMap["rocksdb.write.wal"], " : ", 2)[1], 10, 64)
	stat.interval_write_with_wal = write_with_wal - stat.write_with_wal
	stat.write_with_wal = write_with_wal

	write_stall_micros, _ := strconv.ParseUint(strings.SplitN(statMap["rocksdb.stall.micros"], " : ", 2)[1], 10, 64)
	stat.interval_write_stall_micros = write_stall_micros - stat.write_stall_micros
	stat.write_stall_micros = write_stall_micros

	num_seek, _ := strconv.ParseUint(strings.SplitN(statMap["rocksdb.number.db.seek"], " : ", 2)[1], 10, 64)
	stat.interval_num_seek = num_seek - stat.num_seek
	stat.num_seek = num_seek

	num_next, _ := strconv.ParseUint(strings.SplitN(statMap["rocksdb.number.db.next"], " : ", 2)[1], 10, 64)
	stat.interval_num_next = num_next - stat.num_next
	stat.num_next = num_next

	num_prev, _ := strconv.ParseUint(strings.SplitN(statMap["rocksdb.number.db.prev"], " : ", 2)[1], 10, 64)
	stat.interval_num_prev = num_prev - stat.num_prev
	stat.num_prev = num_prev

	num_seek_found, _ := strconv.ParseUint(strings.SplitN(statMap["rocksdb.number.db.seek.found"], " : ", 2)[1], 10, 64)
	stat.interval_num_seek_found = num_seek_found - stat.num_seek_found
	stat.num_seek_found = num_seek_found

	num_next_found, _ := strconv.ParseUint(strings.SplitN(statMap["rocksdb.number.db.next.found"], " : ", 2)[1], 10, 64)
	stat.interval_num_next_found = num_next_found - stat.num_next_found
	stat.num_next_found = num_next_found

	num_prev_found, _ := strconv.ParseUint(strings.SplitN(statMap["rocksdb.number.db.prev.found"], " : ", 2)[1], 10, 64)
	stat.interval_num_prev_found = num_prev_found - stat.num_prev_found
	stat.num_prev_found = num_prev_found

	iter_read_bytes, _ := strconv.ParseUint(strings.SplitN(statMap["rocksdb.db.iter.bytes.read"], " : ", 2)[1], 10, 64)
	stat.interval_iter_read_bytes = iter_read_bytes - stat.iter_read_bytes
	stat.iter_read_bytes = iter_read_bytes
}

func (stat *internalStats) resetIntervalStartTime() {
	stat.interval_start_time = time.Now()
}

func init() {
	ycsb.RegisterDBCreator("rocksdb", rocksDBCreator{})
}
