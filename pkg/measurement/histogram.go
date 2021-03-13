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

package measurement

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"sync/atomic"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type histogram struct {
	boundCounts         util.ConcurrentMap //时延区间Map
	intervalBoundCounts util.ConcurrentMap //一次统计间隔（如10s）中的时延区间Map
	boundInterval       int64              //时延区间的跨度（默认1000）
	count               int64
	intervalCount       int64 //一次统计间隔（如10s）中的op数量
	sum                 int64
	intervalSum         int64 //一次统计间隔的时长
	min                 int64
	intervalMin         int64 //一次统计间隔（如10s）中的最小时延
	max                 int64
	intervalMax         int64 //一次统计间隔（如10s）中的最大时延
	startTime           time.Time
	intervalStartTime   time.Time //一次统计间隔的开始时刻
}

// Metric name.
const (
	HistogramBuckets        = "histogram.buckets"
	HistogramBucketsDefault = 1000
	ShardCount              = "cmap.shardCount"
	ShardCountDefault       = 32
	ELAPSED                 = "ELAPSED"
	INTERVALELAPSED         = "INTERVALELAPSED"
	COUNT                   = "COUNT"
	INTERVALCOUNT           = "INTERVALCOUNT"
	QPS                     = "QPS"
	INTERVALQPS             = "INTERVALQPS"
	AVG                     = "AVG"
	INTERVALAVG             = "INTERVALAVG"
	MIN                     = "MIN"
	INTERVALMIN             = "INTERVALMIN"
	MAX                     = "MAX"
	INTERVALMAX             = "INTERVALMAX"
	PER99TH                 = "PER99TH"
	INTERVALPER99TH         = "INTERVALPER99TH"
	PER999TH                = "PER999TH"
	INTERVALPER999TH        = "INTERVALPER999TH"
	PER9999TH               = "PER9999TH"
	INTERVALPER9999TH       = "INTERVALPER9999TH"
)

func (h *histogram) Info() ycsb.MeasurementInfo {
	res := h.getInfo()
	delete(res, ELAPSED)
	return newHistogramInfo(res)
}

func newHistogram(p *properties.Properties) *histogram {
	h := new(histogram)
	h.startTime = time.Now()
	h.intervalStartTime = time.Now()
	h.boundCounts = util.New(p.GetInt(ShardCount, ShardCountDefault))
	h.intervalBoundCounts = util.New(ShardCountDefault)
	h.boundInterval = p.GetInt64(HistogramBuckets, HistogramBucketsDefault)
	h.min = math.MaxInt64
	h.intervalMin = math.MaxInt64
	h.max = math.MinInt64
	h.intervalMax = math.MinInt64
	return h
}

func (h *histogram) Measure(latency time.Duration) {
	n := int64(latency / time.Microsecond)

	atomic.AddInt64(&h.sum, n)
	atomic.AddInt64(&h.intervalSum, n)
	atomic.AddInt64(&h.count, 1)
	atomic.AddInt64(&h.intervalCount, 1)
	bound := int(n / h.boundInterval)
	h.boundCounts.Upsert(bound, 1, func(ok bool, existedValue int64, newValue int64) int64 {
		if ok {
			return existedValue + newValue
		}
		return newValue
	})

	h.intervalBoundCounts.Upsert(bound, 1, func(ok bool, existedValue int64, newValue int64) int64 {
		if ok {
			return existedValue + newValue
		}
		return newValue
	})

	for {
		oldMin := atomic.LoadInt64(&h.min)
		if n >= oldMin {
			break
		}

		if atomic.CompareAndSwapInt64(&h.min, oldMin, n) {
			break
		}
	}

	for {
		oldMax := atomic.LoadInt64(&h.max)
		if n <= oldMax {
			break
		}

		if atomic.CompareAndSwapInt64(&h.max, oldMax, n) {
			break
		}
	}

	for {
		intervalOldMin := atomic.LoadInt64(&h.intervalMin)
		if n >= intervalOldMin {
			break
		}

		if atomic.CompareAndSwapInt64(&h.intervalMin, intervalOldMin, n) {
			break
		}
	}

	for {
		intervalOldMax := atomic.LoadInt64(&h.intervalMax)
		if n <= intervalOldMax {
			break
		}

		if atomic.CompareAndSwapInt64(&h.intervalMax, intervalOldMax, n) {
			break
		}
	}
}

func (h *histogram) Summary() string {
	res := h.getInfo()
	h.IntervalReset() //每次打印信息时重置interval量

	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("Takes(s): %.1f, ", res[ELAPSED]))
	buf.WriteString(fmt.Sprintf("Count: %d, ", res[COUNT]))
	buf.WriteString(fmt.Sprintf("OPS: %.1f, ", res[QPS]))
	buf.WriteString(fmt.Sprintf("Avg(us): %d, ", res[AVG]))
	buf.WriteString(fmt.Sprintf("Min(us): %d, ", res[MIN]))
	buf.WriteString(fmt.Sprintf("Max(us): %d, ", res[MAX]))
	buf.WriteString(fmt.Sprintf("99th(us): %d, ", res[PER99TH]))
	buf.WriteString(fmt.Sprintf("99.9th(us): %d, ", res[PER999TH]))
	buf.WriteString(fmt.Sprintf("99.99th(us): %d; ", res[PER9999TH]))
	buf.WriteString(fmt.Sprintf("IntervalTakes(s): %.1f, ", res[INTERVALELAPSED]))
	buf.WriteString(fmt.Sprintf("IntervalCount: %d, ", res[INTERVALCOUNT]))
	buf.WriteString(fmt.Sprintf("IntervalOPS: %.1f, ", res[INTERVALQPS]))
	buf.WriteString(fmt.Sprintf("IntervalAvg(us): %d, ", res[INTERVALAVG]))
	buf.WriteString(fmt.Sprintf("IntervalMin(us): %d, ", res[INTERVALMIN]))
	buf.WriteString(fmt.Sprintf("IntervalMax(us): %d, ", res[INTERVALMAX]))
	buf.WriteString(fmt.Sprintf("Interval99th(us): %d, ", res[INTERVALPER99TH]))
	buf.WriteString(fmt.Sprintf("Interval99.9th(us): %d, ", res[INTERVALPER999TH]))
	buf.WriteString(fmt.Sprintf("Interval99.99th(us): %d", res[INTERVALPER9999TH]))

	return buf.String()
}

func (h *histogram) getInfo() map[string]interface{} {
	min := atomic.LoadInt64(&h.min)
	intervalMin := atomic.LoadInt64(&h.intervalMin)
	max := atomic.LoadInt64(&h.max)
	intervalMax := atomic.LoadInt64(&h.intervalMax)
	sum := atomic.LoadInt64(&h.sum)
	intervalSum := atomic.LoadInt64(&h.intervalSum)
	count := atomic.LoadInt64(&h.count)
	intervalCount := atomic.LoadInt64(&h.intervalCount)

	bounds := h.boundCounts.Keys()
	intervalBounds := h.intervalBoundCounts.Keys()
	sort.Ints(bounds)
	sort.Ints(intervalBounds)

	avg := int64(float64(sum) / float64(count))
	intervalAvg := int64(float64(intervalSum) / float64(intervalCount))
	per99 := 0
	intervalPer99 := 0
	per999 := 0
	intervalPer999 := 0
	per9999 := 0
	intervalPer9999 := 0

	opCount := int64(0)
	for _, bound := range bounds {
		boundCount, _ := h.boundCounts.Get(bound)
		opCount += boundCount
		per := float64(opCount) / float64(count)
		if per99 == 0 && per >= 0.99 {
			per99 = (bound + 1) * 1000
		}

		if per999 == 0 && per >= 0.999 {
			per999 = (bound + 1) * 1000
		}

		if per9999 == 0 && per >= 0.9999 {
			per9999 = (bound + 1) * 1000
		}
	}

	intervalOpCount := int64(0)
	for _, intervalBound := range intervalBounds {
		intervalBoundCount, _ := h.intervalBoundCounts.Get(intervalBound)
		intervalOpCount += intervalBoundCount
		intervalPer := float64(intervalOpCount) / float64(intervalCount)
		if intervalPer99 == 0 && intervalPer >= 0.99 {
			intervalPer99 = (intervalBound + 1) * 1000
		}

		if intervalPer999 == 0 && intervalPer >= 0.999 {
			intervalPer999 = (intervalBound + 1) * 1000
		}

		if intervalPer9999 == 0 && intervalPer >= 0.9999 {
			intervalPer9999 = (intervalBound + 1) * 1000
		}
	}

	elapsed := time.Now().Sub(h.startTime).Seconds()
	intervalElapsed := time.Now().Sub(h.intervalStartTime).Seconds()
	qps := float64(count) / elapsed
	intervalQps := float64(intervalCount) / intervalElapsed
	res := make(map[string]interface{})
	res[ELAPSED] = elapsed
	res[INTERVALELAPSED] = intervalElapsed
	res[COUNT] = count
	res[INTERVALCOUNT] = intervalCount
	res[QPS] = qps
	res[INTERVALQPS] = intervalQps
	res[AVG] = avg
	res[INTERVALAVG] = intervalAvg
	res[MIN] = min
	res[INTERVALMIN] = intervalMin
	res[MAX] = max
	res[INTERVALMAX] = intervalMax
	res[PER99TH] = per99
	res[INTERVALPER99TH] = intervalPer99
	res[PER999TH] = per999
	res[INTERVALPER999TH] = intervalPer999
	res[PER9999TH] = per9999
	res[INTERVALPER9999TH] = intervalPer9999

	return res
}

func (h *histogram) IntervalReset() {
	h.intervalStartTime = time.Now()
	h.intervalBoundCounts = util.New(ShardCountDefault)
	atomic.StoreInt64(&h.intervalMin, math.MaxInt64)
	atomic.StoreInt64(&h.intervalMax, math.MinInt64)
	atomic.StoreInt64(&h.intervalCount, 0)
	atomic.StoreInt64(&h.intervalSum, 0)
}

type histogramInfo struct {
	info map[string]interface{}
}

func newHistogramInfo(info map[string]interface{}) *histogramInfo {
	return &histogramInfo{info: info}
}

func (hi *histogramInfo) Get(metricName string) interface{} {
	if value, ok := hi.info[metricName]; ok {
		return value
	}
	return nil
}
