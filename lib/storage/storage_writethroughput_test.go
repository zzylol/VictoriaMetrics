package storage

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	promlabels "github.com/zzylol/prometheus-sketch-VLDB/prometheus-sketches/model/labels"
	"github.com/zzylol/promsketch"
)

var flagvar int

func init() {
	flag.IntVar(&flagvar, "numts", 1000, "number of timeseries")
}

func TestWriteNormalThroughPut(t *testing.T) {
	scrapeCountBatch := 43200 // seconds, 12 hours
	num_ts := flagvar
	path := "BenchmarkStorageWriteThoughput"
	s := MustOpenStorage(path, 0, 0, 0)
	defer func() {
		s.MustClose()
		if err := os.RemoveAll(path); err != nil {
			t.Fatalf("cannot remove storage at %q: %s", path, err)
		}
	}()

	var mn MetricName
	metricRows := make([]MetricRow, num_ts)
	mn.MetricGroup = []byte("fake_metric")
	for ts_id := 0; ts_id < num_ts; ts_id++ {
		mn.Tags = []Tag{
			{[]byte("machine"), []byte(strconv.Itoa(ts_id))},
		}
		mr := &metricRows[ts_id]
		mr.MetricNameRaw = mn.marshalRaw(mr.MetricNameRaw[:0])
	}

	tNow := time.Now()
	ingestNormalScrapes(s, metricRows, scrapeCountBatch)
	since := time.Since(tNow)

	throughput := 43200.0 * float64(num_ts) / float64(since.Seconds())
	t.Log(num_ts, since.Seconds(), throughput)
}

func ingestNormalScrapes(st *Storage, mrs []MetricRow, scrapeTotCount int) {

	scrapeBatch := 100
	const second = 100
	var count atomic.Int64
	count.Store(0)

	for i := 0; i < scrapeTotCount; i += scrapeBatch {
		currTime := int64(i * second)
		var wg sync.WaitGroup
		lbls := mrs
		for len(lbls) > 0 {
			b := 1000
			batch := lbls[:b]
			lbls = lbls[b:]
			wg.Add(1)
			go func(currTime int64) {
				defer wg.Done()
				for j := 0; j < scrapeBatch; j++ {
					rowsToInsert := make([]MetricRow, 0, len(batch))
					ts := int64(j*second) + currTime
					for _, mr := range batch {
						mr.Value = rand.NormFloat64() * 100000
						mr.Timestamp = ts
						rowsToInsert = append(rowsToInsert, mr)
					}

					if err := st.AddRows(rowsToInsert, defaultPrecisionBits); err != nil {
						panic(fmt.Errorf("cannot add rows to storage: %w", err))
					}
				}
			}(currTime)
		}
		wg.Wait()
	}

	fmt.Println("ingestion completed")
}

func TestWriteZipfThroughPutSketch(t *testing.T) {
	scrapeCountBatch := 43200 // seconds, 12 hours
	num_ts := flagvar
	path := "BenchmarkStorageWriteThoughput"
	s := MustOpenStorage(path, 0, 0, 0)
	defer func() {
		s.MustClose()
		if err := os.RemoveAll(path); err != nil {
			t.Fatalf("cannot remove storage at %q: %s", path, err)
		}
	}()

	promcache := promsketch.NewPromSketches()
	plabels := make([]promlabels.Labels, 0)

	var mn MetricName
	metricRows := make([]MetricRow, num_ts)
	mn.MetricGroup = []byte("fake_metric")
	for ts_id := 0; ts_id < num_ts; ts_id++ {
		mn.Tags = []Tag{
			{[]byte("machine"), []byte(strconv.Itoa(ts_id))},
		}
		fakeMetric := "machine" + strconv.Itoa(ts_id)
		inputLabel := promlabels.FromStrings("fake_metric", fakeMetric)
		plabels = append(plabels, inputLabel)
		mr := &metricRows[ts_id]
		mr.MetricNameRaw = mn.marshalRaw(mr.MetricNameRaw[:0])
		promcache.NewSketchCacheInstance(inputLabel, "quantile_over_time", 100000000, 100000, 10000)
		promcache.NewSketchCacheInstance(inputLabel, "sum_over_time", 100000000, 100000, 10000)
		promcache.NewSketchCacheInstance(inputLabel, "count_over_time", 100000000, 100000, 10000)
		// promcache.NewSketchCacheInstance(inputLabel, "entropy_over_time", 100000000, 100000, 10000)
	}

	tNow := time.Now()
	ingestZipfScrapesSketches(s, metricRows, scrapeCountBatch, promcache, plabels)
	since := time.Since(tNow)

	throughput := 43200.0 * float64(num_ts) / float64(since.Seconds())
	t.Log(num_ts, since.Seconds(), throughput)
}

func ingestZipfScrapesSketches(st *Storage, mrs []MetricRow, scrapeTotCount int, promcache *promsketch.PromSketches, plabels []promlabels.Labels) {
	scrapeBatch := 100
	const second = 100
	var count atomic.Int64
	count.Store(0)
	for i := 0; i < scrapeTotCount; i += scrapeBatch {
		currTime := int64(i * second)
		lbls := mrs
		var wg sync.WaitGroup
		var start_lbl int = 0
		for len(lbls) > 0 {
			b := 1000
			if len(lbls) < 1000 {
				b = len(lbls)
			}
			batch := lbls[:b]
			lbls = lbls[b:]
			wg.Add(1)
			go func(currTime int64, start_lbl int) {
				defer wg.Done()

				var RAND *rand.Rand = rand.New(rand.NewSource(time.Now().Unix()))
				z := rand.NewZipf(RAND, 1.01, 1, uint64(100000))

				var wg_sketch sync.WaitGroup
				wg_sketch.Add(1)
				go func(start_lbl int, currTime int64) {
					defer wg_sketch.Done()
					for j := 0; j < scrapeBatch; j++ {
						ts := int64(j*second) + currTime
						var wg_labels sync.WaitGroup
						// fmt.Println("startlbl=", start_lbl)
						for k := 0; k < len(batch); k += 100 {
							wg_labels.Add(1)
							go func(k int, start_lbl int, ts int64) {
								defer wg_labels.Done()
								var RAND1 *rand.Rand = rand.New(rand.NewSource(time.Now().Unix()))
								z1 := rand.NewZipf(RAND1, 1.01, 1, uint64(100000))
								for l := k; l < k+100; l++ {
									// fmt.Println(start_lbl+l, ts)
									err := promcache.SketchInsert(plabels[start_lbl+l], ts, float64(z1.Uint64()))
									if err != nil {
										panic(err)
									}
								}
							}(k, start_lbl, ts)
						}
						wg_labels.Wait()
					}
				}(start_lbl, currTime)

				for j := 0; j < scrapeBatch; j++ {
					rowsToInsert := make([]MetricRow, 0, len(batch))
					ts := int64(j*second) + currTime
					for _, mr := range batch {
						mr.Value = float64(z.Uint64())
						mr.Timestamp = ts
						rowsToInsert = append(rowsToInsert, mr)
					}

					if err := st.AddRows(rowsToInsert, defaultPrecisionBits); err != nil {
						panic(fmt.Errorf("cannot add rows to storage: %w", err))
					}
				}
				wg_sketch.Wait()
			}(currTime, start_lbl)
			start_lbl += b
		}
		wg.Wait()
	}

	fmt.Println("ingestion completed")
}

func TestWriteZipfThroughPut(t *testing.T) {
	scrapeCountBatch := 43200 // seconds, 12 hours
	num_ts := flagvar
	path := "BenchmarkStorageWriteThoughput"
	s := MustOpenStorage(path, 0, 0, 0)
	defer func() {
		s.MustClose()
		if err := os.RemoveAll(path); err != nil {
			t.Fatalf("cannot remove storage at %q: %s", path, err)
		}
	}()

	var mn MetricName
	metricRows := make([]MetricRow, num_ts)
	mn.MetricGroup = []byte("fake_metric")
	for ts_id := 0; ts_id < num_ts; ts_id++ {
		mn.Tags = []Tag{
			{[]byte("machine"), []byte(strconv.Itoa(ts_id))},
		}
		mr := &metricRows[ts_id]
		mr.MetricNameRaw = mn.marshalRaw(mr.MetricNameRaw[:0])
	}

	tNow := time.Now()
	ingestZipfScrapes(s, metricRows, scrapeCountBatch)
	since := time.Since(tNow)

	throughput := 43200.0 * float64(num_ts) / float64(since.Seconds())
	t.Log(num_ts, since.Seconds(), throughput)
}

func ingestZipfScrapes(st *Storage, mrs []MetricRow, scrapeTotCount int) {

	scrapeBatch := 100
	const second = 100
	var count atomic.Int64
	count.Store(0)
	for i := 0; i < scrapeTotCount; i += scrapeBatch {
		currTime := int64(i * second)
		lbls := mrs
		var wg sync.WaitGroup
		for len(lbls) > 0 {
			b := 1000
			batch := lbls[:b]
			lbls = lbls[b:]
			wg.Add(1)
			go func(currTime int64) {
				defer wg.Done()

				var s float64 = 1.01
				var v float64 = 1
				var RAND *rand.Rand = rand.New(rand.NewSource(time.Now().Unix()))
				z := rand.NewZipf(RAND, s, v, uint64(100000))

				for j := 0; j < scrapeBatch; j++ {
					rowsToInsert := make([]MetricRow, 0, len(batch))
					ts := int64(j*second) + currTime
					for _, mr := range batch {
						mr.Value = float64(z.Uint64())
						mr.Timestamp = ts
						rowsToInsert = append(rowsToInsert, mr)
					}

					if err := st.AddRows(rowsToInsert, defaultPrecisionBits); err != nil {
						panic(fmt.Errorf("cannot add rows to storage: %w", err))
					}
				}
			}(currTime)
		}
		wg.Wait()
	}

	fmt.Println("ingestion completed")
}
