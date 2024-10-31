package common

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

	"github.com/zzylol/VictoriaMetrics/app/vmsketch"
	"github.com/zzylol/VictoriaMetrics/lib/storage"
)

const defaultPrecisionBits = 4

var flagvar int

func init() {
	flag.IntVar(&flagvar, "numts", 1000, "number of timeseries")
}

func TestWriteZipfThroughPut(t *testing.T) {
	scrapeCountBatch := 2160000 // seconds, 12 hours
	num_ts := flagvar
	path := "BenchmarkCtxInsertThoughput"
	s := storage.MustOpenStorage(path, 0, 0, 0)
	defer func() {
		s.MustClose()
		if err := os.RemoveAll(path); err != nil {
			t.Fatalf("cannot remove storage at %q: %s", path, err)
		}
	}()

	vmsketch.Init()

	var mn storage.MetricName
	metricRows := make([]storage.MetricRow, num_ts)
	mn.MetricGroup = []byte("fake_metric")
	for ts_id := 0; ts_id < num_ts; ts_id++ {
		mn.Tags = []storage.Tag{
			{Key: []byte("machine"), Value: []byte(strconv.Itoa(ts_id))},
		}
		mr := &metricRows[ts_id]
		mr.MetricNameRaw = mn.MarshalRaw(mr.MetricNameRaw[:0])

		err := vmsketch.RegisterMetricNameFuncName(&mn, "distinct_over_time", 100000000, 1000000)
		if err != nil {
			panic(fmt.Errorf("Failed register vmsketch cache EHuniv instance %w", err))
		}
		/*

				err := vmsketch.RegisterMetricNameFuncName(&mn, "avg_over_time", 100000000, 1000000)
				if err != nil {
					panic(fmt.Errorf("Failed register vmsketch cache Sampling instance %w", err))
				}

			err := vmsketch.RegisterMetricNameFuncName(&mn, "quantile_over_time", 1000000, 10000)
			if err != nil {
				panic(fmt.Errorf("Failed register vmsketch cache EHKLL instance %w", err))
			}
		*/
	}

	tNow := time.Now()
	ingestZipfScrapes(s, metricRows, scrapeCountBatch)
	since := time.Since(tNow)

	throughput := float64(scrapeCountBatch) * float64(num_ts) / float64(since.Seconds())
	t.Log(num_ts, since.Seconds(), throughput)
}

func ingestZipfScrapes(st *storage.Storage, mrs []storage.MetricRow, scrapeTotCount int) {
	scrapeBatch := 100
	const second = 100
	var count atomic.Int64
	count.Store(0)
	for i := 0; i < scrapeTotCount; i += scrapeBatch {
		var wg sync.WaitGroup
		currTime := int64(i * second)
		lbls := mrs
		for len(lbls) > 0 {
			b := 100
			if len(lbls) < b {
				b = len(lbls)
			}
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
					var wg_sketch sync.WaitGroup
					rowsToInsert := make([]storage.MetricRow, 0, len(batch))
					ts := int64(j*second) + currTime
					for _, mr := range batch {
						mr.Value = float64(z.Uint64())
						// mr.Value = float64(rand.Float64() * 100000)
						mr.Timestamp = ts
						rowsToInsert = append(rowsToInsert, mr)
					}

					wg_sketch.Add(1)
					go func(rowsToInsert []storage.MetricRow) {
						defer wg_sketch.Done()
						for _, mr := range rowsToInsert {
							vmsketch.AddRow(mr.MetricNameRaw, mr.Timestamp, mr.Value)
						}
					}(rowsToInsert)
					if err := st.AddRows(rowsToInsert, defaultPrecisionBits); err != nil {
						panic(fmt.Errorf("cannot add rows to storage: %w", err))
					}
					wg_sketch.Wait()

				}

			}(currTime)
		}
		wg.Wait()
	}

	fmt.Println("ingestion completed")
}
