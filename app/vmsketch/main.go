package vmsketch

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/froot-netsys/promsketch"
	"github.com/zzylol/VictoriaMetrics/app/vmselect/searchutils"
	"github.com/zzylol/VictoriaMetrics/lib/bytesutil"
	"github.com/zzylol/VictoriaMetrics/lib/cgroup"
	"github.com/zzylol/VictoriaMetrics/lib/logger"
	"github.com/zzylol/VictoriaMetrics/lib/querytracer"
	"github.com/zzylol/VictoriaMetrics/lib/storage"
	"github.com/zzylol/VictoriaMetrics/lib/syncwg"
)

var gomaxprocs = cgroup.AvailableCPUs()

var defaultMaxWorkersPerQuery = func() int {
	// maxWorkersLimit is the maximum number of CPU cores, which can be used in parallel
	// for processing an average query, without significant impact on inter-CPU communications.
	const maxWorkersLimit = 32

	n := gomaxprocs
	if n > maxWorkersLimit {
		n = maxWorkersLimit
	}
	return n
}()

// MaxWorkers returns the maximum number of concurrent goroutines, which can be used by RunParallel()
func MaxWorkers() int {
	n := defaultMaxWorkersPerQuery
	if n > gomaxprocs {
		// There is no sense in running more than gomaxprocs CPU-bound concurrent workers,
		// since this may worsen the query performance.
		n = gomaxprocs
	}
	return n
}

var (
	seriesReadPerQuery = metrics.NewHistogram(`vmsketch_series_read_per_query`)
)

// SketchCache is a rollup cache with sketches.
//
// Every sketch call must be wrapped into WG.Add(1) ... WG.Done()
// for proper graceful shutdown when Stop is called.
var SketchCache *promsketch.VMSketches

// WG must be incremented before Storage call.
//
// Use syncwg instead of sync, since Add is called from concurrent goroutines.
var WG syncwg.WaitGroup

// Init initializes vmsketch.
func Init() {
	SketchCache = promsketch.NewVMSketches()
}

// Stop stops the vmsketch
func Stop() {
	logger.Infof("gracefully closing the sketch cache")
	startTime := time.Now()
	WG.WaitAndBlock()
	SketchCache.Stop()
	logger.Infof("successfully stopped the sketch cache in %.3f seconds", time.Since(startTime).Seconds())
}

// RegisterMetricNames registers all the metrics from mrs in the storage.
func RegisterMetricNameFuncName(mn *storage.MetricName, funcName string, window int64, item_window int64) error {
	WG.Add(1)
	err := SketchCache.NewVMSketchCacheInstance(mn, funcName, window, item_window)
	WG.Done()
	return err
}

// SearchMetricNames returns metric names for the given tfss on the given tr.
func SearchAndUpdateWindowMetricNameFuncName(mn *storage.MetricName, funcName string, window int64) bool {
	WG.Add(1)
	lookup := SketchCache.LookupAndUpdateWindowMetricNameFuncName(mn, funcName, window)
	WG.Done()
	return lookup
}

// GetSeriesCount returns the number of time series in the storage.
func GetSeriesCount() (uint64, error) {
	WG.Add(1)
	n := SketchCache.GetSeriesCount()
	WG.Done()
	return n, nil
}

// Result is a single timeseries result.
//
// Search returns Result slice.
type SketchResult struct {
	MetricName *storage.MetricName
	sketchIns  *promsketch.SketchInstances
}

// Results holds results returned from ProcessSearchQuery.
type SketchResults struct {
	deadline   searchutils.Deadline
	sketchInss []SketchResult
}

func (sr *SketchResult) Eval(mn *storage.MetricName, funcName string, args []float64, mint, maxt, cur_time int64) float64 {
	return sr.sketchIns.Eval(mn, funcName, args, mint, maxt, cur_time)
}

// Len returns the number of results in srs.
func (srs *SketchResults) Len() int {
	return len(srs.sketchInss)
}

// Cancel cancels srs work.
func (srs *SketchResults) Cancel() {
	srs.mustClose()
}

func (srs *SketchResults) mustClose() {
	// put something to memory pool
}

type timeseriesWork struct {
	mustStop *atomic.Bool
	deadline searchutils.Deadline
	sr       *SketchResult
	f        func(sr *SketchResult, workerID uint) error
	err      error
}

func (tsw *timeseriesWork) do(workerID uint) error {

	if tsw.mustStop.Load() {
		return nil
	}

	if tsw.deadline.Exceeded() {
		tsw.mustStop.Store(true)
		return fmt.Errorf("timeout exceeded during query execution: %s", tsw.deadline.String())
	}

	if err := tsw.f(tsw.sr, workerID); err != nil {
		tsw.mustStop.Store(true)
		return err
	}

	return nil
}

func timeseriesWorker(qt *querytracer.Tracer, workChs []chan *timeseriesWork, workerID uint) {

	// Perform own work at first.
	rowsProcessed := 0
	seriesProcessed := 0
	ch := workChs[workerID]
	for tsw := range ch {
		tsw.err = tsw.do(workerID)
		seriesProcessed++
	}
	qt.Printf("own work processed: series=%d, samples=%d", seriesProcessed, rowsProcessed)

	// Then help others with the remaining work.
	rowsProcessed = 0
	seriesProcessed = 0
	for i := uint(1); i < uint(len(workChs)); i++ {
		idx := (i + workerID) % uint(len(workChs))
		ch := workChs[idx]
		for len(ch) > 0 {
			// Do not call runtime.Gosched() here in order to give a chance
			// the real owner of the work to complete it, since it consumes additional CPU
			// and slows down the code on systems with big number of CPU cores.
			// See https://github.com/zzylol/VictoriaMetrics/issues/3966#issuecomment-1483208419

			// It is expected that every channel in the workChs is already closed,
			// so the next line should return immediately.
			tsw, ok := <-ch
			if !ok {
				break
			}
			tsw.err = tsw.do(workerID)

			seriesProcessed++
		}
	}
	qt.Printf("others work processed: series=%d, samples=%d", seriesProcessed, rowsProcessed)

}

// RunParallel runs f in parallel for all the results from srs.
//
// f shouldn't hold references to rs after returning.
// workerID is the id of the worker goroutine that calls f. The workerID is in the range [0..MaxWorkers()-1].
// Data processing is immediately stopped if f returns non-nil error.
//
// srs becomes unusable after the call to RunParallel.
func (srs *SketchResults) RunParallel(qt *querytracer.Tracer, f func(sr *SketchResult, workerID uint) error) error {
	qt = qt.NewChild("parallel process of fetched sketch instances")
	defer srs.mustClose()

	rowsProcessedTotal, err := srs.runParallel(qt, f)
	seriesProcessedTotal := len(srs.sketchInss)

	seriesReadPerQuery.Update(float64(seriesProcessedTotal))

	qt.Donef("series=%d, samples=%d", seriesProcessedTotal, rowsProcessedTotal)

	return err
}

func (srs *SketchResults) runParallel(qt *querytracer.Tracer, f func(sr *SketchResult, workerID uint) error) (int, error) {
	tswsLen := len(srs.sketchInss)
	if tswsLen == 0 {
		// Nothing to process
		return 0, nil
	}

	var mustStop atomic.Bool
	initTimeseriesWork := func(tsw *timeseriesWork, sr *SketchResult) {
		tsw.deadline = srs.deadline
		tsw.sr = sr
		tsw.f = f
		tsw.mustStop = &mustStop
	}
	maxWorkers := MaxWorkers()

	if maxWorkers == 1 || tswsLen == 1 {
		// It is faster to process time series in the current goroutine.
		var tsw timeseriesWork

		rowsProcessedTotal := 0
		var err error
		for i := range srs.sketchInss {
			initTimeseriesWork(&tsw, &srs.sketchInss[i])
			err = tsw.do(0)

			if err != nil {
				break
			}
		}

		return rowsProcessedTotal, err
	}

	// Slow path - spin up multiple local workers for parallel data processing.
	// Do not use global workers pool, since it increases inter-CPU memory ping-poing,
	// which reduces the scalability on systems with many CPU cores.

	// Prepare the work for workers.
	tsws := make([]timeseriesWork, len(srs.sketchInss))
	for i := range srs.sketchInss {
		initTimeseriesWork(&tsws[i], &srs.sketchInss[i])
	}

	// Prepare worker channels.
	workers := len(tsws)
	if workers > maxWorkers {
		workers = maxWorkers
	}
	itemsPerWorker := (len(tsws) + workers - 1) / workers
	workChs := make([]chan *timeseriesWork, workers)
	for i := range workChs {
		workChs[i] = make(chan *timeseriesWork, itemsPerWorker)
	}

	// Spread work among workers.
	for i := range tsws {
		idx := i % len(workChs)
		workChs[idx] <- &tsws[i]
	}
	// Mark worker channels as closed.
	for _, workCh := range workChs {
		close(workCh)
	}

	// Start workers and wait until they finish the work.
	var wg sync.WaitGroup
	for i := range workChs {
		wg.Add(1)
		qtChild := qt.NewChild("worker #%d", i)
		go func(workerID uint) {
			timeseriesWorker(qtChild, workChs, workerID)
			qtChild.Done()
			wg.Done()
		}(uint(i))
	}
	wg.Wait()

	// Collect results.
	var firstErr error
	rowsProcessedTotal := 0
	for i := range tsws {
		tsw := &tsws[i]
		if tsw.err != nil && firstErr == nil {
			// Return just the first error, since other errors are likely duplicate the first error.
			firstErr = tsw.err
		}
	}
	return rowsProcessedTotal, firstErr
}

func OutputTimeseriesCoverage(mns []string, funcNames []string) {
	for _, mnstr := range mns {
		mn := storage.GetMetricName()
		defer storage.PutMetricName(mn)
		if err := mn.Unmarshal(bytesutil.ToUnsafeBytes(mnstr)); err != nil {
			return
		}
		SketchCache.OutputTimeseriesCoverage(mn, funcNames)
	}
}

func SearchTimeSeriesCoverage(start, end int64, mns []string, funcNames []string, maxMetrics int, deadline searchutils.Deadline) (*SketchResults, bool, error) {
	srs := &SketchResults{
		deadline:   deadline,
		sketchInss: make([]SketchResult, 0),
	}

	for _, mnstr := range mns {
		mn := &storage.MetricName{}
		if err := mn.Unmarshal(bytesutil.ToUnsafeBytes(mnstr)); err != nil {
			return nil, false, fmt.Errorf("cannot unmarshal metricName %q: %w", mnstr, err)
		}
		if deadline.Exceeded() {
			return nil, false, fmt.Errorf("timeout exceeded before starting the query processing: %s", deadline.String())
		}

		sketchIns, lookup := SketchCache.LookupMetricNameFuncNamesTimeRange(mn, funcNames, start, end)
		if sketchIns == nil {
			return nil, false, fmt.Errorf("sketchIns doesn't allocated")
		}
		if !lookup {
			fmt.Println(sketchIns.PrintMinMaxTimeRange(mn, funcNames[0]))
			return nil, false, fmt.Errorf("sketch cache doesn't cover metricName %q", mnstr)
		}
		srs.sketchInss = append(srs.sketchInss, SketchResult{sketchIns: sketchIns, MetricName: mn})
	}
	return srs, true, nil
}

// AddRows adds mrs to the sketch cache.
//
// The caller should limit the number of concurrent calls to AddRows() in order to limit memory usage.
func AddRows(mrs []storage.MetricRow) error {
	WG.Add(1)

	var firstWarn error
	mn := storage.GetMetricName()
	defer storage.PutMetricName(mn)

	for i := range mrs {
		if err := mn.UnmarshalRaw(mrs[i].MetricNameRaw); err != nil {
			if firstWarn != nil {
				firstWarn = fmt.Errorf("cannot umarshal MetricNameRaw %q: %w", mrs[i].MetricNameRaw, err)
			}
		}

		err := SketchCache.AddRow(mn, mrs[i].Timestamp, mrs[i].Value)
		if err != nil && firstWarn != nil {
			firstWarn = fmt.Errorf("cannot add row to sketch cache MetricNameRaw %q: %w", mrs[i].MetricNameRaw, err)
		}
	}
	WG.Done()
	return firstWarn
}

func AddRow(metricNameRaw []byte, timestamp int64, value float64) error {
	mn := storage.GetMetricName()
	defer storage.PutMetricName(mn)
	if err := mn.UnmarshalRaw(metricNameRaw); err != nil {
		return fmt.Errorf("cannot umarshal MetricNameRaw %q: %w", metricNameRaw, err)
	}

	// fmt.Println(mn, timestamp, value)
	return SketchCache.AddRow(mn, timestamp, value)
}
