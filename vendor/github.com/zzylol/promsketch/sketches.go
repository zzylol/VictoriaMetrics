package promsketch

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zzylol/prometheus-sketch-VLDB/prometheus-sketches/model/labels"
	"github.com/zzylol/prometheus-sketch-VLDB/prometheus-sketches/util/annotations"

	"github.com/enriquebris/goconcurrentqueue"
)

type SketchType int

const (
	SHUniv SketchType = iota + 1
	// EHUniv
	EHCount
	// SHCount
	// EHKLL
	EHDD
	EffSum
	EffSum2
)

var funcSketchMap = map[string]([]SketchType){
	"avg_over_time":      {EffSum, EHCount},
	"count_over_time":    {EHCount},
	"entropy_over_time":  {SHUniv},
	"max_over_time":      {EHDD},
	"min_over_time":      {EHDD},
	"stddev_over_time":   {EffSum, EHCount},
	"stdvar_over_time":   {EffSum, EHCount},
	"sum_over_time":      {EffSum},
	"sum2_over_time":     {EffSum2},
	"Card_over_time":     {SHUniv},
	"L1_over_time":       {SHUniv},
	"L2_over_time":       {SHUniv},
	"quantile_over_time": {EHDD},
}

// SketchConfig bundles sketch configurations for promsketch
type SketchConfig struct {
	// 	CM_config       CMConfig
	CS_config      CSConfig
	Univ_config    UnivConfig
	SH_univ_config SHUnivConfig
	// 	SH_count_config SHCountConfig
	EH_count_config EHCountConfig
	// 	EH_univ_config  EHUnivConfig
	// 	EH_kll_config   EHKLLConfig
	EH_dd_config   EHDDConfig
	EffSum_config  EffSumConfig
	EffSum2_config EffSum2Config
}

type CMConfig struct {
	Row_no int
	Col_no int
}

type CSConfig struct {
	Row_no int
	Col_no int
}

type UnivConfig struct {
	TopK_size int
	Row_no    int
	Col_no    int
	Layer     int
}

type SHUnivConfig struct {
	Beta             float64
	Time_window_size int64
	Univ_config      UnivConfig
}

type SHCountConfig struct {
	Beta             float64
	Time_window_size int64
}

type EHCountConfig struct {
	K                int64
	Time_window_size int64
}

type EHUnivConfig struct {
	K                int64
	Time_window_size int64
	Univ_config      UnivConfig
}

type EHKLLConfig struct {
	K                int64
	Kll_k            int
	Time_window_size int64
}

type EHDDConfig struct {
	K                int64
	Time_window_size int64
	DDAccuracy       float64
}

type EffSumConfig struct {
	Item_window_size int64
	Time_window_size int64
	Epsilon          float64
	R                float64
}

type EffSum2Config struct {
	Item_window_size int64
	Time_window_size int64
	Epsilon          float64
	R                float64
}

// Each series maintain their own sketches
type SketchInstances struct {
	// 	shc    *SmoothHistogramCount
	shuniv *SmoothHistogramUnivMon
	// 	ehuniv *ExpoHistogramUniv
	// 	ehkll  *ExpoHistogramKLL
	ehdd    *ExpoHistogramDD
	ehc     *ExpoHistogramCount
	EffSum  *EfficientSum
	EffSum2 *EfficientSum
}

// TODO: can be more efficient timeseries management?
type memSeries struct {
	id              TSId
	lset            labels.Labels
	sketchInstances *SketchInstances
	memoryPart      goconcurrentqueue.Queue
}

type sketchSeriesHashMap struct {
	unique    map[uint64]*memSeries
	conflicts map[uint64][]*memSeries
}

func (m *sketchSeriesHashMap) get(hash uint64, lset labels.Labels) *memSeries {
	if s, found := m.unique[hash]; found {
		if labels.Equal(s.lset, lset) {
			return s
		}
	}
	for _, s := range m.conflicts[hash] {
		if labels.Equal(s.lset, lset) {
			return s
		}
	}
	return nil
}

func (m *sketchSeriesHashMap) set(hash uint64, s *memSeries) {
	if existing, found := m.unique[hash]; !found || labels.Equal(existing.lset, s.lset) {
		m.unique[hash] = s
		return
	}
	if m.conflicts == nil {
		m.conflicts = make(map[uint64][]*memSeries)
	}
	l := m.conflicts[hash]
	for i, prev := range l {
		if labels.Equal(prev.lset, s.lset) {
			l[i] = s
			return
		}
	}
	m.conflicts[hash] = append(l, s)
}

func (m *sketchSeriesHashMap) del(hash uint64, id TSId) {
	var rem []*memSeries
	unique, found := m.unique[hash]
	switch {
	case !found: // Supplied hash is not stored.
		return
	case unique.id == id:
		conflicts := m.conflicts[hash]
		if len(conflicts) == 0 { // Exactly one series with this hash was stored
			delete(m.unique, hash)
			return
		}
		m.unique[hash] = conflicts[0] // First remaining series goes in 'unique'.
		rem = conflicts[1:]           // Keep the rest.
	default: // The series to delete is somewhere in 'conflicts'. Keep all the ones that don't match.
		for _, s := range m.conflicts[hash] {
			if s.id != id {
				rem = append(rem, s)
			}
		}
	}
	if len(rem) == 0 {
		delete(m.conflicts, hash)
	} else {
		m.conflicts[hash] = rem
	}
}

// sketchSeries holds series by ID and also by hash of their labels.
// ID-based lookups via getByID() are preferred over getByHash() for performance reasons.
type sketchSeries struct {
	size   int
	id     TSId
	hashes []sketchSeriesHashMap
	series []map[TSId]*memSeries
	locks  []stripeLock
}

type PromSketches struct {
	lastSeriesID atomic.Uint64
	numSeries    atomic.Uint64
	series       *sketchSeries
}

func (s *sketchSeries) getByID(id TSId) *memSeries {
	if s.size == 0 {
		return nil
	}
	i := uint64(id) & uint64(s.size-1)

	s.locks[i].RLock()
	series := s.series[i][id]
	s.locks[i].RUnlock()

	return series
}

func (s *sketchSeries) getByHash(hash uint64, lset labels.Labels) *memSeries {
	if s.size == 0 {
		return nil
	}
	i := hash & uint64(s.size-1)
	s.locks[i].RLock()
	series := s.hashes[i].get(hash, lset)
	s.locks[i].RUnlock()

	return series
}

type TSId int

func newSlidingHistorgrams(s *memSeries, stype SketchType, sc *SketchConfig) error {
	if s.sketchInstances == nil {
		s.sketchInstances = &SketchInstances{}
	}

	if stype == SHUniv && s.sketchInstances.shuniv == nil {
		s.sketchInstances.shuniv = SmoothInitUnivMon(sc.SH_univ_config.Beta, sc.SH_univ_config.Time_window_size)
	}

	if stype == EHDD && s.sketchInstances.ehdd == nil {
		s.sketchInstances.ehdd = ExpoInitDD(sc.EH_dd_config.K, sc.EH_dd_config.Time_window_size, sc.EH_dd_config.DDAccuracy)
	}

	if stype == EHCount && s.sketchInstances.ehc == nil {
		s.sketchInstances.ehc = ExpoInitCount(sc.EH_count_config.K, sc.EH_count_config.Time_window_size)
	}

	if stype == EffSum && s.sketchInstances.EffSum == nil {
		s.sketchInstances.EffSum = NewEfficientSum(sc.EffSum_config.Item_window_size, sc.EffSum_config.Time_window_size, sc.EffSum_config.Epsilon, sc.EffSum_config.R)
	}

	if stype == EffSum2 && s.sketchInstances.EffSum2 == nil {
		s.sketchInstances.EffSum2 = NewEfficientSum(sc.EffSum2_config.Item_window_size, sc.EffSum2_config.Time_window_size, sc.EffSum2_config.Epsilon, sc.EffSum2_config.R)
	}

	/*
		if stype == EHUniv && s.sketchInstances.ehuniv == nil {
			s.sketchInstances.ehuniv = ExpoInitUniv(sc.EH_univ_config.K, sc.EH_univ_config.Time_window_size)
		}

		if stype == SHCount && s.sketchInstances.shc == nil {
			s.sketchInstances.shc = SmoothInitCount(sc.SH_count_config.Beta, sc.SH_count_config.Time_window_size)
		}

		if stype == EHKLL && s.sketchInstances.ehkll == nil {
			s.sketchInstances.ehkll = ExpoInitKLL(sc.EH_kll_config.K, sc.EH_kll_config.Kll_k, sc.EH_kll_config.Time_window_size)
		}
	*/
	return nil
}

func NewSketchSeries(stripeSize int) *sketchSeries {
	ss := &sketchSeries{ // TODO: use stripeSeries toreduce lock contention later
		size:   stripeSize,
		id:     0,
		hashes: make([]sketchSeriesHashMap, stripeSize),
		series: make([]map[TSId]*memSeries, stripeSize),
		locks:  make([]stripeLock, stripeSize),
	}

	for i := range ss.series {
		ss.series[i] = map[TSId]*memSeries{}
	}
	for i := range ss.hashes {
		ss.hashes[i] = sketchSeriesHashMap{
			unique:    map[uint64]*memSeries{},
			conflicts: nil,
		}
	}
	return ss
}

func NewPromSketches() *PromSketches {
	ss := NewSketchSeries(5)
	ps := &PromSketches{
		series: ss,
	}
	ps.lastSeriesID.Store(0)
	ps.numSeries.Store(0)
	return ps
}

func NewPromSketchesWithConfig(sketchRuleTests []SketchRuleTest, sc *SketchConfig) *PromSketches {
	ss := NewSketchSeries(5)
	ps := &PromSketches{
		series: ss,
	}
	ps.lastSeriesID.Store(0)
	ps.numSeries.Store(0)
	for _, t := range sketchRuleTests {
		series, _, _ := ps.getOrCreate(t.lset.Hash(), t.lset)
		for stype, exist := range t.stypemap {
			if exist {
				newSketchInstance(series, stype, sc)
			}
		}
	}
	return ps
}

func newMemSeries(lset labels.Labels, id TSId) *memSeries {
	s := &memSeries{
		lset:            lset,
		id:              id,
		sketchInstances: nil,
		memoryPart:      goconcurrentqueue.NewFixedFIFO(500),
	}
	return s
}

func newSketchInstance(series *memSeries, stype SketchType, sc *SketchConfig) error {
	return newSlidingHistorgrams(series, stype, sc)
}

func (ps *PromSketches) NewSketchCacheInstance(lset labels.Labels, funcName string, time_window_size int64, item_window_size int64, value_scale float64) error {
	series, _, _ := ps.getOrCreate(lset.Hash(), lset)
	stypes := funcSketchMap[funcName]
	sc := SketchConfig{
		CS_config:   CSConfig{Row_no: 3, Col_no: 10000},
		Univ_config: UnivConfig{TopK_size: 5, Row_no: 3, Col_no: 10000, Layer: 16},
	}

	for _, stype := range stypes {
		switch stype {
		case SHUniv:
			sc.SH_univ_config = SHUnivConfig{Beta: 0.06, Time_window_size: time_window_size}
		case EHCount:
			sc.EH_count_config = EHCountConfig{K: 100, Time_window_size: time_window_size}
		case EffSum:
			sc.EffSum_config = EffSumConfig{Time_window_size: time_window_size, Item_window_size: item_window_size, Epsilon: 0.01, R: value_scale}
		case EffSum2:
			sc.EffSum2_config = EffSum2Config{Time_window_size: time_window_size, Item_window_size: item_window_size, Epsilon: 0.01, R: value_scale}
		case EHDD:
			sc.EH_dd_config = EHDDConfig{K: 100, Time_window_size: time_window_size, DDAccuracy: 0.01}
		default:
			fmt.Println("[NewSketchCacheInstance] not supported sketch type")
		}
		err := newSketchInstance(series, stype, &sc)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ps *PromSketches) LookUp(lset labels.Labels, funcName string, mint, maxt int64) bool {
	series := ps.series.getByHash(lset.Hash(), lset)
	if series == nil {
		// fmt.Println("[lookup] no timeseries")
		return false
	}
	stypes := funcSketchMap[funcName]
	for _, stype := range stypes {
		switch stype {
		case EHCount:
			if series.sketchInstances.ehc == nil {
				// fmt.Println("[lookup] ehc no sketch instance")
				return false
			} else if series.sketchInstances.ehc.Cover(mint, maxt) == false {
				// fmt.Println("[lookup] ehc not covered")
				return false
			}
		case SHUniv:
			if series.sketchInstances.shuniv == nil {
				// fmt.Println("[lookup] no sketch instance")
				return false
			} else if series.sketchInstances.shuniv.Cover(mint, maxt) == false {
				// fmt.Println("[lookup] not covered")
				return false
			}
		case EffSum:
			if series.sketchInstances.EffSum == nil {
				// fmt.Println("[lookup] effsum no sketch instance")
				return false
			} else if series.sketchInstances.EffSum.Cover(mint, maxt) == false {
				// fmt.Println("[lookup] effsum not covered")
				return false
			}
		case EffSum2:
			if series.sketchInstances.EffSum2 == nil {
				// fmt.Println("[lookup] no sketch instance")
				return false
			} else if series.sketchInstances.EffSum2.Cover(mint, maxt) == false {
				// fmt.Println("[lookup] not covered")
				return false
			}
		case EHDD:
			if series.sketchInstances.ehdd == nil {
				// fmt.Println("[lookup] no sketch instance")
				return false
			} else if series.sketchInstances.ehdd.Cover(mint, maxt) == false {
				// fmt.Println("[lookup] not covered")
				return false
			}
		default:
			return false
		}
	}
	return true
}

func (ps *PromSketches) Eval(funcName string, lset labels.Labels, otherArgs float64, mint, maxt int64) (Vector, annotations.Annotations) {
	sfunc := FunctionCalls[funcName]
	series := ps.series.getByHash(lset.Hash(), lset)

	// Clean memory array and transfer insertion time to evaluation time for SHUniv
	for series.memoryPart.GetLen() > 0 {
		item, err := series.memoryPart.Dequeue()
		if err != nil {
			fmt.Println("memory queue dequeue error")
			break
		}
		series.sketchInstances.shuniv.Update(item.(Sample).T, strconv.FormatFloat(item.(Sample).F, 'f', -1, 64))
		if item.(Sample).T >= maxt {
			break
		}
	}

	return sfunc(context.TODO(), series, otherArgs, mint, maxt), nil
}

func (s *memSeries) MemPartAppend(t int64, val float64) {
	s.memoryPart.Enqueue(Sample{T: t, F: val})
}

func (ps *PromSketches) getOrCreate(hash uint64, lset labels.Labels) (*memSeries, bool, error) {
	s := ps.series.getByHash(hash, lset)
	if s != nil {
		return s, false, nil
	}

	ps.series.id = ps.series.id + 1
	id := ps.series.id

	series := newMemSeries(lset, id)

	i := hash & uint64(ps.series.size-1)
	ps.series.locks[i].Lock()
	ps.series.hashes[i].set(hash, series)
	ps.series.locks[i].Unlock()

	i = uint64(id) & uint64(ps.series.size-1)
	ps.series.locks[i].Lock()
	ps.series.series[i][id] = series
	ps.series.locks[i].Unlock()

	return series, true, nil
}

// SketchInsertInsertionThroughputTest will be called in Prometheus scrape module, only for worst-case insertion throughput test
// t.(int64) is millisecond level timestamp, based on Prometheus timestamp
func (ps *PromSketches) SketchInsertInsertionThroughputTest(lset labels.Labels, t int64, val float64) error {
	t_1 := time.Now()
	s, iscreate, _ := ps.getOrCreate(lset.Hash(), lset)
	if s == nil {
		return errors.New("memSeries not found")
	}
	since_1 := time.Since(t_1)

	// this is for test worst case data insertion throughput
	if iscreate {
		// For insertion throughput test, we enable all sketches for one timeseries
		// otherwise, we can create new sketch instance upon queried as a cache, in addition to pre-defined sketch rules

		fmt.Println("getOrCreate=", since_1)
		t_now := time.Now()
		sketch_types := []SketchType{SHUniv, EHCount, EHDD, EffSum}
		sketch_config := SketchConfig{
			CS_config:       CSConfig{Row_no: 3, Col_no: 4096},
			Univ_config:     UnivConfig{TopK_size: 5, Row_no: 3, Col_no: 4096, Layer: 16},
			SH_univ_config:  SHUnivConfig{Beta: 0.1, Time_window_size: 10000},
			EH_count_config: EHCountConfig{K: 100, Time_window_size: 100000},
			EffSum_config:   EffSumConfig{Time_window_size: 100000, Item_window_size: 100000, Epsilon: 0.01, R: 10000},
			EH_dd_config:    EHDDConfig{K: 100, DDAccuracy: 0.01, Time_window_size: 100000},
		}
		for _, sketch_type := range sketch_types {
			newSketchInstance(s, sketch_type, &sketch_config)
		}
		since := time.Since(t_now)
		fmt.Println("new sketch instance=", since)
	}

	t_2 := time.Now()
	if s.sketchInstances.EffSum != nil {
		s.sketchInstances.EffSum.Insert(t, val)
	}
	if s.sketchInstances.EffSum2 != nil {
		s.sketchInstances.EffSum2.Insert(t, val)
	}
	since_2 := time.Since(t_2)
	fmt.Println("efficientsum=", (s.sketchInstances.EffSum == nil), since_2)

	t_3 := time.Now()
	if s.sketchInstances.ehc != nil {
		s.sketchInstances.ehc.Update(t, val)
	}
	since_3 := time.Since(t_3)
	fmt.Println("ehc=", (s.sketchInstances.ehc == nil), since_3)

	t_4 := time.Now()
	if s.sketchInstances.shuniv != nil {
		s.MemPartAppend(t, val)
	}
	since_4 := time.Since(t_4)
	fmt.Println("shuniv=", (s.sketchInstances.shuniv == nil), since_4)

	t_7 := time.Now()
	if s.sketchInstances.ehdd != nil {
		s.sketchInstances.ehdd.Update(t, val)
	}
	since_7 := time.Since(t_7)
	fmt.Println("ehdd=", (s.sketchInstances.ehdd == nil), since_7)

	return nil
}

// SketchInsertDefinedRules will be called in Prometheus scrape module, with pre-defined sketch rules (hard-coded)
// t.(int64) is millisecond level timestamp, based on Prometheus timestamp
func (ps *PromSketches) SketchInsertDefinedRules(lset labels.Labels, t int64, val float64) error {
	// t_1 := time.Now()
	s, iscreate, _ := ps.getOrCreate(lset.Hash(), lset)
	//since_1 := time.Since(t_1)
	if iscreate {
		//	fmt.Println("getOrCreate=", since_1)
	}

	if s == nil {
		return errors.New("memSeries not found")
	}

	// t_2 := time.Now()
	if s.sketchInstances.EffSum != nil {
		s.sketchInstances.EffSum.Insert(t, val)
	}

	if s.sketchInstances.EffSum2 != nil {
		s.sketchInstances.EffSum2.Insert(t, val)
	}
	//	since_2 := time.Since(t_2)
	//	fmt.Println("effsum=", (s.sketchInstances.EffSum == nil), since_2)

	//	t_3 := time.Now()
	if s.sketchInstances.ehc != nil {
		s.sketchInstances.ehc.Update(t, val)
	}
	//	since_3 := time.Since(t_3)
	//	fmt.Println("ehc=", (s.sketchInstances.ehc == nil), since_3)

	//	t_4 := time.Now()
	if s.sketchInstances.shuniv != nil {
		s.MemPartAppend(t, val)
		// s.sketchInstances.shuniv.Update(t, strconv.FormatFloat(val, 'f', -1, 64))
	}
	//	since_4 := time.Since(t_4)
	//	fmt.Println("shuniv=", (s.sketchInstances.shuniv == nil), since_4)

	//	t_7 := time.Now()
	if s.sketchInstances.ehdd != nil {
		s.sketchInstances.ehdd.Update(t, val)
	}
	//	since_7 := time.Since(t_7)
	//	fmt.Println("ehdd=", (s.sketchInstances.ehdd == nil), since_7)

	return nil
}

// SketchInsert will be called in Prometheus scrape module, for SketchCache version
// t.(int64) is millisecond level timestamp, based on Prometheus timestamp
func (ps *PromSketches) SketchInsert(lset labels.Labels, t int64, val float64) error {
	s := ps.series.getByHash(lset.Hash(), lset)
	if s == nil {
		return nil
	}

	if s.sketchInstances.ehc != nil {
		s.sketchInstances.ehc.Update(t, val)
	}

	if s.sketchInstances.shuniv != nil {
		// s.MemPartAppend(t, val)
		s.sketchInstances.shuniv.Update(t, strconv.FormatFloat(val, 'f', -1, 64))
		/*
			go func() {
				for s.memoryPart.GetLen() >= 500 { // s.memoryPart should be a FIFO queue and concurrency safe; TODO: find a efficient impl
					for i := 0; i < 500; i++ {
						item, err := s.memoryPart.Dequeue()
						if err != nil {
							fmt.Println("memory queue dequeue error")
							break
						}
						s.sketchInstances.shuniv.Update(item.(Sample).T, strconv.FormatFloat(item.(Sample).F, 'f', -1, 64))
					}
				}
			}()
		*/
	}

	if s.sketchInstances.ehdd != nil {
		s.sketchInstances.ehdd.Update(t, val)
	}

	if s.sketchInstances.EffSum != nil {
		s.sketchInstances.EffSum.Insert(t, val)
	}

	if s.sketchInstances.EffSum2 != nil {
		s.sketchInstances.EffSum2.Insert(t, val)
	}

	return nil
}

func (ps *PromSketches) BackgroundSketchInsert() {
	var wg sync.WaitGroup
	for id := 0; id < int(ps.series.id); id++ {
		series := ps.series.getByID(TSId(id))
		if series == nil {
			continue
		}
		wg.Add(1)
		go func() {
			for series.memoryPart.GetLen() > 0 { // s.memoryPart should be a FIFO queue and concurrency safe; TODO: find a efficient impl
				item, err := series.memoryPart.Dequeue()
				if err != nil {
					fmt.Println("memory queue dequeue error")
					break
				}
				series.sketchInstances.shuniv.Update(item.(Sample).T, strconv.FormatFloat(item.(Sample).F, 'f', -1, 64))
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
