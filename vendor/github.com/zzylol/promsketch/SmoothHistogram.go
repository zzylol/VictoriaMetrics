package promsketch

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync/atomic"

	// "sort"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/cespare/xxhash/v2"
	// "github.com/zzylol/prometheus-sketch-VLDB/prometheus-sketches/promql"
)

type HeavyHitter struct {
	hhs []Item
}

type HHCountSketch struct {
	cs   CountSketch
	topK *TopKHeap
}

type SmoothHistogramCS struct {
	seed1            []uint32
	seed2            []uint32
	s_count          int // sketch count
	cs_instances     []CountSketch
	beta             float64
	time_window_size int64
}

type SmoothHistogramCM struct {
	seed1            []uint32
	s_count          int // sketch count
	cm_instances     []CountMinSketch
	beta             float64
	time_window_size int64
}

type SmoothHistogramCount struct {
	s_count          int
	buckets          []CountBucket
	beta             float64
	time_window_size int64
}

/*
type SmoothHistogramHH struct {
	seed1 []uint32
	seed2 []uint32
	update_count_freq int // how many data points per Counting check
	cur_time int
	s_count int
	h_count int
	instances []*HHCountSketch
	shc *SmoothHistogramCount
	beta float64
	epsilon float64
	time_window_size int64
}
*/

type SmoothHistogramUnivMon struct {
	cs_seed1         []uint32
	cs_seed2         []uint32
	seed3            uint32       // univmon seed
	s_count          int          // sketch count
	univs            []UnivSketch // each bucket is a univsketch
	beta             float64
	time_window_size int64
	univPool         UnivSketchPool
	putch            chan int64
	done             chan struct{}
}

/*-----------------------------------------------------
			Smooth Histogram for UnivMon
-------------------------------------------------------*/

func SmoothInitUnivMon(beta float64, time_window_size int64) (shu *SmoothHistogramUnivMon) {
	shu = &SmoothHistogramUnivMon{
		s_count:          0,
		beta:             beta,
		time_window_size: time_window_size,
	}

	shu.putch = make(chan int64, 100)
	shu.done = make(chan struct{}, 100)

	shu.cs_seed1 = make([]uint32, CS_ROW_NO_Univ)
	shu.cs_seed2 = make([]uint32, CS_ROW_NO_Univ)
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < CS_ROW_NO_Univ; i++ {
		shu.cs_seed1[i] = rand.Uint32()
		shu.cs_seed2[i] = rand.Uint32()
	}
	shu.seed3 = rand.Uint32()

	shu.univs = make([]UnivSketch, shu.s_count)

	shu.univPool = UnivSketchPool{pool: make([]UnivSketch, UnivPoolCAP), size: 0, max_size: UnivPoolCAP}
	for i := uint32(0); i < UnivPoolCAP; i++ {
		shu.univPool.pool[i], _ = NewUnivSketch(TOPK_SIZE, CS_ROW_NO_Univ, CS_COL_NO_Univ, CS_LVLS, shu.cs_seed1, shu.cs_seed2, shu.seed3, int64(i)) // same parameters, mergability
	}
	shu.univPool.bm = roaring.New()

	for i := 0; i < 2; i++ {
		go func() {
			for {
				pool_idx := <-shu.putch
				shu.putUnivSketch(pool_idx)
				shu.done <- struct{}{}
			}
		}()

		go func() {
			for {
				<-shu.done
			}
		}()
	}

	return shu
}

func (shu *SmoothHistogramUnivMon) GetMemory() float64 {
	var total_mem float64 = 0
	for i := 0; i < shu.s_count; i++ {
		total_mem += shu.univs[i].GetMemoryKB()
		// fmt.Println(shu.univs[i].GetMemoryKB())
	}
	return total_mem
}

func (shu *SmoothHistogramUnivMon) GetUnivSketch() (UnivSketch, error) {
	/*
		if shu.univPool.size == shu.univPool.max_size && shu.univPool.toclean > 0 { // shu has too much put to clean
			<-shu.done
		}
	*/
	if shu.univPool.size == shu.univPool.max_size {
		tmp, err := NewUnivSketch(TOPK_SIZE, CS_ROW_NO_Univ, CS_COL_NO_Univ, CS_LVLS, shu.cs_seed1, shu.cs_seed2, shu.seed3, int64(shu.univPool.max_size)) // New && init UnivMon
		if err != nil {
			return UnivSketch{}, errors.New("univ sketch allocation failed")
		}
		shu.univPool.mutex.Lock()
		shu.univPool.pool = append(shu.univPool.pool, tmp)
		shu.univPool.bm.Add(shu.univPool.max_size)
		shu.univPool.mutex.Unlock()
		atomic.AddUint32(&shu.univPool.max_size, 1)
		atomic.AddUint32(&shu.univPool.size, 1)
		return tmp, nil
	}

	shu.univPool.mutex.Lock()
	iter := shu.univPool.bm.Iterator()

	idx := uint32(0)
	last := uint32(0)
	last = last - 1
	flag := false
	for iter.HasNext() {
		item := iter.Next()
		if item > 0 && last != item-1 {
			idx = item - 1
			flag = true
			break
		}
		last = item
	}

	if !flag {
		idx = last + 1
	}

	// fmt.Println(idx)

	univ := shu.univPool.pool[idx]
	shu.univPool.bm.Add(idx)
	shu.univPool.mutex.Unlock()
	atomic.AddUint32(&shu.univPool.size, 1)
	return univ, nil
}

func (shu *SmoothHistogramUnivMon) putUnivSketch(pool_idx int64) {
	shu.univPool.pool[pool_idx].Free()
	shu.univPool.mutex.Lock()
	shu.univPool.bm.Remove(uint32(pool_idx))
	shu.univPool.mutex.Unlock()
	atomic.AddUint32(&shu.univPool.size, ^uint32(0))
	atomic.AddUint32(&shu.univPool.toclean, ^uint32(0))
}

func (shu *SmoothHistogramUnivMon) PutUnivSketch(u UnivSketch) error {
	if u.pool_idx == -1 {
		return nil
	}
	// t_now := time.Now()
	// fmt.Println("put ", u.pool_idx)
	atomic.AddUint32(&shu.univPool.toclean, 1)
	shu.putch <- u.pool_idx
	// since := time.Since(t_now)
	// fmt.Println("put time=", since)
	return nil
}

func (shu *SmoothHistogramUnivMon) Update(time_ int64, value string) {
	// t_now := time.Now()
	// var wg sync.WaitGroup

	// init new UnivMon
	// t_now = time.Now()
	tmp, err := shu.GetUnivSketch()
	// since = time.Since(t_now)
	// fmt.Println("shuniv get time=", since)
	if err != nil {
		fmt.Println("[Error] failed to allocate memory for new bucket of UnivMon...")
	}
	tmp.max_time, tmp.min_time = time_, time_
	shu.univs = append(shu.univs, tmp)
	// optimization: calculate hashes for the same key among SH buckets because of mergability
	hash := xxhash.Sum64String(value)
	bottom_layer_num := findBottomLayerNum(hash, CS_LVLS)
	pos := make([][]int32, 0)
	sign := make([][]int32, 0)
	for l := 0; l <= bottom_layer_num; l++ {
		p, si := shu.univs[0].cs_layers[l].position_and_sign([]byte(value))
		pos = append(pos, p)
		sign = append(sign, si)
	}
	shu.univs[shu.s_count].univmon_processing(value, 1, bottom_layer_num, pos, sign)
	shu.s_count++

	for i := 0; i < shu.s_count; i++ {
		// start := time.Now()
		// wg.Add(1)
		// go func(value string, i int) {
		shu.univs[i].univmon_processing(value, 1, bottom_layer_num, pos, sign)
		shu.univs[i].max_time = time_ // time-based window
		// wg.Done()
		// }(value, i)
		// since := time.Since(start)
		// fmt.Println("univmon_processing time=", since)
	}
	// wg.Wait()
	// since := time.Since(t_now)
	// fmt.Println("shuniv univ update time=", since)

	// t_now = time.Now()
	// fmt.Println("s_count=", shu.s_count)
	var (
		total_s1 int64 = 0
		total_s2 int64 = 0
	)
	for i := 0; i <= shu.s_count-3; i++ {
		maxj := i + 1
		t1 := time.Now()
		var compare_value float64 = float64(1.0-0.5*shu.beta) * (shu.univs[i].cs_layers[0].cs_l2())

		for j := i + 2; j <= shu.s_count-3; j++ {
			if (maxj < j) && (float64(shu.univs[j].cs_layers[0].cs_l2()) >= compare_value) {
				maxj = j
			} else {
				break
			}
		}

		s1 := time.Since(t1)
		total_s1 += s1.Microseconds()
		// 	fmt.Println("cs_l2 time=", s1)

		t2 := time.Now()
		shift := maxj - i - 1 // offset to shift

		if shift > 0 { // need to shift
			shu.s_count = shu.s_count - shift
			for idx := i + 1; idx < maxj; idx++ {
				shu.PutUnivSketch(shu.univs[idx])
			}
			shu.univs = append(shu.univs[:i+1], shu.univs[maxj:]...)
		}
		s2 := time.Since(t2)
		total_s2 += s2.Microseconds()
		// fmt.Println("shift time=", s2)

	}

	// since = time.Since(t_now)
	// fmt.Println("remove time 1=", since)

	/*
		if shu.s_count > 3 {
			fmt.Println("avg s1 = (us)", float64(total_s1)/float64(shu.s_count-3))
			fmt.Println("avg s2 = (us)", float64(total_s2)/float64(shu.s_count-3))
		}
	*/

	// t_now = time.Now()

	removed := 0
	for i := 0; i < shu.s_count; i++ {
		if shu.univs[i].min_time+shu.time_window_size < time_ {
			removed++
		} else {
			break
		}
	}

	if removed > 0 {
		// fmt.Println("previous len(univs) =", len(shu.univs))
		shu.s_count = shu.s_count - removed
		for idx := 0; idx < removed; idx++ {
			shu.PutUnivSketch(shu.univs[idx])
		}
		shu.univs = shu.univs[removed:]
		// fmt.Println("removed =", removed, "len(univs) =", len(shu.univs))
	}

	// since = time.Since(t_now)
	// fmt.Println("remove time 2=", since)

	// shu.print_sh_univ_buckets()
	// fmt.Println(time_, shu.s_count)
}

/*
Merge the universal sketches to the interval of t1 to t2;
cur_t - time_window_size <= t1 <= t2 <= cur_t
*/
func (shu *SmoothHistogramUnivMon) QueryIntervalMergeUniv(t1, t2 int64) (univ UnivSketch, err error) {
	var diff1, diff2 int64 = math.MaxInt64, math.MaxInt64
	var from_bucket, to_bucket int = shu.s_count, shu.s_count

	for i := 0; i < shu.s_count; i++ {
		curdiff1 := AbsInt64(shu.univs[i].min_time - t1)
		curdiff2 := AbsInt64(shu.univs[i].min_time - t2)
		if curdiff1 < diff1 {
			diff1 = curdiff1
			from_bucket = i
		}
		if curdiff2 < diff2 {
			diff2 = curdiff2
			to_bucket = i
		}
	}

	// fmt.Println("s_count =", shu.s_count)
	// fmt.Println("from_bucket =", from_bucket)
	// fmt.Println("to_bucket =", to_bucket)

	if from_bucket == shu.s_count || to_bucket == shu.s_count { // TODO: handle the boundary
		// fmt.Println("[SmoothHistogram Error] failed to find buckets for queried interval.")
		return UnivSketch{}, errors.New("bucket not found")
	}

	merged_univ, err := NewUnivSketch(TOPK_SIZE, CS_ROW_NO_Univ, CS_COL_NO_Univ, CS_LVLS, shu.cs_seed1, shu.cs_seed2, shu.seed3, -1) // new && init; seed1 and seed2 should be the same as other UnivSketch
	if err != nil {
		fmt.Println("[Error] failed to allocate memory for new bucket of UnivMon...")
	}

	for i := 0; i < CS_LVLS; i++ {
		for j := 0; j < CS_ROW_NO_Univ; j++ {
			for k := 0; k < CS_COL_NO_Univ; k++ {
				merged_univ.cs_layers[i].count[j][k] = shu.univs[from_bucket].cs_layers[i].count[j][k] - shu.univs[to_bucket].cs_layers[i].count[j][k]
			}
		}

		topK := NewTopKFromHeap(shu.univs[from_bucket].HH_layers[i].topK) // deep copy of heap
		for _, item := range topK.heap {
			// fmt.Println("s_count new = ", shu.s_count)
			item.count = merged_univ.cs_layers[i].EstimateStringCount(item.key)
		}
		for _, item := range topK.heap {
			if item.count != 0 {
				merged_univ.HH_layers[i].topK.Update(item.key, item.count)
			}
		}
	}

	merged_univ.min_time = shu.univs[from_bucket].min_time
	merged_univ.max_time = shu.univs[to_bucket].min_time
	// fmt.Println("merged_univ.min_time =", merged_univ.min_time)
	// fmt.Println("merged_univ.max_time =", merged_univ.max_time)

	return merged_univ, nil
}

func (sh *SmoothHistogramUnivMon) Cover(mint, maxt int64) bool {
	if sh.s_count == 0 {
		return false
	}
	return (sh.univs[0].min_time <= mint) // && sh.univs[sh.s_count-1].max_time >= maxt)
}

func (shu *SmoothHistogramUnivMon) print_sh_univ_buckets() {
	fmt.Println("s_count =", shu.s_count)
	for i := 0; i < shu.s_count; i++ {
		fmt.Println("i =", i, "min_time =", shu.univs[i].min_time, "max_time = ", shu.univs[i].max_time)
	}
}

/*
----------------------------------------------------------

	Smooth Histogram for count, sum, and avg, sum2

----------------------------------------------------------
*/
func SmoothInitCS(beta float64, time_window_size int64) (sh *SmoothHistogramCS) {
	sh = &SmoothHistogramCS{
		s_count:          0,
		beta:             beta,
		time_window_size: time_window_size,
	}

	sh.seed1 = make([]uint32, CS_ROW_NO_Univ)
	sh.seed2 = make([]uint32, CS_ROW_NO_Univ)
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < CS_ROW_NO_Univ; i++ {
		sh.seed1[i] = rand.Uint32()
		sh.seed2[i] = rand.Uint32()
	}

	sh.cs_instances = make([]CountSketch, sh.s_count)

	return sh
}

func (sh *SmoothHistogramCS) Update(time int64, key string, value float64) {
	for i := 0; i < sh.s_count; i++ {
		sh.cs_instances[i].UpdateString(key, value)
		sh.cs_instances[i].max_time = time // time-based window
	}

	// init new CountSketch
	tmp, err := NewCountSketch(CS_ROW_NO, CS_COL_NO, sh.seed1, sh.seed2) // New && init CountSketch
	if err != nil {
		fmt.Println("[Error] failed to allocate memory for new bucket of CS...")
	}
	tmp.max_time, tmp.min_time = time, time
	sh.cs_instances = append(sh.cs_instances, tmp)
	sh.cs_instances[sh.s_count].UpdateString(key, value)
	sh.s_count++

	for i := 0; i <= sh.s_count-3; i++ {
		maxj := i + 1
		var compare_value float64 = float64(1.0-0.5*sh.beta) * (sh.cs_instances[i].cs_l2())

		for j := i + 1; j <= sh.s_count-3; j++ {
			// fmt.Println(compare_value, sh.cs_instances[j].cs_l2())
			if (maxj < j) && (float64(sh.cs_instances[j].cs_l2()) >= compare_value) {
				maxj = j
			} else {
				break
			}
		}

		shift := maxj - i - 1 // offset to shift
		// fmt.Println(shift)
		if shift > 0 { // need to shift
			sh.s_count = sh.s_count - shift
			sh.cs_instances = append(sh.cs_instances[:i+1], sh.cs_instances[maxj:]...)
		}

	}

	removed := 0
	for i := 0; i < sh.s_count; i++ {
		if sh.cs_instances[i].max_time-sh.cs_instances[i].min_time >= sh.time_window_size {
			removed++
		} else {
			break
		}
	}

	if removed > 0 {
		// fmt.Println("previous len(cs_instances) =", len(sh.cs_instances))
		sh.s_count = sh.s_count - removed
		sh.cs_instances = sh.cs_instances[removed:]
		// fmt.Println("removed =", removed, "len(cs_instances) =", len(sh.cs_instances))
	}

	// sh.print_sh_univ_buckets()
}

/*
Merge the universal sketches to the interval of t1 to t2;
cur_t - time_window_size <= t1 < = t2 <= cur_t
*/
func (sh *SmoothHistogramCS) QueryIntervalMergeCS(t1, t2 int64) CountSketch {
	var diff1, diff2 int64 = math.MaxInt64, math.MaxInt64
	var from_bucket, to_bucket int = sh.s_count, sh.s_count

	for i := 0; i < sh.s_count; i++ {
		curdiff1 := AbsInt64(sh.cs_instances[i].min_time - t1)
		curdiff2 := AbsInt64(sh.cs_instances[i].min_time - t2)
		if curdiff1 < diff1 {
			diff1 = curdiff1
			from_bucket = i
		}
		if curdiff2 < diff2 {
			diff2 = curdiff2
			to_bucket = i
		}
	}

	// fmt.Println("s_count =", sh.s_count)
	// fmt.Println("from_bucket =", from_bucket)
	// fmt.Println("to_bucket =", to_bucket)

	if from_bucket == sh.s_count || to_bucket == sh.s_count { // TODO: handle the boundary
		// fmt.Println("[SmoothHistogram Error] failed to find buckets for queried interval.")
		return CountSketch{}
	}

	merged_cs, err := NewCountSketch(CS_ROW_NO_Univ, CS_COL_NO_Univ, sh.seed1, sh.seed2) // new && init; seed1 and seed2 should be the same as other UnivSketch
	if err != nil {
		fmt.Println("[Error] failed to allocate memory for new bucket of CountSketch...")
	}

	for j := 0; j < CS_ROW_NO_Univ; j++ {
		for k := 0; k < CS_COL_NO_Univ; k++ {
			merged_cs.count[j][k] = sh.cs_instances[from_bucket].count[j][k] - sh.cs_instances[to_bucket].count[j][k]
		}
	}

	merged_cs.min_time = sh.cs_instances[from_bucket].min_time
	merged_cs.max_time = sh.cs_instances[to_bucket].min_time
	// fmt.Println("merged_univ.min_time =", merged_univ.min_time)
	// fmt.Println("merged_univ.max_time =", merged_univ.max_time)

	return merged_cs
}

func (sh *SmoothHistogramCS) Cover(mint, maxt int64) bool {
	// fmt.Println("s_count = ", sh.s_count)
	if sh.s_count == 0 {
		return false
	}
	// fmt.Println(sh.cs_instances[0].min_time, sh.cs_instances[sh.s_count-1])
	return (sh.cs_instances[0].min_time <= mint) // && sh.cs_instances[sh.s_count-1].max_time >= maxt)
}

func (sh *SmoothHistogramCS) print_sh_cs_buckets() {
	fmt.Println("s_count =", sh.s_count)
	for i := 0; i < sh.s_count; i++ {
		fmt.Println("i =", i, "min_time =", sh.cs_instances[i].min_time, "max_time = ", sh.cs_instances[i].max_time)
	}
}

/*----------------------------------------------------------
				Smooth Histogram for count, sum, and avg, sum2
----------------------------------------------------------*/

func SmoothInitCount(beta float64, time_window_size int64) (sh *SmoothHistogramCount) {
	sh = &SmoothHistogramCount{
		beta:             beta,
		s_count:          0,
		time_window_size: time_window_size,
	}

	sh.buckets = make([]CountBucket, sh.s_count)

	return sh
}

func (sh *SmoothHistogramCount) GetMemory() float64 {
	return 44 * float64(sh.s_count) / 1024 // KBytes
}

func (sh *SmoothHistogramCount) Update(time int64, value float64) {
	for i := 0; i < sh.s_count; i++ {
		sh.buckets[i].count += 1            // count
		sh.buckets[i].sum += value          // sum
		sh.buckets[i].sum2 += value * value // sum2
		sh.buckets[i].max_time = time
		sh.buckets[i].bucketsize += 1
	}

	tmp := CountBucket{
		count:      1,
		sum:        value,
		sum2:       value * value,
		max_time:   time,
		min_time:   time,
		bucketsize: 1,
	}
	sh.buckets = append(sh.buckets, tmp)
	sh.s_count++

	for i := 0; i < sh.s_count-2; i++ {
		maxj := i + 1
		var compare_value float64 = float64(1.0-0.5*sh.beta) * (sh.buckets[i].sum2)

		for j := i + 1; j < sh.s_count-2; j++ {
			if (maxj < j) && (float64(sh.buckets[j].sum2) >= compare_value) {
				maxj = j
			}
		}

		shift := maxj - i - 1
		if shift > 0 {
			sh.s_count = sh.s_count - shift
			sh.buckets = append(sh.buckets[:i+1], sh.buckets[maxj:]...)
		}
	}

	removed := 0
	for i := 0; i < sh.s_count; i++ {
		if sh.buckets[i].max_time-sh.buckets[i].min_time >= sh.time_window_size {
			removed++
		} else {
			break
		}
	}
	if removed > 0 {
		sh.s_count -= removed
		sh.buckets = sh.buckets[removed:]
	}

	// sh.print_sh_count()
}

func (sh *SmoothHistogramCount) Cover(mint, maxt int64) bool {
	if sh.s_count == 0 {
		return false
	}
	return (sh.buckets[0].min_time <= mint) // && sh.buckets[sh.s_count-1].max_time >= maxt)
}

func (sh *SmoothHistogramCount) print_sh_count() {
	fmt.Println("s_count =", sh.s_count)
	for i := 0; i < sh.s_count; i++ {
		fmt.Println("i =", i, "min_time =", sh.buckets[i].min_time, "max_time = ", sh.buckets[i].max_time)
	}
}

func (sh *SmoothHistogramCount) QueryIntervalCount(t1 int64) (CountBucket, error) {
	var diff int64 = math.MaxInt64
	var return_bucket int = 0
	for i := 0; i < sh.s_count; i++ {
		curdiff := AbsInt64(t1 - sh.buckets[i].min_time)
		if curdiff < diff {
			diff = curdiff
			return_bucket = i
		}
	}
	if sh.s_count > 0 {
		return sh.buckets[return_bucket], nil
	} else {
		return CountBucket{}, errors.New("bucket not found")
	}
}

func (sh *SmoothHistogramCount) QueryT1T2IntervalCount(t1, t2 int64) float64 {
	bucket_1, err1 := sh.QueryIntervalCount(t1)
	bucket_2, err2 := sh.QueryIntervalCount(t2)
	if err1 == nil && err2 == nil {
		return float64(bucket_2.count - bucket_1.count)
	} else {
		return 0
	}
}

func (sh *SmoothHistogramCount) QueryIntervalSum(t1 int64) float64 {
	var diff int64 = math.MaxInt64
	var return_bucket int = 0
	for i := 0; i < sh.s_count; i++ {
		curdiff := AbsInt64(t1 - sh.buckets[i].min_time)
		if curdiff < diff {
			diff = curdiff
			return_bucket = i
		}
	}

	return sh.buckets[return_bucket].sum
}

func (sh *SmoothHistogramCount) QueryT1T2IntervalSum(t1, t2 int64) float64 {
	return sh.QueryIntervalSum(t1) - sh.QueryIntervalSum(t2)
}

func (sh *SmoothHistogramCount) QueryIntervalSum2(t1 int64) float64 {
	var diff int64 = math.MaxInt64
	var return_bucket int = 0
	for i := 0; i < sh.s_count; i++ {
		curdiff := AbsInt64(t1 - sh.buckets[i].min_time)
		if curdiff < diff {
			diff = curdiff
			return_bucket = i
		}
	}

	return sh.buckets[return_bucket].sum2
}

func (sh *SmoothHistogramCount) QueryT1T2IntervalSum2(t1, t2 int64) float64 {
	return sh.QueryIntervalSum2(t1) - sh.QueryIntervalSum2(t2)
}

/*
func SmoothInitSum(beta float64, seed1 []uint32, time_window_size int64) (sh *SmoothHistogramCM) {
	sh = &SmoothHistogramCM{
		beta: beta,
		s_count: 0,
		time_window_size: time_window_size,
	}
	sh.seed1 = make([]uint32, CS_ROW_NO_Univ)
	copy(sh.seed1, seed1)

	sh.cm_instances = make([]CountMinSketch, sh.s_count)

	return sh
}

func (sh *SmoothHistogramCM) SmoothUpdateSum(key string, time int64, value float64) {
	for i := 0; i < sh.s_count; i++ {
		sh.cm_instances[i].CMProcessing(key, value) // for count, sum, sum2
		sh.cm_instances[i].max_time = time // TODO: the max_time for different time series are not consistent
	}

	tmp, err := NewCountMinSketch(CS_ROW_NO_Univ, CS_COL_NO_Univ, sh.seed1) // new && init count sketch
	if err != nil {
		fmt.Println("[Error] failed to allocate memory for countmin sketch...")
	}
	sh.cm_instances = append(sh.cm_instances, tmp)

	sh.cm_instances[sh.s_count].CMProcessing(key, value) // for count, sum, and sum2
	sh.cm_instances[sh.s_count].max_time, sh.cm_instances[sh.s_count].min_time = time, time
	sh.s_count++

	for i := 0; i < sh.s_count - 2; i++ {
		maxj := i + 1
		var compare_value float64 = float64(1.0 - 0.5 * sh.beta) * (sh.cm_instances[i].cm_l1())
		// TODO: sum, count, use l1() and use CountMin, analysis error
		// sum2 (square) use l2() of CountMin

		for j := i + 1; j < sh.s_count - 2; j++ {
			if (maxj < j) && (float64(sh.cm_instances[j].cm_l1()) >= compare_value) {
				maxj = j
			}
		}

		shift := maxj - i - 1
		if shift > 0 {
			sh.s_count = sh.s_count - shift
			sh.cm_instances = append(sh.cm_instances[:i+1], sh.cm_instances[maxj:]...)
		}
	}

	removed := 0
	for i := 0; i < sh.s_count; i++ {
		if sh.cm_instances[i].max_time - sh.cm_instances[i].min_time >= sh.time_window_size {
			removed ++
		} else {
			break
		}
	}
	if removed > 0 {
		sh.s_count -= removed
		sh.cm_instances = sh.cm_instances[removed:]
	}

	sh.print_sh_count()
}

func (sh * SmoothHistogramCM) print_sh_count() {
	fmt.Println("s_count =", sh.s_count)
	for i := 0; i < sh.s_count; i++ {
		fmt.Println("i =", i, "min_time =", sh.cm_instances[i].min_time, "max_time = ", sh.cm_instances[i].max_time)
	}
}

func (sh * SmoothHistogramCM) QueryIntervalCount(key string, t1 int64) float64 {
	var diff int64 = math.MaxInt64
	var return_bucket int = 0
	for i := 0; i < sh.s_count; i++ {
		curdiff := AbsInt64(t1 - sh.cm_instances[i].min_time)
		if curdiff < diff {
			diff = curdiff
			return_bucket = i
		}
	}

	fmt.Println("count return_bucket =", return_bucket)
	return sh.cm_instances[return_bucket].EstimateStringCount(key)
}

func (sh * SmoothHistogramCM) QueryT1T2IntervalCount(key string, t1, t2 int64) float64 {
	return sh.QueryIntervalCount(key, t1) - sh.QueryIntervalCount(key, t2)
}

func (sh * SmoothHistogramCM) QueryIntervalSum(key string, t1 int64) float64 {
	var diff int64 = math.MaxInt64
	var return_bucket int = 0
	for i := 0; i < sh.s_count; i++ {
		// fmt.Println(sh.cs_instances[i].min_time, sh.cs_instances[i].max_time)
		curdiff := AbsInt64(t1 - sh.cm_instances[i].min_time)
		if curdiff < diff {
			diff = curdiff
			return_bucket = i
		}
	}

	fmt.Println("sum return_bucket =", return_bucket)
	return sh.cm_instances[return_bucket].EstimateStringSum(key)
}

func (sh * SmoothHistogramCM) QueryT1T2IntervalSum(key string, t1, t2 int64) float64 {
	return sh.QueryIntervalSum(key, t1) - sh.QueryIntervalSum(key, t2)
}
*/

/*----------------------------------------------------------
				Smooth Histogram for sum2
----------------------------------------------------------*/
/*
func SmoothInitSum2(beta float64, seed1 []uint32, time_window_size int64) (sh *SmoothHistogramCM) {
	sh = &SmoothHistogramCM{
		beta: beta,
		s_count: 0,
		time_window_size: time_window_size,
	}
	sh.seed1 = make([]uint32, CS_ROW_NO_Univ)
	copy(sh.seed1, seed1)

	sh.cm_instances = make([]CountMinSketch, sh.s_count)

	return sh
}

func (sh *SmoothHistogramCM) SmoothUpdateSum2(key string, time int64, value float64) {
	for i := 0; i < sh.s_count; i++ {
		sh.cm_instances[i].CMProcessing(key, value) // for count, sum, sum2
		sh.cm_instances[i].max_time = time
	}

	tmp, err := NewCountMinSketch(CS_ROW_NO_Univ, CS_COL_NO_Univ, sh.seed1) // new && init count sketch
	if err != nil {
		fmt.Println("[Error] failed to allocate memory for countmin sketch...")
	}
	sh.cm_instances = append(sh.cm_instances, tmp)

	sh.cm_instances[sh.s_count].CMProcessing(key, value) // for count, sum, and sum2
	sh.cm_instances[sh.s_count].max_time, sh.cm_instances[sh.s_count].min_time = time, time
	sh.s_count++

	for i := 0; i < sh.s_count - 2; i++ {
		maxj := i + 1
		var compare_value float64 = float64(1.0 - 0.5 * sh.beta) * (sh.cm_instances[i].cm_l2())
		// TODO: sum, count, and square, use l1() and use CountMin, analysis error

		for j := i + 1; j < sh.s_count - 2; j++ {
			if (maxj < j) && (float64(sh.cm_instances[j].cm_l2()) >= compare_value) {
				maxj = j
			}
		}

		shift := maxj - i - 1
		if shift > 0 {
			sh.s_count = sh.s_count - shift
			sh.cm_instances = append(sh.cm_instances[:i+1], sh.cm_instances[maxj:]...)
		}
	}

	removed := 0
	for i := 0; i < sh.s_count; i++ {
		if sh.cm_instances[i].max_time - sh.cm_instances[i].min_time >= sh.time_window_size {
			removed ++
		} else {
			break
		}
	}
	if removed > 0 {
		sh.s_count -= removed
		sh.cm_instances = sh.cm_instances[removed:]
	}
}

func (sh * SmoothHistogramCM) QueryIntervalSum2(key string, t1 int64) float64 {
	var diff int64 = math.MaxInt64
	var return_bucket int = 0
	for i := 0; i < sh.s_count; i++ {
		// fmt.Println(sh.cs_instances[i].min_time, sh.cs_instances[i].max_time)
		curdiff := AbsInt64(t1 - sh.cm_instances[i].min_time)
		if curdiff < diff {
			diff = curdiff
			return_bucket = i
		}
	}

	return sh.cm_instances[return_bucket].EstimateStringSum2(key)
}

func (sh * SmoothHistogramCM) QueryT1T2IntervalSum2(key string, t1, t2 int64) float64 {
	return sh.QueryIntervalSum2(key, t1) - sh.QueryIntervalSum2(key, t2)
}
*/

/*------------------------------------------------------------------------------
			Smooth Histogram for Heavy Hitters
--------------------------------------------------------------------------------*/
/*
func (shh *SmoothHistogramHH) smooth_init_hh(beta float64, epsilon float64, seed1, seed2 []uint32, update_count_freq int, time_window_size int64) {
	shh.beta = beta
	shh.epsilon = epsilon

	shh.s_count = 0
	shh.h_count = 0
	shh.update_count_freq = update_count_freq
	shh.time_window_size = time_window_size

	shh.seed1 = make([]uint32, CS_ROW_NO_Univ)
	shh.seed2 = make([]uint32, CS_ROW_NO_Univ)
	copy(shh.seed1, seed1)
	copy(shh.seed2, seed2)

	shh.instances = make([]*HHCountSketch, shh.s_count)
	shh.shc = smooth_init_count(nil, 0, shh.beta)

	fmt.Println("hh smooth historgram init is done.")
}

func (shh *SmoothHistogramHH) smooth_update_hh(key string) {
	for i := 0; i < shh.s_count; i++ {
		median_count := shh.instances[i].cs.UpdateAndEstimateString(key, 1)
		shh.instances[i].topK.Update(key, median_count)
	}

	// init new CountSketch
	tmp, err := NewCountSketch(CS_ROW_NO_Univ, CS_COL_NO_Univ, shh.seed1, shh.seed2) // new && init a count sketch
	if err != nil {
		fmt.Println("[Error] failed to allocate memory for new count Sketch bucket...")
	}
	tmp_topk := NewTopKHeap(TOPK_SIZE)
	shh.instances = append(shh.instances, &HHCountSketch{tmp, tmp_topk})
	median_count := shh.instances[shh.s_count].cs.UpdateAndEstimateString(key, 1)
	shh.instances[shh.s_count].topK.Update(key, median_count)
	shh.s_count++

	for i := 0; i <= shh.s_count - 3; i++ {
		maxj := i + 1
		var compare_value float64 = float64(1.0 - 0.5 * shh.beta) * (shh.instances[i].cs.cs_l2())
		for j := i + 1; j <= shh.s_count - 3; j++ {
			if (maxj < j) && (float64(shh.instances[j].cs.cs_l2()) >= compare_value) {
				maxj = j
			}
		}

		shift := maxj - i - 1 // offsert to shift
		if shift > 0 { // need to shift
			// for j := i+1; j < maxj; j++ {
				// clean the memory for the CS heap -- auto-managed by garbage collector
			// }

			shh.s_count -= shift
			shh.instances = shh.instances[shift:]
		}
	}

	removed := 0
	for i := 0; i < shh.s_count; i++ {
		if shh.instances[i].cs.max_time - shh.instances[i].cs.min_time >= shh.time_window_size {
			removed ++
		} else {
			break
		}
	}

	if removed > 0 {
		// for j := 0; j < removed; j++ {
			// deleteMinHeap(&shh->CSInstances[j].topK); // clean the memory for the CS heap
		// }
		shh.s_count -= removed
		shh.instances = shh.instances[removed: ]
	}

	if shh.cur_time % shh.update_count_freq == 0 {
		threshold := shh.epsilon * shh.epsilon * 3.0 / 4.0
		for i := 0; i < shh.s_count; i++ {
			for _, item := range shh.instances[i].topK.heap {
				if item.count > int64(threshold * shh.instances[i].cs.cs_l2()){
					shh.shc.smooth_insert_count(item.key)
				}
			}
		}
	}

}
*/

/*--------------------------------------------------------------------
				Smooth Histogram for Counting
--------------------------------------------------------------------*/

/*
func smooth_init_count(given_keys []string, given_key_size int, beta float64) (shc *SmoothHistogramCount) {
	shc = &SmoothHistogramCount{
		beta: beta,
		excluded_zero: 0,
		given_key_size: given_key_size,
	}

	shc.keys = make(map[string]int)

	if given_keys != nil && given_key_size != 0 {
		for i := 0; i < given_key_size; i++ {
			shc.keys[given_keys[i]] = i
		}
	}

	shc.stored_keys = make([]string, given_key_size)
	shc.s_count = make([]int, given_key_size)
	shc.zero_count = make([]int, given_key_size)
	shc.buckets = make([][]CountBucket, given_key_size)

	for i := 0; i < given_key_size; i++ {
		shc.stored_keys[i] = given_keys[i]
		shc.s_count[i] = 0
		shc.zero_count[i] = 0
		shc.buckets[i] = make([]CountBucket, shc.s_count[i])
	}

	fmt.Println("Init the Smooth Histogram for Counting is done.")

	return shc
}

func (shc *SmoothHistogramCount) smooth_insert_count(insert_key string) int {

	_, ok := shc.keys[insert_key]

	if !ok {
		if shc.excluded_zero > 0 {
			for i := 0; i < shc.given_key_size; i++ {
				shc.zero_count[i] += shc.excluded_zero
			}
			shc.excluded_zero = 0
		}
		shc.given_key_size++
		shc.keys[insert_key] = shc.given_key_size - 1

		shc.stored_keys = append(shc.stored_keys, insert_key)
		shc.s_count = append(shc.s_count, 0)
		shc.zero_count = append(shc.zero_count, 0)
		tmp_countbucket := make([]CountBucket, shc.s_count[shc.given_key_size-1])
		shc.buckets = append(shc.buckets, tmp_countbucket)
	} else {
		fmt.Println("[Warning] The key to insert is already tracked in the SmoothHistogram.")
		return -1
	}

	return 1
}

func (shc *SmoothHistogramCount) smooth_delete_count(delete_key string) {

	found, ok := shc.keys[delete_key]

	if ok {
		index := found
		shc.given_key_size--
		shc.stored_keys = append(shc.stored_keys[:index], shc.stored_keys[index + 1:]...)
		shc.s_count = append(shc.s_count[:index], shc.s_count[index + 1:]...)
		shc.zero_count = append(shc.zero_count[:index], shc.zero_count[index + 1:]...)
		shc.buckets = append(shc.buckets[:index], shc.buckets[index + 1:]...)
	} else {
		fmt.Println("[Warning] the key to delete is not found in the SmoothHistogram...")
	}
}

func (shc *SmoothHistogramCount) smooth_update_count(key string) {

	found, ok := shc.keys[key]

	if ok {
		index := found
		for i := 0; i < shc.given_key_size; i++ {
			shc.zero_count[i] += shc.excluded_zero + 1
		}
		shc.excluded_zero = 0

		for i := 0; i < shc.s_count[index]; i++ {
			shc.buckets[index][i].counter++
			shc.buckets[index][i].bucketsize += shc.zero_count[index]
		}
		shc.zero_count[index] = 0 // reset zero counter after bucket updates
		tmp_countbucket := CountBucket{
			bucketsize : 1,
			counter : 1,
		}
		shc.buckets[index] = append(shc.buckets[index], tmp_countbucket)
		shc.s_count[index]++

		for i := 0; i < shc.s_count[index] - 2; i++ {
			maxj := i + 1
			var compare_value float64 = (1.0 - 0.5 * shc.beta) * (float64(shc.buckets[index][i].counter))
			for j := i+1; j < shc.s_count[index] - 2; j++ {
				if (maxj < j) && (float64(shc.buckets[index][j].counter) >= compare_value) {
					maxj = j
				}
			}
			shift := maxj - i - 1
			if shift > 0 {
				shc.buckets[index] = append(shc.buckets[index][:i+1], shc.buckets[index][maxj:]...)
				shc.s_count[index] = shc.s_count[index] - shift
			}
		}

		// below is size-based sliding window
		removed := 0
		for i := 0; i < shc.s_count[index]; i++ {
			if shc.buckets[index][i].bucketsize > WINDOW_SIZE + 1 {
				removed ++
			} else {
				break
			}
		}


		// removed := 0
		// for i := 0; i < shc.s_count[index]; i++ {
		//	if shc.buckets[index][i].max_time - shc.buckets[index][i].min_time > shc.time_window_size  {
		//		removed ++
		//	} else {
		//		break
		//	}
		//}


		if removed > 0 {
			shc.s_count[index] = shc.s_count[index] - removed
			shc.buckets[index] = shc.buckets[index][removed:]
		}
	} else {
		shc.excluded_zero++
	}
}

func (shc *SmoothHistogramCount) print_buckets_count() {
	for i := 0; i < shc.given_key_size; i++ {
		for j := 0; j < shc.s_count[i]; j++ {
			fmt.Print("item#%d, bucket %d, coutner %d, bucketsize %d\n", i, j, shc.buckets[i][j].counter, shc.buckets[i][j].bucketsize)
		}
	}
}
*/

/*----------------------------------------------------------
				Interval Query functions
----------------------------------------------------------*/

/*
Query the L2 on the interval of t1 to current time;
l2_over_time
*/
/*
func query_interval_l2(sh *SmoothHistogramCS, t1 int64) float64 {
	var diff int64 = math.MaxInt64
	var return_bucket int = 0
	for i := 0; i < sh.s_count; i++ {
		curdiff := AbsInt64(t1 - sh.cs_instances[i].min_time)
		if curdiff < diff {
			diff = curdiff
			return_bucket = i
		}
	}
	return sh.cs_instances[return_bucket].cs_l2()
}
*/

/*
Query the sum of a given key in the interval t1 to current time.
sum_over_time, avg_over_time
*/
/*
func (sh * SmoothHistogramCS) query_interval_sum(key string, t1 int64) float64 {
	var diff int64 = math.MaxInt64
	var return_bucket int = 0
	for i := 0; i < sh.s_count; i++ {
		// fmt.Println(sh.cs_instances[i].min_time, sh.cs_instances[i].max_time)
		curdiff := AbsInt64(t1 - sh.cs_instances[i].min_time)
		if curdiff < diff {
			diff = curdiff
			return_bucket = i
		}
	}

	return sh.cs_instances[return_bucket].EstimateStringSum(key)
}
*/

/*
Query the count of items of a given key in the interval t1 to current time.
count_over_time()
*/
/*
func (sh * SmoothHistogramCS) query_interval_count(key string, t1 int64) float64 {
	var diff int64 = math.MaxInt64
	var return_bucket int = 0
	for i := 0; i < sh.s_count; i++ {
		curdiff := AbsInt64(t1 - sh.cs_instances[i].min_time)
		if curdiff < diff {
			diff = curdiff
			return_bucket = i
		}
	}

	return float64(sh.cs_instances[return_bucket].EstimateStringFrequency(key))
}
*/

/*
Query the l2 sum of items of a given key in the interval t1 to current time.
stddev_over_time, stdvar_over_time
*/
/*
func (sh * SmoothHistogramCS) query_interval_l2(key string, t1 int64) float64 {
	var diff int64 = math.MaxInt64
	var return_bucket int = 0
	for i := 0; i < sh.s_count; i++ {
		curdiff := AbsInt64(t1 - sh.cs_instances[i].min_time)
		if curdiff < diff {
			diff = curdiff
			return_bucket = i
		}
	}

	return sh.cs_instances[return_bucket].EstimateStringL2(key)
}


func (sh * SmoothHistogramCS) query_interval_entropy(key string, t1 int64) float64 {
	var diff int64 = math.MaxInt64
	var return_bucket int = 0
	for i := 0; i < sh.s_count; i++ {
		curdiff := AbsInt64(t1 - sh.cs_instances[i].min_time)
		if curdiff < diff {
			diff = curdiff
			return_bucket = i
		}
	}

	return sh.cs_instances[return_bucket].EstimateStringEntropy(key)
}

// L1 norm based on count sketch
func (sh * SmoothHistogramCS) query_T1T2interval_sum(key string, t1, t2 int64) float64 {
	return sh.query_interval_sum(key, t1) - sh.query_interval_sum(key, t2)
}

// L0 norm based on count sketch
func (sh * SmoothHistogramCS) query_T1T2interval_count(key string, t1, t2 int64) float64 {
	return sh.query_interval_count(key, t1) - sh.query_interval_count(key, t2)
}

// L2 norm based on count sketch
func (sh * SmoothHistogramCS) query_T1T2interval_l2(key string, t1, t2 int64) float64 {
	return sh.query_interval_l2(key, t1) - sh.query_interval_l2(key, t2)
}

func (sh * SmoothHistogramCS) query_T1T2interval_entropy(key string, t1, t2 int64) float64 {
	return sh.query_interval_entropy(key, t1) - sh.query_interval_entropy(key, t2)
}
*/

/*
// L2 norm based on count sketch
func (sh * SmoothHistogramCS) query_T1T2interval_l2(key string, t1, t2 int64) float64 {
	var diff1 int64 = math.MaxInt64
	var diff2 int64 = math.MaxInt64
	var from_bucket, to_bucket int = 0, 0
	for i := 0; i < sh.s_count; i++ {
		curdiff1 := AbsInt64(t1 - sh.cs_instances[i].min_time)
		if curdiff1 < diff1 {
			diff1 = curdiff1
			from_bucket = i
		}
		curdiff2 := AbsInt64(t2 - sh.cs_instances[i].min_time)
		if curdiff2 < diff2 {
			diff2 = curdiff2
			to_bucket = i
		}
	}


	sos := make([]float64, CS_ROW_NO_Univ)
	for j := 0; j < CS_ROW_NO_Univ; j++ {
		sos[j] = 0
	}
	for i := 0; i < CS_ROW_NO_Univ; i++ {
		for j := 0; j < CS_COL_NO_Univ; j++ {
			var temp_dif float64 = AbsFloat64(sh.cs_instances[from_bucket].count[i][j] - sh.cs_instances[to_bucket].count[i][j]);
			sos[i] += temp_dif * temp_dif
		}
	}

	sort.Slice(sos, func(i, j int) bool { return sos[i] < sos[j] })
	median := sos[CS_ROW_NO_Univ / 2]
	return math.Sqrt(median)
}
*/

/*
Query the Heavy Hitters >=threshold in the interval t1 to t2.
*/
/*
func (shc *SmoothHistogramCount) query_interval_hh(t1, t2 int64, threshold float64) (hh *HeavyHitter) {
	hh = &HeavyHitter{
		hhs: make([]Item, 0),
	}
	for i := 0; i < shc.given_key_size; i++ {
		key_count := shc.query_interval_count(t1, t2, shc.stored_keys[i])
		if key_count >= threshold {
			hh.hhs = append(hh.hhs, Item{
				key: shc.stored_keys[i],
				count: key_count,
			})
		}
	}

	return hh
}
*/

/*
func (shc *SmoothHistogramCount) query_interval_count(t2, t1 int64, query_key string) float64 {
	diff1, diff2 := math.MaxInt64, math.MaxInt64
	var from_bucket, to_bucket int = 0, 0

	found, ok := shc.keys[query_key]
	var index int

	if ok {
		index = found
	} else {
		return -1
	}

	for i := 0; i < shc.s_count[index]; i++ {
		curdiff1 := AbsInt64(t1 - (shc.buckets[index][i].bucketsize + shc.zero_count[index] + shc.excluded_zero))
		curdiff2 := AbsInt64(t2 - (shc.buckets[index][i].bucketsize + shc.zero_count[index] + shc.excluded_zero))
		if curdiff1 < diff1 {
			diff1 = curdiff1
			to_bucket = i
		}

		if curdiff2	< diff2 {
			diff2 = curdiff2
			from_bucket = i
		}
	}
	interval_count := shc.buckets[index][from_bucket].counter - shc.buckets[index][to_bucket].counter
	return interval_count
}
*/
