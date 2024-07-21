package promsketch

import (
	// "fmt"

	"hash"
	"hash/fnv"
	"math"

	"github.com/cespare/xxhash/v2"
	//"strconv"
)

/*
Can be used for Prometheus functions: count_over_time, entropy_over_time (newly added), hh(topk)_over_time (newly added),
 card_over_time (newly added), sum_over_time, avg_over_time, stddev_over_time, stdvar_over_time, min_over_time, max_over_time
*/

type HHLayerStruct struct {
	topK *TopKHeap
	// HH_table map[string]([]Item)
	// HH_list	[]Item
}

func NewHHLayerStruct(k int) (hh_layer_s HHLayerStruct, err error) {
	topkheap := NewTopKHeap(k)
	hh_layer_s = HHLayerStruct{
		topK: topkheap,
		// HH_table: make(map[string]([]Item)),
	}

	return hh_layer_s, nil
}

type UnivSketch struct {
	k           int // topK
	row         int
	col         int
	layer       int
	hasher      hash.Hash64
	cs_layers   []CountSketchUniv
	HH_layers   []HHLayerStruct
	seed        uint32 // one hash for all layers
	bucketsize  int    // for sliding window model based on item size; per sketch
	max_time    int64  // for sliding window model based on time; per sketch
	min_time    int64  // for sliding window model based on time; per sketch
	pool_idx    int64
	heap_update int
}

// New create a new Universal Sketch with row hashing funtions and col counters per row of a Count Sketch.
func NewUnivSketch(k, row, col, layer int, seed1, seed2 []uint32, seed3 uint32, pool_idx int64) (us UnivSketch, err error) {

	us = UnivSketch{
		k:           k,
		row:         row,
		col:         col,
		layer:       layer,
		hasher:      fnv.New64(),
		pool_idx:    pool_idx,
		heap_update: 0,
		seed:        seed3,
	}

	// t_now := time.Now()
	us.cs_layers = make([]CountSketchUniv, layer)
	us.HH_layers = make([]HHLayerStruct, layer)
	// since := time.Since(t_now)
	// fmt.Println("make time", since.Seconds())

	for i := 0; i < layer; i++ {
		// t_now := time.Now()
		us.cs_layers[i], _ = NewCountSketchUniv(row, col, seed1, seed2)
		// since := time.Since(t_now)
		// fmt.Println("new cs time", since)
	}

	// t_now := time.Now()
	for i := 0; i < layer; i++ {
		us.HH_layers[i], _ = NewHHLayerStruct(k)
	}
	// since := time.Since(t_now)
	// fmt.Println("new heap time", since.Seconds())

	return us, nil
}

func (us UnivSketch) Free() {
	for i := 0; i < us.layer; i++ {
		us.cs_layers[i].CleanCountSketchUniv()
		us.HH_layers[i].topK.Clean()
	}
}

func (us UnivSketch) GetMemoryKB() float64 {
	var total_topk float64 = 0
	for i := 0; i < us.layer; i++ {
		total_topk += us.HH_layers[i].topK.GetMemoryBytes()
	}
	return (float64(CS_COL_NO_Univ)*float64(CS_ROW_NO_Univ)*float64(us.layer)*8 + total_topk) / 1024
}

// Update Universal Sketch

// find the last possible layer for each key
func (us UnivSketch) findBottomLayerNum(hash uint64, layer int) int {
	// optimization -- hash only once
	// if hash mod 2 == 1, go down
	for l := 0; l < layer-1; l++ {
		if ((hash >> l) & 1) == 0 {
			return l
		}
	}
	return layer - 1
}

// update multiple layers from top to bottom_layer_num
// insert a key into Universal Sketch
func (us UnivSketch) update(key string, value int64, bottom_layer_num int) {
	// t := time.Now()
	// update item key to the bottom CS layer only

	// since := time.Since(t)
	// fmt.Println("cs update time=", since)
	// use bottom layer value to update upper layer topk heap

	// t := time.Now()
	// us.heap_update = (us.heap_update + 1) % 10
	// r := rand.Intn(10)
	// if r == 0 {
	for l := 0; l <= bottom_layer_num; l++ {
		median_count := us.cs_layers[l].UpdateAndEstimateString(key, value) // add item key to the layer
		us.HH_layers[l].topK.Update(key, median_count)
	}
	// }
	// since := time.Since(t)
	// fmt.Println("topk update time=", since)
}

func (us UnivSketch) univmon_processing(key string, value int64) {
	// t := time.Now()
	// hash := wyhash.Hash([]byte(key), uint64(us.seed))
	hash := xxhash.Sum64String(key)
	// since := time.Since(t)
	// fmt.Println("hash key time=", since)
	// t = time.Now()
	bottom_layer_num := us.findBottomLayerNum(hash, CS_LVLS)
	// since = time.Since(t)
	// fmt.Println("find bottom layer time=", since)
	// t = time.Now()
	us.update(key, value, bottom_layer_num)
	// since = time.Since(t)
	// fmt.Println("univmon update time=", since)
}

// Query Universal Sketch
func (us UnivSketch) calcGSumHeuristic(g func(float64) float64) float64 {
	Y := make([]float64, us.layer)
	var coe float64 = 1
	var hash float64 = 0.0
	var tmp float64 = 0

	Y[us.layer-1] = 0

	for _, item := range us.HH_layers[us.layer-1].topK.heap {
		// fmt.Println(item.key, coe, item.count)
		tmp += g(float64(item.count))
	}
	Y[us.layer-1] = tmp

	for i := (us.layer - 2); i >= 0; i-- {
		tmp = 0
		// fmt.Println("==============")
		for _, item := range us.HH_layers[i].topK.heap {
			// fmt.Println(item.key, item.count)
			hash = 0.0
			for _, next_layer_item := range us.HH_layers[i+1].topK.heap {
				if item.key == next_layer_item.key {
					hash = 1.0
					break
				}
			}
			coe = 1 - 2*hash
			// fmt.Println(item.key, coe, item.count)
			tmp += coe * g(float64(item.count))
		}
		Y[i] = 2*Y[i+1] + tmp
	}

	return Y[0]
}

func (us UnivSketch) calcGSum(g func(float64) float64) float64 {
	return us.calcGSumHeuristic(g)
}

func (us UnivSketch) calcL1() float64 {
	return us.calcGSum(func(x float64) float64 { return x })
}

func (us UnivSketch) calcL2() float64 {
	return us.calcGSum(func(x float64) float64 { return x * x })
}

func (us UnivSketch) calcEntropy() float64 {
	return us.calcGSum(func(x float64) float64 {
		if x > 0 {
			return x * math.Log2(x)
		} else {
			return 0
		}
	})
}

func (us UnivSketch) calcCard() float64 {
	return us.calcGSum(func(x float64) float64 { return 1 })
}

func (us UnivSketch) MergeWith(other UnivSketch) { // Addition
	for i := 0; i < CS_LVLS; i++ {
		for j := 0; j < CS_ROW_NO_Univ; j++ {
			for k := 0; k < CS_COL_NO_Univ; k++ {
				us.cs_layers[i].count[j][k] = us.cs_layers[i].count[j][k] + other.cs_layers[i].count[j][k]
			}
		}

		for _, item := range us.HH_layers[i].topK.heap {
			us.HH_layers[i].topK.Update(item.key, us.cs_layers[i].EstimateStringCount(item.key))
		}

		for _, item := range other.HH_layers[i].topK.heap {
			us.HH_layers[i].topK.Update(item.key, us.cs_layers[i].EstimateStringCount(item.key))
		}
	}
}
