package promsketch

import (
	"github.com/zzylol/prometheus-sketch-VLDB/prometheus-sketches/util/zeropool"
)

const WINDOW_SIZE int = 1000000
const HASH_SEED int = 2147483647

/* sketch configurations */
const CM_ROW_NO int = 5
const CM_COL_NO int = 1000

// for UnivMon
const CS_ROW_NO_Univ int = 3
const CS_COL_NO_Univ int = 512
const CS_LVLS int = 4

const CS_ROW_NO int = 5
const CS_COL_NO int = 4096
const CS_ONE_COL_NO int = 100000

const TOPK int = 1
const TOPK_SIZE int = 100
const TOPK_SIZE2 int = 200

const INTERVAL int = 1000 // ms
const MILLION int = 1000000
const BILLION int = 1000000000

const UnivPoolCAP uint32 = 10

const maxPointsSliceSize = 5000

var zero_int64_uarr []int64

func init() {
	zero_int64_uarr = make([]int64, CS_COL_NO_Univ)
	for i := 0; i < CS_COL_NO_Univ; i++ {
		zero_int64_uarr[i] = 0
	}
}

type CountBucket struct {
	count      int64
	sum        float64
	sum2       float64
	bucketsize int
	min_time   int64
	max_time   int64
}

var (
	farr2Pool = zeropool.New(func() [][]float64 {
		tmp := make([][]float64, CS_ROW_NO_Univ)
		for r := 0; r < CS_ROW_NO_Univ; r++ {
			tmp[r] = make([]float64, CS_COL_NO_Univ)
			tmp[r][0] = 0
			for c := 1; c < CS_COL_NO_Univ; c *= 2 {
				copy(tmp[r][c:], tmp[r][:c])
			}
		}
		return tmp
	})

	iarr2Pool = zeropool.New(func() [][]int64 {
		tmp := make([][]int64, CS_ROW_NO_Univ)
		for r := 0; r < CS_ROW_NO_Univ; r++ {
			tmp[r] = make([]int64, CS_COL_NO_Univ)
			tmp[r][0] = 0
			for c := 1; c < CS_COL_NO_Univ; c *= 2 {
				copy(tmp[r][c:], tmp[r][:c])
			}
		}
		return tmp
	})

	iarrPool = zeropool.New(func() []int64 {
		tmp := make([]int64, CS_ROW_NO_Univ)
		for r := 0; r < CS_ROW_NO_Univ; r++ {
			tmp[r] = 0
		}
		return tmp
	})

	farrPool = zeropool.New(func() []float64 {
		tmp := make([]float64, CS_ROW_NO_Univ)
		for r := 0; r < CS_ROW_NO_Univ; r++ {
			tmp[r] = 0
		}
		return tmp
	})
)

func MedianOfThree(a, b, c int64) int64 {
	if a <= b && b <= c || c <= b && b <= a {
		return b
	} else if a <= c && c <= b || b <= c && c <= a {
		return c
	} else {
		return a
	}
}

func AbsInt(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func AbsInt64(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

func AbsFloat64(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

func MaxFloat64(x float64, y float64) float64 {
	if x < y {
		return y
	} else {
		return x
	}
}

func SignInt(x int) int {
	if x < 0 {
		return -1
	} else {
		return 1
	}
}

func SignFloat64(x float64) float64 {
	if x < 0 {
		return -1
	} else {
		return 1
	}
}

func MinInt(x int, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

func i64tob(val uint64) []byte {
	r := make([]byte, 8)
	for i := uint64(0); i < 8; i++ {
		r[i] = byte((val >> (i * 8)) & 0xff)
	}
	return r
}

func btoi64(val []byte) uint64 {
	r := uint64(0)
	for i := uint64(0); i < 8; i++ {
		r |= uint64(val[i]) << (8 * i)
	}
	return r
}

func i32tob(val uint32) []byte {
	r := make([]byte, 4)
	for i := uint32(0); i < 4; i++ {
		r[i] = byte((val >> (8 * i)) & 0xff)
	}
	return r
}

func btoi32(val []byte) uint32 {
	r := uint32(0)
	for i := uint32(0); i < 4; i++ {
		r |= uint32(val[i]) << (8 * i)
	}
	return r
}

func MinInt64(a, b int64) int64 {
	if a < b {
		return a
	} else {
		return b
	}
}

func MaxInt64(a, b int64) int64 {
	if a > b {
		return a
	} else {
		return b
	}
}
