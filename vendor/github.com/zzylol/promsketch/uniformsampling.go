package promsketch

import (
	"math"
	"math/rand"
	"sort"
)

type UniformSampling struct {
	Arr              []Sample
	Max_size         int
	Time_window_size int64
	Sampling_rate    float64
	Cur_time         int64
	K                int
}

func NewUniformSampling(Time_window_size int64, Sampling_rate float64, Max_size int) *UniformSampling {
	s := UniformSampling{
		Arr:              make([]Sample, 0),
		Max_size:         Max_size,
		Time_window_size: Time_window_size,
		Sampling_rate:    Sampling_rate,
		Cur_time:         0,
		K:                0,
	}
	return &s
}

func (s *UniformSampling) GetMemory() float64 {
	return (float64(len(s.Arr))*16 + 24) / 1024
}

func (s *UniformSampling) Insert(t int64, x float64) {
	s.Cur_time = t
	shift := 0
	// fmt.Println(s.Arr)
	for i := range s.Arr {
		if s.Arr[i].T < s.Cur_time-s.Time_window_size {
			shift = i
		} else {
			shift = i
			break
		}
	}
	s.Arr = s.Arr[shift:]
	r := rand.Float64()
	if r < s.Sampling_rate {
		s.Arr = append(s.Arr, Sample{T: t, F: x})
	}
}

/*
func (s *UniformSampling) Insert(t int64, x float64) {
	// Reservoir sampling
	s.Cur_time = t
	shift := 0
	fmt.Println(s.Arr)
	for i := range s.Arr {
		if s.Arr[i].T < s.Cur_time-s.Time_window_size {
			shift = i
		} else {
			shift = i
			break
		}
	}
	s.Arr = s.Arr[shift:]
	fmt.Println(shift)
	fmt.Println(s.Arr)
	s.K -= shift
	if s.K < s.Max_size {
		s.Arr = append(s.Arr, Sample{T: t, F: x})
	} else {
		r := rand.Float64()
		if r < float64(s.Max_size)/float64(s.K+1) {
			if len(s.Arr) < s.Max_size {
				s.Arr = append(s.Arr, Sample{T: t, F: x})
			} else {
				r_idx := rand.Intn(len(s.Arr))
				s.Arr = append(s.Arr[:r_idx], s.Arr[r_idx+1:]...)
				s.Arr = append(s.Arr, Sample{T: t, F: x})
			}
		}
	}
	s.K += 1
}
*/

// quantile calculates the given quantile of a vector of samples.
//
// The Vector will be sorted.
// If 'values' has zero elements, NaN is returned.
// If q==NaN, NaN is returned.
// If q<0, -Inf is returned.
// If q>1, +Inf is returned.
func quantile(q float64, values []float64) float64 {
	if len(values) == 0 || math.IsNaN(q) {
		return math.NaN()
	}
	if q < 0 {
		return math.Inf(-1)
	}
	if q > 1 {
		return math.Inf(+1)
	}
	sort.Float64s(values)

	n := float64(len(values))
	// When the quantile lies between two samples,
	// we use a weighted average of the two samples.
	rank := q * (n - 1)

	lowerIndex := math.Max(0, math.Floor(rank))
	upperIndex := math.Min(n-1, lowerIndex+1)

	weight := rank - math.Floor(rank)
	return values[int(lowerIndex)]*(1-weight) + values[int(upperIndex)]*weight
}

// quantile calculates the given quantile of a vector of samples.
//
// The Vector will be sorted.
// If 'values' has zero elements, NaN is returned.
// If q==NaN, NaN is returned.
// If q<0, -Inf is returned.
// If q>1, +Inf is returned.
func quantiles(qs []float64, values []float64) (quantiles []float64) {
	sort.Float64s(values)
	n := float64(len(values))
	for _, q := range qs {
		if q < 0 {
			quantiles = append(quantiles, float64(math.Inf(-1)))
		} else if q > 1 {
			quantiles = append(quantiles, float64(math.Inf(+1)))
		} else {
			// When the quantile lies between two samples,
			// we use a weighted average of the two samples.
			rank := q * (n - 1)
			lowerIndex := math.Max(0, math.Floor(rank))
			upperIndex := math.Min(n-1, lowerIndex+1)
			weight := rank - math.Floor(rank)
			quantiles = append(quantiles, values[int(lowerIndex)]*(1-weight)+values[int(upperIndex)]*weight)
		}
	}
	return quantiles
}

func (s *UniformSampling) QueryQuantile(phis []float64) []float64 {
	values := make([]float64, len(s.Arr))
	for i, v := range s.Arr {
		values[i] = v.F
	}
	return quantiles(phis, values)
}

func (s *UniformSampling) QuerySum() float64 {
	var sum float64 = 0
	for _, v := range s.Arr {
		sum += v.F
	}
	return sum / float64(s.Sampling_rate)
}

func (s *UniformSampling) QueryMax() float64 {
	var max float64 = 0
	for _, v := range s.Arr {
		if v.F > max {
			max = v.F
		}
	}
	return max
}

func (s *UniformSampling) QueryMin() float64 {
	var min float64 = s.Arr[0].F
	for _, v := range s.Arr {
		if v.F < min {
			min = v.F
		}
	}
	return min
}

func (s *UniformSampling) QuerySum2() float64 {
	var sum2 float64 = 0
	for _, v := range s.Arr {
		sum2 += v.F * v.F
	}
	return sum2 / float64(s.Sampling_rate)
}

func (s *UniformSampling) QueryCount() float64 {
	return float64(len(s.Arr)) / float64(s.Sampling_rate)
}

func (s *UniformSampling) QueryL1() float64 {
	m := make(map[float64]int)
	for _, v := range s.Arr {
		if _, ok := m[v.F]; !ok {
			m[v.F] = 1
		} else {
			m[v.F] += 1
		}
	}
	var l1 float64 = 0
	for _, v := range m {
		l1 += float64(v)
	}
	return l1
}

func (s *UniformSampling) QueryL2() float64 {
	m := make(map[float64]int)
	for _, v := range s.Arr {
		if _, ok := m[v.F]; !ok {
			m[v.F] = 1
		} else {
			m[v.F] += 1
		}
	}
	var l2 float64 = 0
	for _, v := range m {
		l2 += float64(v * v)
	}
	return math.Sqrt(l2)
}

func (s *UniformSampling) QueryEntropy() float64 {
	m := make(map[float64]int)
	for _, v := range s.Arr {
		if _, ok := m[v.F]; !ok {
			m[v.F] = 1
		} else {
			m[v.F] += 1
		}
	}
	var entropy float64 = 0
	for _, v := range m {
		entropy += float64(v) * math.Log2(float64(v))
	}
	return math.Log2(float64(len(m))) - entropy/float64(len(m))
}

func (s *UniformSampling) QueryDistinct() float64 {
	m := make(map[float64]int)
	for _, v := range s.Arr {
		m[v.F] = 1
	}
	return float64(len(m))
}
