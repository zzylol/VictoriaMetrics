package promsketch

import (
	"context"
	"math"
)

type FunctionCall func(ctx context.Context, series *memSeries, c float64, t1, t2 int64) Vector

// FunctionCalls is a list of all functions supported by PromQL, including their types.
var FunctionCalls = map[string]FunctionCall{
	"change_over_time":   funcChangeOverTime,
	"avg_over_time":      funcAvgOverTime,
	"count_over_time":    funcCountOverTime,
	"entropy_over_time":  funcEntropyOverTime,
	"max_over_time":      funcMaxOverTime,
	"min_over_time":      funcMinOverTime,
	"stddev_over_time":   funcStddevOverTime,
	"stdvar_over_time":   funcStdvarOverTime,
	"sum_over_time":      funcSumOverTime,
	"sum2_over_time":     funcSum2OverTime,
	"Card_over_time":     funcCardOverTime,
	"L1_over_time":       funcL1OverTime,
	"L2_over_time":       funcL2OverTime,
	"quantile_over_time": funcQuantileOverTime,
}

// TODO: add last item value in the change data structure
func funcChangeOverTime(ctx context.Context, series *memSeries, c float64, t1, t2 int64) Vector {
	bucket_1, err1 := series.sketchInstances.ehc.QueryIntervalMergeCount(t1, t2)
	if err1 != nil {
		return make(Vector, 0)
	}

	count := float64(bucket_1.count)

	return Vector{Sample{
		F: count,
	}}
}

func funcAvgOverTime(ctx context.Context, series *memSeries, c float64, t1, t2 int64) Vector {
	bucket_1, err1 := series.sketchInstances.ehc.QueryIntervalMergeCount(t1, t2)
	if err1 != nil {
		return make(Vector, 0)
	}

	count := float64(bucket_1.count)
	sum := series.sketchInstances.EffSum.Query(t1, t2, false)

	avg := float64(sum) / float64(count)
	return Vector{Sample{
		F: avg,
	}}
}

func funcSumOverTime(ctx context.Context, series *memSeries, c float64, t1, t2 int64) Vector {
	sum := series.sketchInstances.EffSum.Query(t1, t2, false)
	return Vector{Sample{
		F: sum,
	}}
}

func funcSum2OverTime(ctx context.Context, series *memSeries, c float64, t1, t2 int64) Vector {
	sum2 := series.sketchInstances.EffSum2.Query(t1, t2, false)
	return Vector{Sample{
		F: sum2,
	}}
}

func funcCountOverTime(ctx context.Context, series *memSeries, c float64, t1, t2 int64) Vector {
	merged_bucket, err := series.sketchInstances.ehc.QueryIntervalMergeCount(t1, t2)
	if err != nil {
		return make(Vector, 0)
	} else {
		count := merged_bucket.count
		return Vector{Sample{
			F: float64(count),
		}}
	}
}

func funcSumOverTimeEH(ctx context.Context, series *memSeries, c float64, t1, t2 int64) Vector {
	merged_bucket, err := series.sketchInstances.ehc.QueryIntervalMergeCount(t1, t2)
	if err != nil {
		return make(Vector, 0)
	} else {
		sum := merged_bucket.sum
		return Vector{Sample{
			F: sum,
		}}
	}

}

func funcSum2OverTimeEH(ctx context.Context, series *memSeries, c float64, t1, t2 int64) Vector {
	merged_bucket, err := series.sketchInstances.ehc.QueryIntervalMergeCount(t1, t2)
	if err != nil {
		return make(Vector, 0)
	} else {
		sum2 := merged_bucket.sum2
		return Vector{Sample{
			F: sum2,
		}}
	}
}

func funcStddevOverTime(ctx context.Context, series *memSeries, c float64, t1, t2 int64) Vector {
	bucket_1, err1 := series.sketchInstances.ehc.QueryIntervalMergeCount(t1, t2)
	if err1 != nil {
		return make(Vector, 0)
	}

	count := float64(bucket_1.count - bucket_1.count)
	sum := series.sketchInstances.EffSum.Query(t1, t2, false)
	sum2 := series.sketchInstances.EffSum2.Query(t1, t2, false)

	stddev := math.Sqrt(sum2/count - math.Pow(sum/count, 2))
	return Vector{Sample{
		F: float64(stddev),
	}}
}

func funcStdvarOverTime(ctx context.Context, series *memSeries, c float64, t1, t2 int64) Vector {
	bucket_1, err1 := series.sketchInstances.ehc.QueryIntervalMergeCount(t1, t2)
	if err1 != nil {
		return make(Vector, 0)
	}

	count := float64(bucket_1.count - bucket_1.count)
	sum := series.sketchInstances.EffSum.Query(t1, t2, false)
	sum2 := series.sketchInstances.EffSum2.Query(t1, t2, false)

	stdvar := sum2/count - math.Pow(sum/count, 2)
	return Vector{Sample{
		F: stdvar,
	}}

}

func funcEntropyOverTime(ctx context.Context, series *memSeries, c float64, t1, t2 int64) Vector {
	merged_univ, err := series.sketchInstances.shuniv.QueryIntervalMergeUniv(t1, t2)
	if err != nil {
		return make(Vector, 0)
	}
	entropynorm := merged_univ.calcEntropy()

	bucket_1, err1 := series.sketchInstances.ehc.QueryIntervalMergeCount(t1, t2)
	if err1 != nil {
		return make(Vector, 0)
	}

	m := float64(bucket_1.count - bucket_1.count)

	entropy := math.Log(m)/math.Log(2) - entropynorm/m
	return Vector{Sample{
		F: entropy,
	}}
}

func funcCardOverTime(ctx context.Context, series *memSeries, c float64, t1, t2 int64) Vector {
	merged_univ, err := series.sketchInstances.shuniv.QueryIntervalMergeUniv(t1, t2)
	if err != nil {
		return make(Vector, 0)
	}
	card := merged_univ.calcCard()
	return Vector{Sample{
		F: card,
	}}
}

func funcL1OverTime(ctx context.Context, series *memSeries, c float64, t1, t2 int64) Vector {
	merged_univ, err := series.sketchInstances.shuniv.QueryIntervalMergeUniv(t1, t2)
	if err != nil {
		return make(Vector, 0)
	}
	l1 := merged_univ.calcL1()
	return Vector{Sample{
		F: l1,
	}}
}

func funcL2OverTime(ctx context.Context, series *memSeries, c float64, t1, t2 int64) Vector {
	merged_univ, err := series.sketchInstances.shuniv.QueryIntervalMergeUniv(t1, t2)
	if err != nil {
		return make(Vector, 0)
	}
	l2 := merged_univ.calcL2()
	return Vector{Sample{
		F: l2,
	}}
}

func funcQuantileOverTime(ctx context.Context, series *memSeries, phi float64, t1, t2 int64) Vector {
	merged_dd := series.sketchInstances.ehdd.QueryIntervalMergeDD(t1, t2)
	phis := []float64{phi}
	q_values, _ := merged_dd.GetValuesAtQuantiles(phis)
	return Vector{Sample{
		F: q_values[0],
	}}
}

func funcMinOverTime(ctx context.Context, series *memSeries, c float64, t1, t2 int64) Vector {
	merged_dd := series.sketchInstances.ehdd.QueryIntervalMergeDD(t1, t2)
	phis := []float64{0}
	q_values, _ := merged_dd.GetValuesAtQuantiles(phis)
	return Vector{Sample{
		F: q_values[0],
	}}
}

func funcMaxOverTime(ctx context.Context, series *memSeries, c float64, t1, t2 int64) Vector {
	merged_dd := series.sketchInstances.ehdd.QueryIntervalMergeDD(t1, t2)
	phis := []float64{1}
	q_values, _ := merged_dd.GetValuesAtQuantiles(phis)
	return Vector{Sample{
		F: q_values[0],
	}}
}
