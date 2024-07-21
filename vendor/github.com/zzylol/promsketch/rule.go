package promsketch

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/zzylol/prometheus-sketch-VLDB/prometheus-sketches/model/labels"
	"go.uber.org/atomic"
)

type ruleOrigin struct{}

// RuleHealth describes the health state of a rule.
type RuleHealth string

// The possible health states of a rule based on the last execution.
const (
	HealthUnknown RuleHealth = "unknown"
	HealthGood    RuleHealth = "ok"
	HealthBad     RuleHealth = "err"
)

// For prototyping, we manually add sketch rules in code currently.
type SketchRuleTest struct {
	name          string
	sfunc         FunctionCall
	lset          labels.Labels
	c             float64
	window_size   int64 // us
	eval_interval int64 // us
	stypemap      map[SketchType]bool
}

var SketchRuleTests []SketchRuleTest

type SketchRule struct {
	name   string
	sfunc  FunctionCall
	labels labels.Labels
	Opts   *ManagerOptions
	series *memSeries
	// The health of the sketch rule.
	health *atomic.String
	// Timestamp of last evaluation of the sketch rule.
	evaluationTimestamp *atomic.Time
	// The last error seen by the sketch rule.
	lastError *atomic.Error
	// Duration of how long it took to evaluate the rule.
	evaluationDuration *atomic.Duration

	interval    time.Duration
	window_size int64 // us
	c           float64

	done        chan struct{}
	terminated  chan struct{}
	managerDone chan struct{}
	logger      log.Logger
	timeout     time.Duration
}

func NewSketchRule(sr SketchRuleTest, ps *PromSketches, Opts *ManagerOptions, mdone chan struct{}, mlogger log.Logger) *SketchRule {
	return &SketchRule{
		name:                sr.name,
		sfunc:               sr.sfunc,
		interval:            time.Duration(sr.eval_interval * int64(time.Microsecond)),
		window_size:         sr.window_size,
		c:                   sr.c,
		labels:              sr.lset,
		Opts:                Opts,
		health:              atomic.NewString(string(HealthUnknown)),
		evaluationTimestamp: atomic.NewTime(time.Time{}),
		evaluationDuration:  atomic.NewDuration(0),
		lastError:           atomic.NewError(nil),
		done:                make(chan struct{}),
		terminated:          make(chan struct{}),
		managerDone:         mdone,
		logger:              mlogger,
		timeout:             time.Duration(100) * time.Microsecond,
		series:              ps.series.getByHash(sr.lset.Hash(), sr.lset),
	}
}

func (rule *SketchRule) run(ctx context.Context) {
	defer close(rule.terminated)

	// Wait an initial amount to have consistently slotted intervals.
	evalTimestamp := rule.EvalTimestamp(time.Now().UnixNano()).Add(rule.interval)
	select {
	case <-time.After(time.Until(evalTimestamp)):
	case <-rule.done:
		return
	}

	// The assumption here is that since the ticker was started after having
	// waited for `evalTimestamp` to pass, the ticks will trigger soon
	// after each `evalTimestamp + N * g.interval` occurrence.
	tick := time.NewTicker(rule.interval)
	defer tick.Stop()

	for {
		select {
		case <-rule.done:
			return
		default:
			select {
			case <-rule.done:
				return
			case <-tick.C:
				missed := (time.Since(evalTimestamp) / rule.interval) - 1
				evalTimestamp = evalTimestamp.Add((missed + 1) * rule.interval)

				rule.evalIterationFunc(ctx, evalTimestamp)
				fmt.Println(rule.GetEvaluationDuration())
			}
		}
	}
}

func NewOriginContext(ctx context.Context, rule string) context.Context {
	return context.WithValue(ctx, ruleOrigin{}, rule)
}

// FromOriginContext returns the RuleDetail origin data from the context.
func FromOriginContext(ctx context.Context) string {
	if rule, ok := ctx.Value(ruleOrigin{}).(string); ok {
		return rule
	}
	return ""
}

func (rule *SketchRule) evalIterationFunc(ctx context.Context, ts time.Time) {
	var (
		samplesTotal atomic.Float64
	)
	ctx = NewOriginContext(ctx, rule.name)

	defer func(t time.Time) {
		since := time.Since(t)
		rule.SetEvaluationDuration(since)
		rule.SetEvaluationTimestamp(t)
	}(time.Now())

	vector, err := rule.Eval(ctx, ts)
	if err != nil {
		rule.SetHealth(HealthBad)
		rule.SetLastError(err)
		level.Warn(rule.logger).Log("msg", "Evaluating rule failed", "rule", rule, "err", err)
	}
	rule.SetHealth(HealthGood)
	rule.SetLastError(nil)
	samplesTotal.Add(float64(len(vector)))

}

func (rule *SketchRule) Eval(ctx context.Context, ts time.Time) (Vector, error) {
	ctx, cancel := context.WithTimeout(ctx, rule.timeout)
	defer cancel()
	t2 := int64(ts.Unix())
	t1 := MaxInt64(int64(0), t2-rule.window_size)
	t_now := time.Now()
	vector := rule.sfunc(ctx, rule.series, rule.c, t1, t2)
	since := time.Since(t_now)
	fmt.Println("[rule] query processing time: ", since.Seconds(), "(s)")
	t_now = time.Now()
	rule.AppendToStorage(ctx, vector)
	since = time.Since(t_now)
	fmt.Println("[rule] store results time: ", since.Seconds(), "(s)")

	return vector, nil
}

func (rule *SketchRule) AppendToStorage(ctx context.Context, vector Vector) {
	lb := labels.NewBuilder(labels.EmptyLabels())

	for i := range vector {
		sample := &vector[i]

		lb.Reset(sample.Metric)
		lb.Set(labels.MetricName, rule.name)

		rule.labels.Range(func(l labels.Label) {
			lb.Set(l.Name, l.Value)
		})

		sample.Metric = lb.Labels()
	}

	app := rule.Opts.Appendable.Appender(ctx)
	defer func() {
		if err := app.Commit(); err != nil {
			rule.SetHealth(HealthBad)
			rule.SetLastError(err)
			level.Warn(rule.logger).Log("msg", "Rule sample appending failed", "err", err)
			return
		}
	}()

	for _, s := range vector {
		_, err := app.Append(0, s.Metric, s.T, s.F)

		if err != nil {
			rule.SetHealth(HealthBad)
			rule.SetLastError(err)
			level.Warn(rule.logger).Log("msg", "Failed to add evaluated rule", "rule:", rule, "err", err)

		}
	}
}

// SetEvaluationDuration updates evaluationDuration to the time in seconds it took to evaluate the rule on its last evaluation.
func (rule *SketchRule) SetEvaluationDuration(dur time.Duration) {
	rule.evaluationDuration.Store(dur)
}

// SetLastError sets the current error seen by the sketch rule.
func (rule *SketchRule) SetLastError(err error) {
	rule.lastError.Store(err)
}

// LastError returns the last error seen by the sketch rule.
func (rule *SketchRule) LastError() error {
	return rule.lastError.Load()
}

// SetHealth sets the current health of the sketch rule.
func (rule *SketchRule) SetHealth(health RuleHealth) {
	rule.health.Store(string(health))
}

// Health returns the current health of the sketch rule.
func (rule *SketchRule) Health() RuleHealth {
	return RuleHealth(rule.health.Load())
}

// GetEvaluationDuration returns the time in seconds it took to evaluate the sketch rule.
func (rule *SketchRule) GetEvaluationDuration() time.Duration {
	return rule.evaluationDuration.Load()
}

// SetEvaluationTimestamp updates evaluationTimestamp to the timestamp of when the rule was last evaluated.
func (rule *SketchRule) SetEvaluationTimestamp(ts time.Time) {
	rule.evaluationTimestamp.Store(ts)
}

// GetEvaluationTimestamp returns the time the evaluation took place.
func (rule *SketchRule) GetEvaluationTimestamp() time.Time {
	return rule.evaluationTimestamp.Load()
}

// EvalTimestamp returns the immediately preceding consistently slotted evaluation time.
func (rule *SketchRule) EvalTimestamp(startTime int64) time.Time {
	var (
		offset = int64(rule.hash() % uint64(rule.interval))

		// This group's evaluation times differ from the perfect time intervals by `offset` nanoseconds.
		// But we can only use `% interval` to align with the interval. And `% interval` will always
		// align with the perfect time intervals, instead of this group's. Because of this we add
		// `offset` _after_ aligning with the perfect time interval.
		//
		// There can be cases where adding `offset` to the perfect evaluation time can yield a
		// timestamp in the future, which is not what EvalTimestamp should do.
		// So we subtract one `offset` to make sure that `now - (now % interval) + offset` gives an
		// evaluation time in the past.
		adjNow = startTime - offset

		// Adjust to perfect evaluation intervals.
		base = adjNow - (adjNow % int64(rule.interval))

		// Add one offset to randomize the evaluation times of this group.
		next = base + offset
	)

	return time.Unix(0, next).UTC()
}

func (rule *SketchRule) hash() uint64 {
	return rule.labels.Hash()
}

func (r *SketchRule) stop() {
	close(r.done)
	<-r.terminated
}
