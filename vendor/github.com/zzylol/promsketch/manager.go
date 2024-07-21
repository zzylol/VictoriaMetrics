package promsketch

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/zzylol/prometheus-sketch-VLDB/prometheus-sketches/model/labels"
	"github.com/zzylol/prometheus-sketch-VLDB/prometheus-sketches/storage"
)

// ManagerOptions bundles options for the Manager.
type ManagerOptions struct {
	RuleTests []SketchRuleTest
	Context   context.Context
	Logger    log.Logger
	// Registerer             prometheus.Registerer
	Appendable             storage.Appendable
	Queryable              storage.Queryable
	OutageTolerance        time.Duration
	ForGracePeriod         time.Duration
	ResendDelay            time.Duration
	MaxConcurrentEvals     int64
	ConcurrentEvalsEnabled bool
}

// The Manager manages recording and alerting rules.
// For prototyping, we assume input sketch rules are all independent and can be evaluated in parallel if permitted.
type Manager struct {
	Sconfig  *SketchConfig
	Sketches *PromSketches
	Rules    []*SketchRule
	Opts     *ManagerOptions
	mtx      sync.RWMutex
	block    chan struct{}
	done     chan struct{}
	restored bool

	logger log.Logger
}

func NewManager(o *ManagerOptions) *Manager {

	m := &Manager{
		Rules:  make([]*SketchRule, 0),
		Opts:   o,
		block:  make(chan struct{}),
		done:   make(chan struct{}),
		logger: o.Logger,
	}

	return m
}

func NewManagerWithSketches(o *ManagerOptions, sc *SketchConfig) *Manager {

	m := &Manager{
		Rules:   make([]*SketchRule, 0),
		Opts:    o,
		Sconfig: sc,
		block:   make(chan struct{}),
		done:    make(chan struct{}),
		logger:  o.Logger,
	}

	// For PromSketch prototype performance test only
	m.NewSketchRules(sc)

	return m
}

func (m *Manager) NewSketchRules(sc *SketchConfig) {
	lset := labels.FromStrings("fake_metric", "machine0") // TODO: use more lsets

	avgsmap := make(map[SketchType]bool)
	avgsmap[EHCount] = true
	entropysmap := make(map[SketchType]bool)
	entropysmap[SHUniv] = true
	entropysmap[EffSum] = true
	entropysmap[EHCount] = true
	quantilesmap := make(map[SketchType]bool)
	quantilesmap[EHDD] = true
	m.Opts.RuleTests = []SketchRuleTest{
		{"avg_over_time", funcAvgOverTime, lset, -1, 1000000, 1000000, avgsmap},
		{"count_over_time", funcCountOverTime, lset, -1, 1000000, 1000000, avgsmap},
		{"entropy_over_time", funcEntropyOverTime, lset, -1, 1000000, 1000000, entropysmap},
		{"l1_over_time", funcL1OverTime, lset, -1, 1000000, 1000000, entropysmap},
		{"quantile_over_time", funcQuantileOverTime, lset, 0.5, 1000000, 1000000, quantilesmap},
	}

	m.Sketches = NewPromSketchesWithConfig(m.Opts.RuleTests, sc) // storage

	for _, t := range m.Opts.RuleTests {
		m.Rules = append(m.Rules, NewSketchRule(t, m.Sketches, m.Opts, m.done, m.logger)) // query
	}
}

// Run starts processing of the rule manager. It is blocking.
func (m *Manager) Run() {
	level.Info(m.logger).Log("msg", "Starting sketch manager...")
	m.start()
	<-m.done // synchronization
}

func (m *Manager) start() {
	for _, r := range m.Rules {
		go func(r *SketchRule) {
			r.run(m.Opts.Context)
		}(r)
	}
}

// Stop the rule manager's rule evaluation cycles.
func (m *Manager) Stop() {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	level.Info(m.logger).Log("msg", "Stopping sketch manager...")

	for _, r := range m.Rules {
		r.stop()
	}

	// Shut down the rules waiting multiple evaluation intervals to write
	// staleness markers.
	close(m.done)

	level.Info(m.logger).Log("msg", "Sketch manager stopped")
}
