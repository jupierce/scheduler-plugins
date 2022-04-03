package gravity

import (
	"sync"
)

type MetricsSample struct {
	CpuMillis int64
	Memory    int64
}

type MetricAdder struct {
	CpuMillis int64
	Memory    int64
}

type FreshMetricsMap struct {
	samples       *FreshMap
	adders        *FreshMap
	adderLifetime int64
	mu            sync.Mutex
}

func NewFreshMetricsMap(ln int, sampleMaxTTL int64, adderLifetime int64) (m *FreshMetricsMap) {
	m = &FreshMetricsMap{
		samples:       NewFreshMap(ln, sampleMaxTTL),
		adders:        NewFreshMap(ln, adderLifetime+5),
		adderLifetime: adderLifetime,
	}
	return
}

func (m *FreshMetricsMap) Put(nodeName string, cpuMillis int64, memory int64) *MetricsSample {
	value := &MetricsSample{
		CpuMillis: cpuMillis,
		Memory:    memory,
	}
	m.samples.Put(nodeName, value)
	return value
}

func (m *FreshMetricsMap) AddAdder(nodeName string, cpuMillis int64, memory int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	newAdder := &MetricAdder{
		CpuMillis: cpuMillis,
		Memory:    memory,
	}
	storedInt := m.adders.Get(nodeName, m.adderLifetime)
	if storedInt != nil {
		stored := storedInt.(*MetricAdder)
		// Stack any current adder on this one. This will give old
		// adder values a longer functional effect, but that
		// should be fine.
		newAdder.CpuMillis += stored.CpuMillis
		newAdder.Memory += stored.Memory
	}
	m.adders.Put(nodeName, newAdder)
}

func (m *FreshMetricsMap) processAdder(nodeName string, sample *MetricsSample) *MetricsSample {
	var adder *MetricAdder
	adderInt := m.adders.Get(nodeName, m.adderLifetime)
	if adderInt != nil {
		adder = adderInt.(*MetricAdder)
	}

	if adder == nil {
		// No change necessary
		return sample
	} else {
		computed := &MetricsSample{
			CpuMillis: sample.CpuMillis + adder.CpuMillis,
			Memory:    sample.Memory + adder.Memory,
		}
		return computed
	}
}

func (m *FreshMetricsMap) Get(nodeName string, noOlderThan int64) *MetricsSample {
	var stored *MetricsSample
	storedInt := m.samples.Get(nodeName, noOlderThan)
	if storedInt != nil {
		stored = storedInt.(*MetricsSample)
		return m.processAdder(nodeName, stored)
	}
	return nil
}

// GetOrPut helps make sure that
func (m *FreshMetricsMap) GetOrPut(nodeName string, noOlderThan int64, getValues func() (int64, int64, error)) (*MetricsSample, error) {

	// Don't return value directly -- use m.Get afterward to include adders
	stored, err := m.samples.GetOrPut(nodeName, noOlderThan, func() (interface{}, error) {
		cpu, memory, err := getValues()
		if err != nil {
			return nil, err
		}
		return &MetricsSample{
			CpuMillis: cpu,
			Memory:    memory,
		}, nil
	})

	if err != nil {
		return nil, err
	}
	// Don't try to call Get here. We must add to what we found.
	// It but possible that .Get would return nil if the expiration
	// of the metric happened between GetOrPut and a call here.
	return m.processAdder(nodeName, stored.(*MetricsSample)), nil
}
