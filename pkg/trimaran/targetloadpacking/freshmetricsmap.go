package targetloadpacking

import (
	"sync"
)

type MetricsSample struct {
	CpuUtil    int
	MemoryUtil int
}

type MetricAdder struct {
	CpuUtil    int
	MemoryUtil int
}

type FreshMetricsMap struct {
	samples *FreshMap
	adders *FreshMap
	adderLifetime int64
	mu sync.Mutex
}

func NewFreshMetricsMap(ln int, sampleMaxTTL int64, adderLifetime int64) (m *FreshMetricsMap) {
	m = &FreshMetricsMap{
		samples: NewFreshMap(ln, sampleMaxTTL),
		adders: NewFreshMap(ln, adderLifetime + 5),
		adderLifetime: adderLifetime,
	}
	return
}

func (m *FreshMetricsMap) Put(nodeName string, cpuUtil int, memoryUtil int) *MetricsSample {
	value := &MetricsSample{
		CpuUtil: cpuUtil,
		MemoryUtil: memoryUtil,
	}
	m.samples.Put(nodeName, value)
	return value
}

func (m *FreshMetricsMap) AddAdder(nodeName string, cpuUtil int, memoryUtil int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	newAdder := &MetricAdder{
		CpuUtil: cpuUtil,
		MemoryUtil: memoryUtil,
	}
	storedInt := m.adders.Get(nodeName, m.adderLifetime)
	if storedInt != nil {
		stored := storedInt.(*MetricAdder)
		// Stack any current adder on this one. This will give old
		// adder values a longer functional effect, but that
		// should be fine.
		newAdder.CpuUtil += stored.CpuUtil
		newAdder.MemoryUtil += stored.MemoryUtil
	}
	m.adders.Put(nodeName, newAdder)
}

func (m *FreshMetricsMap) processAdder(nodeName string, sample *MetricsSample ) *MetricsSample {
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
			CpuUtil: sample.CpuUtil + adder.CpuUtil,
			MemoryUtil: sample.MemoryUtil + adder.MemoryUtil,
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
func (m *FreshMetricsMap) GetOrPut(nodeName string, noOlderThan int64, getValues func()(int, int, error)) (*MetricsSample, error) {

	// Don't return value directly -- use m.Get afterward to include adders
	stored, err := m.samples.GetOrPut(nodeName, noOlderThan, func()(interface{}, error){
		cpu, memory, err := getValues()
		if err != nil {
			return nil, err
		}
		return &MetricsSample{
			CpuUtil: cpu,
			MemoryUtil: memory,
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