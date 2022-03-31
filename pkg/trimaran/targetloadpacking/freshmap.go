package targetloadpacking

import (
	"fmt"
	"sync"
	"time"
)

type MetricsSample struct {
	CpuUtil    int
	MemoryUtil int
	lastPut    int64
}
type TTLMap struct {
	m map[string]*MetricsSample
	maxTTL int
	l sync.Mutex
}

func FreshMap(ln int, maxTTL int) (m *TTLMap) {
	m = &TTLMap{m: make(map[string]*MetricsSample, ln), maxTTL: maxTTL}
	go func() {
		// Periodically remove expired record if no one
		// is calling Get on them.
		for range time.Tick(time.Minute) {
			fmt.Println("Clearing..")
			for k, _ := range m.m {
				m.Get(k)
			}
		}
	}()
	return
}

func (m *TTLMap) Len() int {
	return len(m.m)
}

func (m *TTLMap) Put(k string, cpuUtil int, memoryUtil int) {
	m.l.Lock()
	defer m.l.Unlock()
	it := MetricsSample{CpuUtil: cpuUtil, MemoryUtil: memoryUtil, lastPut: time.Now().Unix()}
	m.m[k] = &it
}

func (m *TTLMap) Get(k string) *MetricsSample {
	m.l.Lock()
	defer m.l.Unlock()
	if it, ok := m.m[k]; ok {
		if time.Now().Unix() - it.lastPut > int64(m.maxTTL) {
			// The sample is too old, remove old record and return nil
			delete(m.m, k)
			return nil
		}
		return it
	}
	return nil
}
