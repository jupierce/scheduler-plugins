/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package gravity

import (
	"sync"
	"time"
)

type item struct {
	v          interface{}
	creationTs int64
}

type FreshMap struct {
	items      map[string]*item
	maxTTL     int64
	l          sync.Mutex
	getOrLocks map[string]*sync.Mutex
}

func NewFreshMap(ln int, maxTTL int64) (m *FreshMap) {
	m = &FreshMap{
		items:      make(map[string]*item, ln),
		maxTTL:     maxTTL,
		getOrLocks: make(map[string]*sync.Mutex, ln),
	}
	go func() {
		// Periodically remove expired record if no one
		// is calling Get on them.
		for range time.Tick(time.Minute) {
			for k := range m.items {
				if m.Get(k, maxTTL) == nil {
					m.l.Lock()
					delete(m.items, k)
					m.l.Unlock()
				}
			}
		}
	}()
	return
}

func (m *FreshMap) Len() int {
	return len(m.items)
}

func (m *FreshMap) Put(k string, value interface{}) {
	m.l.Lock()
	defer m.l.Unlock()
	it := item{v: value, creationTs: time.Now().Unix()}
	m.items[k] = &it
}

func (m *FreshMap) Get(k string, noOlderThan int64) interface{} {
	m.l.Lock()
	defer m.l.Unlock()
	if it, ok := m.items[k]; ok {
		if noOlderThan == 0 || time.Now().Unix()-it.creationTs > noOlderThan {
			// The sample is too old for the caller
			return nil
		}
		return it.v
	}
	return nil
}

// GetOrPut helps make sure that if there are a queue of threads attempting
// to get a value, only one will make the actual heavyweight request for
// the latest value.
func (m *FreshMap) GetOrPut(k string, noOlderThan int64, getValue func() (interface{}, error)) (interface{}, error) {
	m.l.Lock()
	keyLock, ok := m.getOrLocks[k]
	if !ok {
		keyLock = &sync.Mutex{}
		m.getOrLocks[k] = keyLock
	}
	m.l.Unlock()

	keyLock.Lock()
	defer keyLock.Unlock()

	defer func() {
		m.l.Lock()
		delete(m.getOrLocks, k)
		m.l.Unlock()
	}()

	value := m.Get(k, noOlderThan)
	if value == nil {
		value, err := getValue()
		if err != nil {
			return nil, err
		}
		m.Put(k, value)
		return value, nil
	} else {
		return value, nil
	}
}
