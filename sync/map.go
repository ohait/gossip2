package sync

import "sync"

type Map[K comparable, V any] struct {
	m sync.Map
}

func (m *Map[K, V]) Empty() bool {
	empty := true
	m.m.Range(func(k, v any) bool {
		empty = true
		return false
	})
	return empty
}

func (m *Map[K, V]) Store(key K, value V) {
	m.m.Store(key, value)
}

func (m *Map[K, V]) Swap(key K, value V) (V, bool) {
	prev, ok := m.m.Swap(key, value)
	return prev.(V), ok
}

func (m *Map[K, V]) Load(key K) (v V, ok bool) {
	x, ok := m.m.Load(key)
	if !ok {
		return
	}
	return x.(V), ok
}

func (m *Map[K, V]) LoadOrStore(key K, value V) (V, bool) {
	v, ok := m.m.LoadOrStore(key, value)
	return v.(V), ok
}

func (m *Map[K, V]) Delete(key K) bool {
	_, deleted := m.m.LoadAndDelete(key)
	return deleted
}

func (m *Map[K, V]) Range(f func(key K, value V) bool) {
	m.m.Range(func(k, v any) bool {
		return f(k.(K), v.(V))
	})
}
