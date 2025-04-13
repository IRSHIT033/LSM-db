package memtable

import (
	"sync"
)

// MemTable represents an in-memory table that stores key-value pairs
type MemTable struct {
	mu    sync.RWMutex
	table map[string][]byte
	size  int
}

// NewMemTable creates a new MemTable instance
func NewMemTable() *MemTable {
	return &MemTable{
		table: make(map[string][]byte),
	}
}

// Put stores a key-value pair in the MemTable
func (mt *MemTable) Put(key, value []byte) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	keyStr := string(key)
	oldValue := mt.table[keyStr]

	// Update size
	mt.size -= len(oldValue)
	mt.size += len(value)

	mt.table[keyStr] = value
}

// Get retrieves a value for a given key
func (mt *MemTable) Get(key []byte) ([]byte, bool) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	value, found := mt.table[string(key)]
	return value, found
}

// Size returns the current size of the MemTable in bytes
func (mt *MemTable) Size() int {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.size
}

// Iterator returns an iterator over all key-value pairs
func (mt *MemTable) Iterator() *Iterator {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	keys := make([]string, 0, len(mt.table))
	for k := range mt.table {
		keys = append(keys, k)
	}

	return &Iterator{
		keys:   keys,
		table:  mt.table,
		cursor: 0,
	}
}

// Iterator provides a way to iterate over MemTable entries
type Iterator struct {
	keys   []string
	table  map[string][]byte
	cursor int
}

// Next advances the iterator to the next key-value pair
func (it *Iterator) Next() bool {
	it.cursor++
	return it.cursor <= len(it.keys)
}

// Key returns the current key
func (it *Iterator) Key() []byte {
	return []byte(it.keys[it.cursor-1])
}

// Value returns the current value
func (it *Iterator) Value() []byte {
	return it.table[it.keys[it.cursor-1]]
}
