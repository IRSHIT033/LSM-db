package lsmdb

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/IRSHIT033/lsmdb/memtable"
	"github.com/IRSHIT033/lsmdb/sstable"
	"github.com/IRSHIT033/lsmdb/wal"
	"gopkg.in/yaml.v3"
)

// DB represents the LSM tree database
type DB struct {
	mu       sync.RWMutex
	memtable *memtable.MemTable
	wal      *wal.WAL
	sstables []*sstable.SSTable
	config   *Config
	stopChan chan struct{} // Channel to stop background compaction
}

// Config holds database configuration
type Config struct {
	MemTableSize    int    `yaml:"mem_table_size"`   // Maximum size of memtable in bytes
	DataDir         string `yaml:"data_dir"`         // Directory to store SSTables
	MaxLevel        int    `yaml:"max_level"`        // Maximum number of levels
	LevelSize       int    `yaml:"level_size"`       // Maximum number of SSTables per level
	CompactInterval int    `yaml:"compact_interval"` // Interval between compaction runs in seconds
}

// NewDB creates a new LSM database instance
func NewDB(configPath string) (*DB, error) {
	// Read and parse YAML config
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %v", err)
	}

	var config Config
	if err := yaml.Unmarshal(configData, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %v", err)
	}

	db := &DB{
		config:   &config,
		sstables: make([]*sstable.SSTable, 0),
		stopChan: make(chan struct{}),
	}

	// Initialize WAL
	wal, err := wal.NewWAL(config.DataDir)
	if err != nil {
		return nil, err
	}
	db.wal = wal

	// Initialize MemTable
	db.memtable = memtable.NewMemTable()

	// Load existing SSTables
	levelDir := filepath.Join(config.DataDir, "level")
	if err := os.MkdirAll(levelDir, 0755); err != nil {
		return nil, err
	}

	// Read all level directories
	levelDirs, err := os.ReadDir(levelDir)
	if err != nil {
		return nil, err
	}

	for _, levelDir := range levelDirs {
		if !levelDir.IsDir() {
			continue
		}

		level, err := strconv.Atoi(levelDir.Name())
		if err != nil {
			continue // Skip invalid level directories
		}

		sst, err := sstable.NewSSTable(config.DataDir, level)
		if err != nil {
			return nil, fmt.Errorf("failed to load SSTable at level %d: %v", level, err)
		}
		db.sstables = append(db.sstables, sst)
	}

	// Recover from WAL
	entries, err := db.wal.Recover()
	if err != nil {
		return nil, fmt.Errorf("failed to recover from WAL: %v", err)
	}

	// Replay WAL entries to memtable
	for _, entry := range entries {
		db.memtable.Put(entry.Key, entry.Value)
	}

	// If memtable is not empty after recovery, check if it needs to be flushed
	if db.memtable.Size() > 0 && db.memtable.Size() >= db.config.MemTableSize {
		if err := db.flushMemTable(); err != nil {
			return nil, fmt.Errorf("failed to flush memtable after recovery: %v", err)
		}
	}

	// Start background compaction
	go db.startBackgroundCompaction()

	return db, nil
}

// Put stores a key-value pair in the database
func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Write to WAL first
	if err := db.wal.Write(key, value); err != nil {
		return err
	}

	// Write to memtable
	db.memtable.Put(key, value)

	// Check if memtable needs to be flushed
	if db.memtable.Size() >= db.config.MemTableSize {
		if err := db.flushMemTable(); err != nil {
			return err
		}
	}

	return nil
}

// Get retrieves a value for a given key
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// First check memtable
	if value, found := db.memtable.Get(key); found {
		return value, nil
	}

	// Then check SSTables in reverse order (newest first)
	for i := len(db.sstables) - 1; i >= 0; i-- {
		if value, found := db.sstables[i].Get(key); found {
			return value, nil
		}
	}

	return nil, nil
}

// Delete removes a key from the database
func (db *DB) Delete(key []byte) error {
	return db.Put(key, nil) // Tombstone value
}

// compactLevel performs compaction on a specific level
func (db *DB) compactLevel(level int) error {
	if level >= db.config.MaxLevel {
		return nil // No compaction needed for highest level
	}

	// Get SSTables for current level
	levelSSTables := make([]*sstable.SSTable, 0)
	for _, sst := range db.sstables {
		if strings.Contains(sst.GetPath(), fmt.Sprintf("level/%d", level)) {
			levelSSTables = append(levelSSTables, sst)
		}
	}

	// Check if compaction is needed
	if len(levelSSTables) < db.config.LevelSize {
		return nil
	}

	// Merge SSTables
	merged, err := sstable.Merge(levelSSTables, db.config.DataDir, level+1)
	if err != nil {
		return err
	}

	// Remove old SSTables
	for _, sst := range levelSSTables {
		if err := sst.Delete(); err != nil {
			return err
		}
	}

	// Update SSTables list
	newSSTables := make([]*sstable.SSTable, 0)
	for _, sst := range db.sstables {
		if !strings.Contains(sst.GetPath(), fmt.Sprintf("level/%d", level)) {
			newSSTables = append(newSSTables, sst)
		}
	}
	newSSTables = append(newSSTables, merged)
	db.sstables = newSSTables

	// Recursively compact next level
	return db.compactLevel(level + 1)
}

// flushMemTable writes the current memtable to disk as an SSTable
func (db *DB) flushMemTable() error {
	// Create new SSTable
	sst, err := sstable.NewSSTable(db.config.DataDir, 0)
	if err != nil {
		return err
	}

	// Convert memtable entries to SSTable entries
	entries := make([]sstable.Entry, 0)
	iter := db.memtable.Iterator()
	for iter.Next() {
		entries = append(entries, sstable.Entry{
			Key:   iter.Key(),
			Value: iter.Value(),
		})
	}

	// Write entries to SSTable
	if err := sst.Write(entries); err != nil {
		return err
	}

	// Add SSTable to list
	db.sstables = append(db.sstables, sst)

	// Clear memtable
	db.memtable = memtable.NewMemTable()

	// Clear WAL
	if err := db.wal.Clear(); err != nil {
		return err
	}

	return nil
}

// startBackgroundCompaction runs compaction at regular intervals
func (db *DB) startBackgroundCompaction() {
	ticker := time.NewTicker(time.Duration(db.config.CompactInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Run compaction on all levels
			for level := 0; level < db.config.MaxLevel; level++ {
				if err := db.compactLevel(level); err != nil {
					fmt.Printf("Error during compaction at level %d: %v\n", level, err)
				}
			}
		case <-db.stopChan:
			return
		}
	}
}

// Close closes the database and performs cleanup
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Stop background compaction
	close(db.stopChan)

	// Close WAL
	if err := db.wal.Close(); err != nil {
		return err
	}

	// Close all SSTables
	for _, sst := range db.sstables {
		if err := sst.Close(); err != nil {
			return err
		}
	}

	return nil
}
