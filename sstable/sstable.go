package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// SSTable represents a Sorted String Table
type SSTable struct {
	mu       sync.RWMutex
	path     string
	index    map[string]int64
	dataFile *os.File
}

// NewSSTable creates a new SSTable
func NewSSTable(dataDir string, level int) (*SSTable, error) {
	path := filepath.Join(dataDir, "level", fmt.Sprint(level))
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	dataFilePath := filepath.Join(path, "data.sst")

	// Open file in append mode if it exists, create if it doesn't
	file, err := os.OpenFile(dataFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	sst := &SSTable{
		path:     path,
		index:    make(map[string]int64),
		dataFile: file,
	}

	// If file exists and has content, load the index
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	if info.Size() > 0 {
		if err := sst.loadIndex(); err != nil {
			file.Close()
			return nil, err
		}
	}

	return sst, nil
}

// loadIndex loads the index from the existing SSTable file
func (s *SSTable) loadIndex() error {
	offset := int64(0)
	for {
		// Read key length
		var keyLen uint32
		if err := binary.Read(s.dataFile, binary.LittleEndian, &keyLen); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// Read value length
		var valueLen uint32
		if err := binary.Read(s.dataFile, binary.LittleEndian, &valueLen); err != nil {
			return err
		}

		// Read key
		key := make([]byte, keyLen)
		if _, err := s.dataFile.Read(key); err != nil {
			return err
		}

		// Skip value (we only need the key for the index)
		if _, err := s.dataFile.Seek(int64(valueLen), 1); err != nil {
			return err
		}

		// Store key and offset in index
		s.index[string(key)] = offset

		// Update offset for next entry
		offset += 8 + int64(keyLen) + int64(valueLen)
	}

	return nil
}

// Write writes a batch of key-value pairs to the SSTable
func (s *SSTable) Write(entries []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Sort entries by key
	sort.Slice(entries, func(i, j int) bool {
		return string(entries[i].Key) < string(entries[j].Key)
	})

	// Write entries to data file
	for _, entry := range entries {
		offset, err := s.writeEntry(entry)
		if err != nil {
			return err
		}
		s.index[string(entry.Key)] = offset
	}

	return nil
}

// Get retrieves a value for a given key
func (s *SSTable) Get(key []byte) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	offset, found := s.index[string(key)]
	if !found {
		return nil, false
	}

	entry, err := s.readEntry(offset)
	if err != nil {
		return nil, false
	}

	return entry.Value, true
}

// writeEntry writes a single entry to the data file
func (s *SSTable) writeEntry(entry Entry) (int64, error) {
	info, err := s.dataFile.Stat()
	if err != nil {
		return 0, err
	}
	offset := info.Size()

	// Write key length (4 bytes)
	if err := binary.Write(s.dataFile, binary.LittleEndian, uint32(len(entry.Key))); err != nil {
		return 0, err
	}

	// Write value length (4 bytes)
	if err := binary.Write(s.dataFile, binary.LittleEndian, uint32(len(entry.Value))); err != nil {
		return 0, err
	}

	// Write key
	if _, err := s.dataFile.Write(entry.Key); err != nil {
		return 0, err
	}

	// Write value
	if _, err := s.dataFile.Write(entry.Value); err != nil {
		return 0, err
	}

	return offset, nil
}

// readEntry reads a single entry from the data file
func (s *SSTable) readEntry(offset int64) (Entry, error) {
	if _, err := s.dataFile.Seek(offset, 0); err != nil {
		return Entry{}, err
	}

	var keyLen, valueLen uint32
	if err := binary.Read(s.dataFile, binary.LittleEndian, &keyLen); err != nil {
		return Entry{}, err
	}
	if err := binary.Read(s.dataFile, binary.LittleEndian, &valueLen); err != nil {
		return Entry{}, err
	}

	key := make([]byte, keyLen)
	value := make([]byte, valueLen)

	if _, err := s.dataFile.Read(key); err != nil {
		return Entry{}, err
	}
	if _, err := s.dataFile.Read(value); err != nil {
		return Entry{}, err
	}

	return Entry{Key: key, Value: value}, nil
}

// Entry represents a key-value pair in the SSTable
type Entry struct {
	Key   []byte
	Value []byte
}

// Close closes the SSTable
func (s *SSTable) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dataFile.Close()
}

// Merge merges multiple SSTables into a new SSTable
func Merge(sstables []*SSTable, dataDir string, level int) (*SSTable, error) {
	// Create new SSTable for merged data
	merged, err := NewSSTable(dataDir, level)
	if err != nil {
		return nil, err
	}

	// Collect all entries from all SSTables
	var allEntries []Entry
	for _, sst := range sstables {
		entries, err := sst.getAllEntries()
		if err != nil {
			return nil, err
		}
		allEntries = append(allEntries, entries...)
	}

	// Sort all entries by key
	sort.Slice(allEntries, func(i, j int) bool {
		return string(allEntries[i].Key) < string(allEntries[j].Key)
	})

	// Remove duplicates (keep latest version)
	if len(allEntries) > 0 {
		uniqueEntries := []Entry{allEntries[0]}
		for i := 1; i < len(allEntries); i++ {
			if string(allEntries[i].Key) != string(allEntries[i-1].Key) {
				uniqueEntries = append(uniqueEntries, allEntries[i])
			}
		}
		allEntries = uniqueEntries
	}

	// Write merged entries to new SSTable
	if err := merged.Write(allEntries); err != nil {
		return nil, err
	}

	return merged, nil
}

// getAllEntries returns all entries in the SSTable
func (s *SSTable) getAllEntries() ([]Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var entries []Entry
	offset := int64(0)

	for {
		entry, err := s.readEntry(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		entries = append(entries, entry)
		offset += 8 + int64(len(entry.Key)) + int64(len(entry.Value))
	}

	return entries, nil
}

// Delete removes the SSTable files
func (s *SSTable) Delete() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.dataFile.Close(); err != nil {
		return err
	}

	return os.RemoveAll(s.path)
}

// GetPath returns the path of the SSTable
func (s *SSTable) GetPath() string {
	return s.path
}
