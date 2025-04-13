package wal

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// WAL represents a Write-Ahead Log
type WAL struct {
	mu     sync.Mutex
	file   *os.File
	path   string
	offset int64
}

// NewWAL creates a new WAL instance
func NewWAL(dataDir string) (*WAL, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	path := filepath.Join(dataDir, "wal.log")
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	// Get current file size
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	return &WAL{
		file:   file,
		path:   path,
		offset: info.Size(),
	}, nil
}

// Write appends a key-value pair to the WAL
func (w *WAL) Write(key, value []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Write key length (4 bytes)
	if err := binary.Write(w.file, binary.LittleEndian, uint32(len(key))); err != nil {
		return err
	}

	// Write value length (4 bytes)
	if err := binary.Write(w.file, binary.LittleEndian, uint32(len(value))); err != nil {
		return err
	}

	// Write key
	if _, err := w.file.Write(key); err != nil {
		return err
	}

	// Write value
	if _, err := w.file.Write(value); err != nil {
		return err
	}

	// Flush to disk
	if err := w.file.Sync(); err != nil {
		return err
	}

	w.offset += 8 + int64(len(key)) + int64(len(value))
	return nil
}

// Close closes the WAL file
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

// Clear clears the WAL file
func (w *WAL) Clear() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Close(); err != nil {
		return err
	}

	file, err := os.Create(w.path)
	if err != nil {
		return err
	}

	w.file = file
	w.offset = 0
	return nil
}

// Recover reads the WAL file and returns all entries
func (w *WAL) Recover() ([]struct{ Key, Value []byte }, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Close current file and open for reading
	if err := w.file.Close(); err != nil {
		return nil, err
	}

	file, err := os.Open(w.path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var entries []struct{ Key, Value []byte }
	offset := int64(0)

	for {
		// Read key length
		var keyLen uint32
		if err := binary.Read(file, binary.LittleEndian, &keyLen); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// Read value length
		var valueLen uint32
		if err := binary.Read(file, binary.LittleEndian, &valueLen); err != nil {
			return nil, err
		}

		// Read key
		key := make([]byte, keyLen)
		if _, err := file.Read(key); err != nil {
			return nil, err
		}

		// Read value
		value := make([]byte, valueLen)
		if _, err := file.Read(value); err != nil {
			return nil, err
		}

		entries = append(entries, struct{ Key, Value []byte }{key, value})
		offset += 8 + int64(keyLen) + int64(valueLen)
	}

	// Reopen file for writing
	w.file, err = os.OpenFile(w.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	w.offset = offset
	return entries, nil
}
