# LSM Tree Database

A simple key-value database implementation using Log-Structured Merge (LSM) tree in Go.

## Features

- In-memory MemTable for fast writes
- Write-Ahead Log (WAL) for durability
- Sorted String Tables (SSTables) for persistent storage
- Basic compaction support
- Thread-safe operations

## Architecture

The database consists of several components:

1. **MemTable**: An in-memory table that stores recent writes
2. **WAL**: Write-Ahead Log for durability
3. **SSTable**: Sorted String Tables for persistent storage
4. **Compaction**: Merges SSTables to maintain performance

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/yourusername/lsmdb.git
   cd lsmdb
   ```

2. Build the project:
   ```bash
   make build
   ```

## Usage

The database provides a simple CLI interface. After building, you can run it with:

```bash
make run
```

Or directly:

```bash
./lsmdb
```

### Basic Operations

The CLI supports the following commands:

- `put <key> <value>` - Store a key-value pair
- `get <key>` - Retrieve a value by key
- `delete <key>` - Remove a key-value pair
- `exit` - Exit the CLI

## Project Structure

```
.
├── cmd/
│   └── cli/          # Command-line interface
├── memtable/         # In-memory table implementation
├── sstable/          # Sorted String Table implementation
├── wal/              # Write-Ahead Log implementation
├── data/             # Data directory (ignored in git)
├── go.mod            # Go module definition
├── Makefile          # Build and run commands
└── README.md         # This file
```

## Implementation Details

### MemTable

- Uses a map for O(1) lookups
- Tracks size for flush triggers
- Thread-safe with mutex

### WAL

- Binary format with length-prefixed records
- Appends-only for durability
- Used for recovery

### SSTable

- Sorted key-value pairs
- Index for fast lookups
- Binary format with length-prefixed records

## Development

### Building

```bash
make build
```

### Running

```bash
make run
```

### Cleaning

```bash
make clean
```

## Future Improvements

- Implement proper compaction strategy
- Add bloom filters for faster lookups
- Add compression
- Add range scans
- Add transaction support
- Add backup/restore functionality

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

[Add your license here]
# LSM-db
