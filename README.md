# DB-Go: A Simple B-Tree Database Implementation

## Overview

DB-Go is a simple, file-based database implementation written in Go that uses a B-tree data structure for efficient data storage and retrieval. This project demonstrates the core concepts of database internals, including:

- **B-tree indexing** for efficient key-value storage
- **Page-based storage** with caching
- **ACID-like properties** through proper file I/O handling
- **SQL-like interface** for data manipulation

## Features

### Data Structure
- **B-tree implementation** with both leaf and internal nodes
- **Page-based storage** (4KB pages) with memory caching
- **Binary search** for efficient key lookups
- **Automatic node splitting** when pages become full
- **Parent pointer tracking** for tree navigation

### Supported Operations
- **INSERT**: Add new records with unique IDs
- **SELECT**: Retrieve and display all records
- **Duplicate key detection**: Prevents inserting records with existing IDs
- **Tree visualization**: Display the B-tree structure

### Data Model
Each record contains:
- **ID**: 32-bit unsigned integer (primary key)
- **Username**: 32-byte string
- **Email**: 255-byte string

## How to Build and Run

### Prerequisites
- Go 1.16 or later

### Building
```bash
go build -o db-go main.go
```

### Running
```bash
./db-go <database_file>
```

Example:
```bash
./db-go mydb.db
```

## Usage

### Starting the Database
When you start the database, you'll see a prompt:
```
db >
```

### Available Commands

#### Insert Data
```sql
insert <id> <username> <email>
```
- `id`: Must be a positive integer (unique)
- `username`: Up to 32 characters
- `email`: Up to 255 characters

Examples:
```
db > insert 1 alice alice@example.com
db > insert 2 bob bob@example.com
db > insert 3 charlie charlie@example.com
```

#### Select Data
```sql
select
```
Displays all records in the database:
```
db > select
(1, alice, alice@example.com)
(2, bob, bob@example.com)
(3, charlie, charlie@example.com)
Executed.
```

#### Meta Commands

**.constants** - Display database constants:
```
db > .constants
Constants: 
Row Size: 291
Common Node Header Size: 6
Leaf Node Header Size: 14
Leaf Node Cell Size: 295
Leaf Node Space for Size: 4082
Leaf Node Max Cells: 13
```

**.btree** - Visualize the B-tree structure:
```
db > .btree
Tree:
- internal (size 1)
  - leaf (size 7)
    - 1
    - 2
    - 3
    - 4
    - 5
    - 6
    - 7
  - key 7
  - leaf (size 6)
    - 8
    - 9
    - 10
    - 11
    - 12
    - 13
```

**.exit** - Close the database and exit:
```
db > .exit
Goodbye!
```

## Technical Details

### B-tree Structure

#### Leaf Nodes
- Store actual data records
- Maintain sorted order by key
- Link to next leaf node for sequential access
- Maximum 13 cells per leaf node (configurable)

#### Internal Nodes
- Store child pointers and separator keys
- Maximum 3 keys per internal node (configurable)
- Use binary search for efficient child selection

#### Node Layout
```
Common Header (6 bytes):
- Node Type (1 byte)
- Is Root (1 byte) 
- Parent Pointer (4 bytes)

Leaf Node Header (14 bytes):
- Common Header (6 bytes)
- Number of Cells (4 bytes)
- Next Leaf Pointer (4 bytes)

Internal Node Header (14 bytes):
- Common Header (6 bytes)
- Number of Keys (4 bytes)
- Right Child Pointer (4 bytes)
```

### Page Management
- **Page Size**: 4KB (4096 bytes)
- **Cache**: In-memory page cache with lazy loading
- **Persistence**: Pages flushed to disk on close
- **File Format**: Raw binary with page-aligned data

### Performance Characteristics
- **Insert**: O(log n) average case
- **Select**: O(n) for full table scan, O(log n) for key lookup
- **Space**: Efficient with automatic node splitting
- **Memory**: Configurable page cache (default 100 pages)

## Error Handling

The database handles various error conditions:

- **Duplicate Key**: Attempting to insert a record with an existing ID
- **Syntax Error**: Malformed SQL statements
- **String Too Long**: Username or email exceeding size limits
- **Negative ID**: Attempting to use negative IDs
- **File Corruption**: Invalid page alignment or structure

## Limitations

- **Single Table**: Only supports one table per database file
- **Fixed Schema**: Record structure is hardcoded
- **No Transactions**: No ACID transaction support
- **No Indexes**: Only primary key indexing
- **No Updates/Deletes**: Only INSERT and SELECT operations
- **No Concurrency**: Single-threaded access only

## Educational Value

This implementation demonstrates:

1. **Database Internals**: How databases store and retrieve data
2. **B-tree Algorithms**: Tree balancing and node splitting
3. **Memory Management**: Page caching and buffer management
4. **File I/O**: Efficient disk-based storage
5. **Data Structures**: Complex tree implementations in Go

## Future Enhancements

Potential improvements could include:
- Multiple table support
- UPDATE and DELETE operations
- Secondary indexes
- Transaction support
- Concurrent access
- SQL parser for more complex queries
- Data compression
- Backup and recovery mechanisms

## License

This is an educational project. Feel free to use and modify for learning purposes. 