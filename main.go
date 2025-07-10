package main

import (
	"bufio"
	"fmt"
	"io"
	"unsafe"
	"os"
	"strings"
	"syscall"
)

type MetaCommandResult int 

const (
	MetaSuccess MetaCommandResult = iota 
	MetaUnrecognized
)
type PrepareResult int 

const(
	PrepareSuccess PrepareResult = iota
	PrepareUnrecognized
	PrepareSyntaxError
	PrepareNegativeID
	PrepareStringTooLong
)

type StatementType int 

const (
	StatementInsert StatementType = iota
	StatementSelect
)

type ExecuteResult int

const (
	ExecuteSuccess ExecuteResult = iota
	ExecuteTableFull
)

const (
	columnUserSize = 32
	columnEmailSize = 255
)
type Row struct {
	id       uint32
	username [columnUserSize]byte
	email    [columnEmailSize]byte
}

type Statement struct { 
	rowToInsert Row
	Type        StatementType
}

type InputBuffer struct {
	buffer       string
	bufferLength int
	inputLength  int
}

type Cursor struct {
	table *Table
	rowNum int
	endOfTable bool
}

// create new cursors
func tableStart(table *Table) *Cursor {
	return &Cursor{
		table: table,
		rowNum:	0,
		endOfTable: table.numRows ==0,
	}
}

func tableEnd(table *Table) *Cursor {
	return &Cursor{
		table: table,
		rowNum:	table.numRows,
		endOfTable: true,
	}
}

//(was rowSlot) where to read/write in memory for a particular row 
// returns pointer to position described by cursor
func cursorValue(cursor *Cursor) []byte {
	rowNum := cursor.rowNum
	pageNum := rowNum / RowPerPage
	rowOffset := rowNum % RowPerPage
	byteOffset := rowOffset * rowSize
	page := getPage(cursor.table.pager, pageNum)
	return page[byteOffset : byteOffset+rowSize]
}

func cursorAdvance(cursor *Cursor) {
	cursor.rowNum +=1
	if cursor.rowNum >= cursor.table.numRows {
		cursor.endOfTable = true
	}
}

func newInputBuffer() *InputBuffer {
	return &InputBuffer{
		buffer:       "",
		bufferLength: 0,
		inputLength:  0,
	}
}


func doMetaCommand(input *InputBuffer, table *Table) MetaCommandResult {
	if strings.TrimSpace(input.buffer) == ".exit" {
		dbClose(table)
		closeInputBuffer(input)
		fmt.Println("Goodbye!")
		os.Exit(0)
		return MetaSuccess
	} else {
		return MetaUnrecognized
	}
}

const (
	idSize = int(unsafe.Sizeof(Row{}.id))
	userSize = int(unsafe.Sizeof(Row{}.username))
	emailSize = int(unsafe.Sizeof(Row{}.email))
	idOffset = 0
	userOffset = idOffset + idSize
	emailOffset = userOffset + userSize
	rowSize = idSize + userSize + emailSize
)

// convert to and from compact representation 
func serializeRow(source *Row, destination []byte) {
	copy(destination[idOffset:idOffset+idSize], (*[idSize]byte)(unsafe.Pointer(&source.id))[:])

	copy(destination[userOffset:userOffset+userSize], (*[userSize]byte)(unsafe.Pointer(&source.username))[:])

	copy(destination[emailOffset:emailOffset+emailSize], (*[emailSize]byte)(unsafe.Pointer(&source.email))[:])
}

func unserializeRow(source []byte, destination *Row) {
	idBytes := source[idOffset : idOffset+idSize]
	destination.id = *(*uint32)(unsafe.Pointer(&idBytes[0]))

	copy(destination.username[:], source[userOffset:userOffset+userSize])

	copy(destination.email[:], source[emailOffset:emailOffset+emailSize])
}

// Table structure that points to pages of rows and tracks # of rows
const (
	PageSize = 4096
	TableMaxPages = 100
	RowPerPage = PageSize / rowSize
	tableMaxRows = int(RowPerPage) * TableMaxPages
)

type Table struct {
	numRows  int
	Pages    [TableMaxPages][]byte 
	pager    *Pager
}


// func to fetch a page
func getPage(pager *Pager, pageNum int) []byte {
	if pageNum >= TableMaxPages {
		fmt.Printf("Tried to fetch page number out of bounds. %d >= %d\n", pageNum, TableMaxPages)
		os.Exit(1)
	}
	if pager.pages[pageNum] == nil {
		// Cache miss. Allocate memory and load from file
		page := make([]byte, PageSize)
		numPages := pager.fileLength / PageSize
		// might save partial page at end of file
		if pager.fileLength%PageSize != 0 {
			numPages += 1
		}
		if pageNum < int(numPages) {
			_, err := pager.fileDescriptor.Seek(int64(pageNum*PageSize), io.SeekStart)
			if err != nil {
				fmt.Printf("Error seeking file: %v\n", err)
				os.Exit(1)
			}
			bytesRead, err := pager.fileDescriptor.Read(page)
			if err != nil && err != io.EOF {
				fmt.Printf("Error reading file: %v\n", err)
				os.Exit(1)
			}
			// If we read less than PageSize, pad with zeros
			if bytesRead < PageSize {
				for i := bytesRead; i < PageSize; i++ {
					page[i] = 0
				}
			}
		}
		pager.pages[pageNum] = page
	}
	return pager.pages[pageNum]
}

func PrepareStatement(input *InputBuffer, statement *Statement) PrepareResult {
	if strings.HasPrefix(input.buffer, "insert"){
		statement.Type = StatementInsert
		var id int
		var username, email string
		argsAssigned, err := fmt.Sscanf(input.buffer, "insert %d %s %s", &id, &username, &email)
		if err != nil || argsAssigned != 3 {
			return PrepareSyntaxError
		}
		if id < 0 {
			return PrepareNegativeID
		}
		if len(username) > columnUserSize {
			return PrepareStringTooLong
		}
		if len(email) > columnEmailSize {
			return PrepareStringTooLong
		}
		statement.rowToInsert.id = uint32(id)
		copy(statement.rowToInsert.username[:], []byte(username))
		copy(statement.rowToInsert.email[:], []byte(email))
		return PrepareSuccess
	}
	if strings.HasPrefix(input.buffer, "select"){
		statement.Type = StatementSelect
		return PrepareSuccess
	}
	return PrepareUnrecognized
}

func executeInsert(statement *Statement, table *Table) ExecuteResult {
	if table.numRows >= tableMaxRows {
		return ExecuteTableFull
	}
	rowToInsert := &statement.rowToInsert
	cursor := tableEnd(table)
	buffer := cursorValue(cursor)
	serializeRow(rowToInsert, buffer)
	table.numRows += 1
	fmt.Printf("[DEBUG] Inserted row. numRows=%d\n", table.numRows)
	return ExecuteSuccess
}

func executeSelect(statement *Statement, table *Table) ExecuteResult {
	fmt.Printf("[DEBUG] Selecting. numRows=%d\n", table.numRows)
	var row Row
	cursor := tableStart(table)
	for !cursor.endOfTable {
		buffer := cursorValue(cursor)
		unserializeRow(buffer, &row)
		printRow(&row)
		cursorAdvance(cursor)
	}
	return ExecuteSuccess
}

func printRow(row *Row) {
	fmt.Printf("(%d, %s, %s)\n", 
		row.id, 
		strings.TrimRight(string(row.username[:]), "\x00"),
		strings.TrimRight(string(row.email[:]), "\x00"))
	os.Stdout.Sync()
}

func executeStatement(statement *Statement, table *Table) ExecuteResult {
	switch statement.Type {
	case StatementInsert:
		return executeInsert(statement, table)
	case StatementSelect:
		return executeSelect(statement, table)
	default:
		return ExecuteSuccess
	}
}

// open a connection (open the db file, initialize pager&table)
func dbOpen(filename *string) *Table {
	pager := pagerOpen(filename)
	numRows := int(pager.fileLength) / rowSize
	table := &Table{}
	table.pager = pager
	table.numRows = numRows
	fmt.Printf("[DEBUG] Opened DB: fileLength=%d, numRows=%d\n", pager.fileLength, numRows)
	return table
}

// flushes page cache to disk, closes the db file, frees memory for Pager and Table
func dbClose(table *Table) {
	pager := table.pager
	numFullPages := int(table.numRows) / RowPerPage

	for i := 0; i < numFullPages; i++ {
		if pager.pages[i] == nil {
			continue
		}
		pagerFlush(pager, i, PageSize)
		pager.pages[i] = nil 
	}
	// chance of a partial page to write to the end of file 
	// no need after B Tree implementation
	numAddrows := table.numRows % RowPerPage
	if numAddrows > 0 {
		pageNum := numFullPages
		if pager.pages[pageNum] != nil {
			pagerFlush(pager, pageNum, numAddrows * rowSize)
			// no need to free. Go does it for us
			pager.pages[pageNum] = nil
		}
	}
	err := pager.fileDescriptor.Close()
	if err != nil {
		fmt.Printf("Error closing db file: %v\n", err)
		os.Exit(1)
	}
	fileInfo, statErr := os.Stat(pager.filename)
	if statErr == nil {
		fmt.Printf("[DEBUG] Closed DB: file size is %d bytes\n", fileInfo.Size())
	}
	for i := 0; i < TableMaxPages; i++ {
		page := pager.pages[i]
		if page != nil { 
			pager.pages[i] = nil
		}
	}
}
// will ensure flush
func pagerFlush(pager *Pager, pageNum int, size int) {
	if pager.pages[pageNum] == nil {
		fmt.Printf("Tried to flush a null page\n")
		os.Exit(1)
	}
	_, err := pager.fileDescriptor.Seek(int64(pageNum*PageSize), io.SeekStart)
	if err != nil {
		fmt.Printf("Error seeking file: %v\n", err)
		os.Exit(1)
	}
	bytesWritten, err := pager.fileDescriptor.Write(pager.pages[pageNum][:size])
	if err != nil {
		fmt.Printf("Error writing file: %v\n", err)
		os.Exit(1)
	}
	if bytesWritten != size {
		fmt.Printf("Error: partial write. Expected %d, got %d\n", size, bytesWritten)
		os.Exit(1)
	}
}

type Pager struct {
	fileDescriptor *os.File
	fileLength     int64
	pages          [TableMaxPages][]byte
	filename       string
}

// opens db file, tracks size, initialize the page cache to all nulls
func pagerOpen(filename *string) *Pager {
	fd, err := syscall.Open(*filename, syscall.O_RDWR|syscall.O_CREAT, 0600)
	if err != nil {
		fmt.Println("Unable to openfile")
		os.Exit(1)
	}
	fileLength, err := syscall.Seek(fd, 0, io.SeekEnd)
	if err != nil {
		fmt.Println("Error seeking:", err)
		os.Exit(1)
	}
	pager := &Pager{}
	pager.fileDescriptor = os.NewFile(uintptr(fd), *filename)
	pager.fileLength = fileLength
	pager.filename = *filename
	for i := 0; i < TableMaxPages; i++ {
		pager.pages[i] = nil
	}
	return pager
}

func newTable() *Table {
	return &Table{
		numRows: 0,
		Pages:   [TableMaxPages][]byte{},
		pager:   nil,
	}
}

func closeInputBuffer(inputBuffer *InputBuffer) {
	inputBuffer.buffer = ""
	inputBuffer.bufferLength = 0
	inputBuffer.inputLength = 0
}

func printPrompt() {
	fmt.Print("db > ")
	os.Stdout.Sync()
}

func readInput(inputBuffer *InputBuffer, reader *bufio.Reader) bool {
	line, err := reader.ReadString('\n')
	if err != nil {
		if err.Error() == "EOF" {
			return false // EOF reached
		}
		fmt.Println("Error reading input:", err)
		os.Exit(1)
	}
	// Remove the newline character
	line = strings.TrimSpace(line)
	inputBuffer.buffer = line
	inputBuffer.bufferLength = len(line)
	inputBuffer.inputLength = len(line)
	return true // Indicate success
}

func main() {
	inputBuffer := newInputBuffer()
	if len(os.Args) < 2 {
		fmt.Printf("Must supply a db filename.\n")
		os.Exit(1)
	}
	filename := os.Args[1]
	table := dbOpen(&filename)
	reader := bufio.NewReader(os.Stdin)
	
	for {
		printPrompt()
		if !readInput(inputBuffer, reader) {
			break // EOF reached
		}

		if len(inputBuffer.buffer) > 0 && inputBuffer.buffer[0] == '.' {
			switch doMetaCommand(inputBuffer, table) {
			case MetaSuccess:
				continue
			case MetaUnrecognized:
				fmt.Printf("Unrecognized command '%s'\n", inputBuffer.buffer)
				os.Stdout.Sync()
				continue
			}
		}

		var statement Statement

		switch PrepareStatement(inputBuffer, &statement) {
		case PrepareSuccess:
			result := executeStatement(&statement, table)
			if result == ExecuteSuccess {
				fmt.Println("Executed.")
				os.Stdout.Sync()
			} else if result == ExecuteTableFull {
				fmt.Println("Error: Table full.")
				os.Stdout.Sync()
			}
		case PrepareNegativeID:
			fmt.Println("ID must be positive.")
			os.Stdout.Sync()
			continue
		case PrepareStringTooLong:
			fmt.Println("String is too long.")
			os.Stdout.Sync()
			continue
		case PrepareSyntaxError:
			fmt.Println("Syntax error. Could not parse statement.")
			os.Stdout.Sync()
			continue
		case PrepareUnrecognized:
			fmt.Printf("Unrecognized keyword at start of '%s'.\n", inputBuffer.buffer)
			os.Stdout.Sync()
			continue
		}
	}
}