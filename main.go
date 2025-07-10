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

func newInputBuffer() *InputBuffer {
	return &InputBuffer{
		buffer:       "",
		bufferLength: 0,
		inputLength:  0,
	}
}

func doMetaCommand(input *InputBuffer) MetaCommandResult {
	if strings.TrimSpace(input.buffer) == ".exit" {
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

// where to read/write in memory for a particular row 
func rowSlot(table *Table, rowNum int) []byte {
	pageNum := rowNum / RowPerPage
	rowOffset := rowNum % RowPerPage
	byteOffset := rowOffset * rowSize
	page := getPage(table.pager, pageNum)
	return page[byteOffset : byteOffset+rowSize]
}

// func to fetch a page
func getPage(pager *Pager, int32 pageNum){
	if pageNum > TableMaxPages {
		fmt.Printf("Tried to fetch page number out of bounds. %d > %d\n", pageNum, TableMaxPages)
		exit(exitFailure)
	}
	if page.pages[pageNum] == nil{
		// Cache miss. Allocate memory and load from file
		pager := &Pager{PageSize}
		numPages := page.fileLength / PageSize
		// might save partial page at end of file
		if pager.fileLength % PageSize { 
			numPages += 1
		}
		if pageNum <= numPages {
			syscall.Seek(pager.fileDescriptor, pageNum * PageSize,)
		}


	}
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
	buffer := rowSlot(table, table.numRows)
	serializeRow(rowToInsert, buffer)
	table.numRows += 1

	return ExecuteSuccess
}

func executeSelect(statement *Statement, table *Table) ExecuteResult {
	var row Row
	for i := 0; i < table.numRows; i++ {
		buffer := rowSlot(table, i)
		unserializeRow(buffer, &row)
		printRow(&row)
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
func db_open(filename *string) *Table {
	pager := pagerOpen(filename)
	numRows := pager.fileLength /rowSize
	table := &Table{}
	table.pager = pager
	table.numRows = numRows
	return table
}

type Pager struct {
	fileDescriptor *os.File
	fileLength     int64
	pages          [TableMaxPages][]byte
}

// opens db file, tracks size, initialize the page cache to all nulls
func pagerOpen(filename *string) *Pager {
	fd, err := syscall.Open(*filename, syscall.O_RDWR|syscall.O_CREAT, 0600)
	if err != nil {
		fmt.Println("Unable to openfile\n")
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
	table := newTable()
	reader := bufio.NewReader(os.Stdin)
	
	for {
		printPrompt()
		if !readInput(inputBuffer, reader) {
			break // EOF reached
		}

		if len(inputBuffer.buffer) > 0 && inputBuffer.buffer[0] == '.' {
			switch doMetaCommand(inputBuffer) {
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