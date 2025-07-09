package main

import (
	"bufio"
	"fmt"
	"unsafe"
	"os"
	"strings"
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
}

// where to read/write in memory for a particular row 
func rowSlot(table *Table, rowNum int) []byte {
	pageNum := rowNum / RowPerPage
	if table.Pages[pageNum] == nil {
		table.Pages[pageNum] = make([]byte, PageSize) 
	}
	rowOffset := rowNum % RowPerPage
	byteOffset := rowOffset * rowSize
	page := table.Pages[pageNum]
	return page[byteOffset : byteOffset+rowSize]
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

func newTable() *Table {
	table := &Table{numRows: 0}
	for i := 0; i < TableMaxPages; i++ {
		table.Pages[i] = nil
	}
	return table
}


func closeInputBuffer(inputBuffer *InputBuffer) {
	inputBuffer.buffer = ""
	inputBuffer.bufferLength = 0
	inputBuffer.inputLength = 0
}

func printPrompt() {
	fmt.Print("db > ")
}

func readInput(inputBuffer *InputBuffer) {
	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	if err != nil {
		if err.Error() == "EOF" {
			fmt.Println("Error reading input")
			os.Exit(1)
		}
		fmt.Println("Error reading input:", err)
		os.Exit(1)
	}
	// Remove the newline character
	line = strings.TrimSpace(line)
	inputBuffer.buffer = line
	inputBuffer.bufferLength = len(line)
	inputBuffer.inputLength = len(line)
}

func main() {
	inputBuffer := newInputBuffer()
	table := newTable()
	
	for {
		printPrompt()
		readInput(inputBuffer)

		if len(inputBuffer.buffer) > 0 && inputBuffer.buffer[0] == '.' {
			switch doMetaCommand(inputBuffer) {
			case MetaSuccess:
				continue
			case MetaUnrecognized:
				fmt.Printf("Unrecognized command '%s'\n", inputBuffer.buffer)
				continue
			}
		}

		var statement Statement

		switch PrepareStatement(inputBuffer, &statement) {
		case PrepareSuccess:
			result := executeStatement(&statement, table)
			if result == ExecuteSuccess {
				fmt.Println("Executed.")
			} else if result == ExecuteTableFull {
				fmt.Println("Error: Table full.")
			}
		case PrepareSyntaxError:
			fmt.Println("Syntax error. Could not parse statement.")
			continue
		case PrepareUnrecognized:
			fmt.Printf("Unrecognized keyword at start of '%s'.\n", inputBuffer.buffer)
			continue
		}
	}
}