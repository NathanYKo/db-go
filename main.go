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
	ExecuteDuplicateKey
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
	pageNum uint32
	cellNum uint32
	endOfTable bool
}

// Node Layout
type NodeType int

const (
	NodeInternal NodeType = iota
	NodeLeaf
)

const (
	NodeTypeSize = 1
	NodeTypeOffset = 0
	IsRootSize = 1
	IsRootOffset = NodeTypeSize
	ParentPointerSize = 4
	ParentPointerOffset = IsRootOffset + IsRootSize
	CommonNodeHeaderSize = NodeTypeSize + IsRootSize + ParentPointerSize
)

// Leaf Node Header Layout 
const (
	LeafNodeNumCellsSize = 4
	LeafNodeNumCellsOffset = CommonNodeHeaderSize
	LeafNodeHeaderSize = CommonNodeHeaderSize + LeafNodeNumCellsSize
)

// Leaf Node Sizes
const (
	leafNodeRightSplitCount = (LeafNodeMaxCells + 1) / 2
	leafNodeLeftSplitCount = (LeafNodeMaxCells + 1) - leafNodeRightSplitCount
 )

// Internal Node header Layout 
const (
	internalNodeNumKeysSize = 4
	internalNodeNumKeysOffset = CommonNodeHeaderSize
	internalNodeRightChildSize = 4
	internalNodeRightChildOffset = internalNodeNumKeysOffset + internalNodeNumKeysSize
	internalNodeHeaderSize = CommonNodeHeaderSize + internalNodeNumKeysSize + internalNodeRightChildSize
)

// Internal Node Body Layout 
const ( 
	internalNodeKeySize = 4
	internalNodeChildSize = 4
	internalNodeCellSize = internalNodeChildSize + internalNodeKeySize
)

// Access to nodes 
func leafNodeNumCells(node unsafe.Pointer) *uint32 {
	return (*uint32)(unsafe.Add(node, LeafNodeNumCellsOffset))
}
func leafNodeCell(node unsafe.Pointer, cellNum uint32) unsafe.Pointer {
	return unsafe.Add(node, LeafNodeHeaderSize+int(cellNum)*LeafNodeCellSize)
}

func leafNodeKey(node unsafe.Pointer, cellNum uint32) *uint32 {
	return (*uint32)(leafNodeCell(node, cellNum))
}

func leafNodeValue(node unsafe.Pointer, cellNum uint32) unsafe.Pointer {
	return unsafe.Add(leafNodeCell(node, cellNum), LeafNodeKeySize)
}

func initializeLeafNode(node unsafe.Pointer) {
	setNodeType(node, NodeLeaf)
	setNodeRoot(node, false)
	*leafNodeNumCells(node) = 0
}

func initializeInternalNode(node unsafe.Pointer) { 
	setNodeType(node, NodeInternal)
	setNodeRoot(node, false)
	*internalNodeNumKeys(node) = 0
}
// create new cursors
func tableStart(table *Table) *Cursor {
	cursor := &Cursor{
		table: table,
		cellNum: 0,
		pageNum: table.RootPageNum,
		endOfTable: false,
	}

	rootNode := getPage(table.pager, int(table.RootPageNum))
	numCells := *leafNodeNumCells(unsafe.Pointer(&rootNode[0]))
	cursor.endOfTable = (numCells == 0)

	return cursor
}

// Return postion of given key 
// If no key present, return position where it should be inserted
func tableFind(table *Table, key uint32) *Cursor {
	rootPageNum := table.RootPageNum
	rootNode := getPage(table.pager, int(rootPageNum))
	
	if getNodeType(unsafe.Pointer(&rootNode[0])) == NodeLeaf {
		return leafNodeFind(table, rootPageNum, key)
	} else {
		fmt.Printf("Need to implement searching an internal node\n")
		os.Exit(1)
		return nil // This will never be reached due to os.Exit(1)
	}
}

func leafNodeFind(table *Table, pageNum uint32, key uint32) *Cursor { 
	node := getPage(table.pager, int(pageNum))
	nodePtr := unsafe.Pointer(&node[0])
	numCells := *leafNodeNumCells(nodePtr)

	cursor := &Cursor{
		table: table,
		pageNum: pageNum,
		cellNum: 0,
		endOfTable: false,
	}
	
	// Binary search to find the key
	onePastMaxIndex := numCells
	for cursor.cellNum < onePastMaxIndex {
		indexOnRight := (cursor.cellNum + onePastMaxIndex) / 2
		keyAtIndex := *leafNodeKey(nodePtr, indexOnRight)
		
		if key == keyAtIndex {
			cursor.cellNum = indexOnRight
			return cursor
		}
		if key < keyAtIndex {
			onePastMaxIndex = indexOnRight
		} else {
			cursor.cellNum = indexOnRight + 1
		}
	}
	
	return cursor
}

func getNodeType(node unsafe.Pointer) NodeType {
	value := *(*uint8)(unsafe.Add(node, NodeTypeOffset))
	return NodeType(value)
}

func setNodeType(node unsafe.Pointer, nodeType NodeType) {
	value := uint8(nodeType)
	*(*uint8)(unsafe.Add(node, NodeTypeOffset)) = value
} 

//(was rowSlot) where to read/write in memory for a particular row 
// returns pointer to position described by cursor
func cursorValue(cursor *Cursor) []byte {
	page := getPage(cursor.table.pager, int(cursor.pageNum))
	return page
}

func cursorAdvance(cursor *Cursor) {
	cursor.cellNum += 1
	page := getPage(cursor.table.pager, int(cursor.pageNum))
	numCells := *leafNodeNumCells(unsafe.Pointer(&page[0]))
	if cursor.cellNum >= numCells {
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

func printConstants() {
	fmt.Printf("Row Size: %d\n", rowSize)
	fmt.Printf("Common Node Header Size: %d\n", CommonNodeHeaderSize)
	fmt.Printf("Leaf Node Header Size: %d\n", LeafNodeHeaderSize)
	fmt.Printf("Leaf Node Cell Size: %d\n", LeafNodeCellSize)
	fmt.Printf("Leaf Node Space for Size: %d\n", LeafNodeSpaceForCells)
	fmt.Printf("Leaf Node Max Cells: %d\n", LeafNodeMaxCells)

}

func doMetaCommand(input *InputBuffer, table *Table) MetaCommandResult {
	trimmed := strings.TrimSpace(input.buffer)
	
	if trimmed == ".exit" {
		dbClose(table)
		closeInputBuffer(input)
		fmt.Println("Goodbye!")
		os.Exit(0)
		return MetaSuccess
	} else if trimmed == ".constants" {
		fmt.Println("Constants: \n")
		printConstants()
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

// Leaf Node Body Layout 
const (
	LeafNodeKeySize = 4
	LeafNodeKeyOffset = 0 
	LeafNodeValueSize = rowSize
	LeafnodeValueOffset = LeafNodeKeyOffset + LeafNodeKeySize
	LeafNodeCellSize = LeafNodeKeySize + LeafNodeValueSize
	LeafNodeSpaceForCells = PageSize - LeafNodeHeaderSize
	LeafNodeMaxCells = LeafNodeSpaceForCells / LeafNodeCellSize
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
	//no need to track num of rows
	// RowPerPage = PageSize / rowSize
	// tableMaxRows = int(RowPerPage) * TableMaxPages
)

type Table struct {
	RootPageNum uint32
	Pages       [TableMaxPages][]byte 
	pager       *Pager
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

		if pageNum >= int(pager.numPages) {
			pager.numPages = uint32(pageNum + 1)
		}
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
	node := getPage(table.pager, int(table.RootPageNum))
	nodePtr := unsafe.Pointer(&node[0])
	numCells := *leafNodeNumCells(nodePtr)
	

	rowToInsert := &statement.rowToInsert
	keyToInsert := rowToInsert.id
	cursor := tableFind(table, keyToInsert)
	fmt.Printf("[DEBUG] Inserting key %d, cursor.cellNum=%d, numCells=%d\n", keyToInsert, cursor.cellNum, numCells)
	os.Stdout.Sync()
	
	if cursor.cellNum < numCells {
		keyAtIndex := *leafNodeKey(unsafe.Pointer(&node[0]), cursor.cellNum)
		fmt.Printf("[DEBUG] Key at index %d is %d\n", cursor.cellNum, keyAtIndex)
		os.Stdout.Sync()
		if keyAtIndex == keyToInsert {
			return ExecuteDuplicateKey
		}
	}
	leafNodeInsert(cursor, rowToInsert.id, rowToInsert)
	return ExecuteSuccess
}

func executeSelect(statement *Statement, table *Table) ExecuteResult {
	fmt.Printf("[DEBUG] Selecting.\n")
	var row Row
	cursor := tableStart(table)
	for !cursor.endOfTable {
		page := cursorValue(cursor)
		value := leafNodeValue(unsafe.Pointer(&page[0]), cursor.cellNum)
		unserializeRow((*[rowSize]byte)(value)[:], &row)
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
	table := &Table{}
	table.pager = pager
	table.RootPageNum = 0
	
	if pager.numPages == 0 {
		rootNode := getPage(pager, 0)
		initializeLeafNode(unsafe.Pointer(&rootNode[0]))
		setNodeRoot(unsafe.Pointer(&rootNode[0]), false)
	}
	
	fmt.Printf("[DEBUG] Opened DB: fileLength=%d, numPages=%d\n", pager.fileLength, pager.numPages)
	return table
}

// until we recycle free pages, new pages will go until end of db file
func getUnusedPageNum(pager *Pager) uint32 {
	return pager.numPages
}
// Creates a new root
func createNewRoot(table *Table, rightChildPageNum uint32) {
	// Handle splitting root
	// Old root copied to new page, becomes left child
	// Address of right child passed in 
	// Re-iniialize root page to contain new root node
	// New root node points to children
	root := getPage(table.pager, int(table.RootPageNum))
	leftChildPageNum := getUnusedPageNum(table.pager)
	leftChild := getPage(table.pager, int(leftChildPageNum))

	// reuse old root page
	copy(leftChild[:PageSize], root[:PageSize])
	setNodeRoot(unsafe.Pointer(&leftChild[0]), false)
	
	// root node is a new internal node with one key and two children
	initializeInternalNode(unsafe.Pointer(&root[0]))
	setNodeRoot(unsafe.Pointer(&root[0]), true)
	*internalNodeNumKeys(unsafe.Pointer(&root[0])) = 1
	*(*uint32)(unsafe.Add(unsafe.Pointer(&root[0]), internalNodeHeaderSize)) = leftChildPageNum
	leftChildMaxKey := getNodeMaxKey(unsafe.Pointer(&leftChild[0]))
	*internalNodeKey(unsafe.Pointer(&root[0]), 0) = leftChildMaxKey
	*internalNodeRightChild(unsafe.Pointer(&root[0])) = rightChildPageNum
}

// Read and write to internal node
func internalNodeNumKeys(node unsafe.Pointer) *uint32 {
	return (*uint32)(unsafe.Add(node, internalNodeNumKeysOffset))
}

func internalNodeRightChild(node unsafe.Pointer) *uint32 {
	return (*uint32)(unsafe.Add(node, internalNodeRightChildOffset))
}

func internalNodeCell(node unsafe.Pointer, cellNum uint32) unsafe.Pointer {
	return unsafe.Add(node, internalNodeHeaderSize + int(cellNum * internalNodeCellSize))
}

func internalNodeChild(node unsafe.Pointer, childNum uint32) uint32 { 
	numKeys := *internalNodeNumKeys(node)
	if childNum > numKeys { 
		fmt.Printf("Tried to access child_num %d > num_keys %d\n", childNum, numKeys)
		os.Exit(1)
		return 0
	} else if childNum == numKeys{
		return *internalNodeRightChild(node)
	} else {
		return *(*uint32)(internalNodeCell(node, childNum))
	}
}

func getNodeMaxKey(node unsafe.Pointer) uint32 {
	switch getNodeType(node) {
	case NodeInternal:
		numKeys := *internalNodeNumKeys(node)
		return *internalNodeKey(node, numKeys - 1)
	case NodeLeaf:
		numCells := *leafNodeNumCells(node)
		return *leafNodeKey(node, numCells - 1)
	default: 
		panic("Unknown node type in getNodeMaxKey")
	}
}

func isNoderoot(node unsafe.Pointer) bool {
	value := *(*uint8)(unsafe.Add(node, IsRootOffset))
	return value != 0
}

func setNodeRoot(node unsafe.Pointer, isRoot bool) {
	value := uint8(0)
	if isRoot {
		value = 1
	}
	*(*uint8)(unsafe.Add(node, IsRootOffset)) = value
}

func internalNodeKey(node unsafe.Pointer, keyNum uint32) *uint32 { 
	return (*uint32)(unsafe.Add(internalNodeCell(node, keyNum), internalNodeChildSize))
}

func internalNodeFind(table *Table, pageNum uint32, key uint32) *Cursor {
	node := getPage(table.pager, pageNum) 
	numKeys := *internalNodeNumKeys(node)

	// Binary search to find index of child to search 
	minIndex := 0
	maxIndex := numKeys // One more child than keys
	for minIndex != maxIndex {
		index := (minIndex + maxIndex) / 2
		keyToRight := *internalNodeKey(node, index)
		if keyToRight >= key {
			maxIndex = index
		} else {
			minIndex = index +1
		}
	}
	// When we find correct child, call the correct search func
	childNum := *internalNodeChild(node, minIndex)
	child := getPage(table.Pager, childNum)
	switch getNodeType(child) { 
	case NodeLeaf:
		return leafNodeFind(table, childNum, key)
	case NodeInternal:
		return internalNodeFind(table, childNum, key)
	}
}

// Creates a new node and moves half cells over. insert new val in one of two node. update parent or make new parent
func leafNodeSplitAndInsert(cursor *Cursor, key uint32, value *Row) {
	// new node
	oldNode := getPage(cursor.table.pager, int(cursor.pageNum))
	newPageNum := getUnusedPageNum(cursor.table.pager)
	newNode := getPage(cursor.table.pager, int(newPageNum))
	initializeLeafNode(unsafe.Pointer(&newNode[0]))
	// divide between two old and new nodes
	// start from right, move each key to correct position
	for i := int(LeafNodeMaxCells); i >= 0; i-- {
		var destinationNode unsafe.Pointer
		if i >= leafNodeLeftSplitCount {
			destinationNode = unsafe.Pointer(&newNode[0])
		} else {
			destinationNode = unsafe.Pointer(&oldNode[0])
		}
		indexWithinNode := uint32(i % leafNodeLeftSplitCount)
		destination := leafNodeCell(destinationNode, indexWithinNode)

		if uint32(i) == cursor.cellNum {
			serializeRow(value, (*[rowSize]byte)(destination)[:])
		} else if uint32(i) > cursor.cellNum {
			src := leafNodeCell(unsafe.Pointer(&oldNode[0]), uint32(i-1))
			copy(
				(*[LeafNodeCellSize]byte)(destination)[:],
				(*[LeafNodeCellSize]byte)(src)[:],
			)
		} else {
			src := leafNodeCell(unsafe.Pointer(&oldNode[0]), uint32(i))
			copy(
				(*[LeafNodeCellSize]byte)(destination)[:],
				(*[LeafNodeCellSize]byte)(src)[:],
			)
		}
	} 
	// update cell count on both leaf nodes 
	*leafNodeNumCells(unsafe.Pointer(&oldNode[0])) = uint32(leafNodeLeftSplitCount)
	*leafNodeNumCells(unsafe.Pointer(&newNode[0])) = uint32(leafNodeRightSplitCount)

	// update nodes' parent 
	if isNoderoot(unsafe.Pointer(&oldNode[0])) { 
		createNewRoot(cursor.table, newPageNum)
	} else { 
		return internalNodeFind(table, rootPageNum)
	}
}


func leafNodeInsert(cursor *Cursor, key uint32, value *Row) {
	node := getPage(cursor.table.pager, int(cursor.pageNum))
	nodePtr := unsafe.Pointer(&node[0])
	numCells := *leafNodeNumCells(nodePtr)
	
	if numCells >= uint32(LeafNodeMaxCells) {
		// node full then split
		leafNodeSplitAndInsert(cursor, key, value) 
		return
	}
	
	if cursor.cellNum < numCells {
		// make room for new cells 
		for i := numCells; i > cursor.cellNum; i-- {
			dest := leafNodeCell(nodePtr, i)
			src := leafNodeCell(nodePtr, i-1)
			copy((*[LeafNodeCellSize]byte)(dest)[:], (*[LeafNodeCellSize]byte)(src)[:])
		} 
	}
	
	// Insert the new cell
	cell := leafNodeCell(nodePtr, cursor.cellNum)
	keyPtr := (*uint32)(cell)
	*keyPtr = key
	
	valuePtr := leafNodeValue(nodePtr, cursor.cellNum)
	serializeRow(value, (*[rowSize]byte)(valuePtr)[:])
	
	// Update the number of cells
	*leafNodeNumCells(nodePtr) = numCells + 1
}

// flushes page cache to disk, closes the db file, frees memory for Pager and Table
func dbClose(table *Table) {
	pager := table.pager

	for i := 0; i < int(pager.numPages); i++ {
		if pager.pages[i] == nil {
			continue
		}
		pagerFlush(pager, i)
		pager.pages[i] = nil 
	}
	// // chance of a partial page to write to the end of file 
	// // no need after B Tree implementation
	// numAddrows := table.numRows % RowPerPage
	// if numAddrows > 0 {
	// 	pageNum := numFullPages
	// 	if pager.pages[pageNum] != nil {
	// 		pagerFlush(pager, pageNum, numAddrows * rowSize)
	// 		// no need to free. Go does it for us
	// 		pager.pages[pageNum] = nil
	// 	}
	// }
	// err := pager.fileDescriptor.Close()
	// if err != nil {
	// 	fmt.Printf("Error closing db file: %v\n", err)
	// 	os.Exit(1)
	// }
	// fileInfo, statErr := os.Stat(pager.filename)
	// if statErr == nil {
	// 	fmt.Printf("[DEBUG] Closed DB: file size is %d bytes\n", fileInfo.Size())
	// }
	for i := 0; i < TableMaxPages; i++ {
		page := pager.pages[i]
		if page != nil { 
			pager.pages[i] = nil
		}
	}
}
// will ensure flush
func pagerFlush(pager *Pager, pageNum int) {
	if pager.pages[pageNum] == nil {
		fmt.Printf("Tried to flush a null page\n")
		os.Exit(1)
	}
	_, err := pager.fileDescriptor.Seek(int64(pageNum*PageSize), io.SeekStart)
	if err != nil {
		fmt.Printf("Error seeking file: %v\n", err)
		os.Exit(1)
	}
	bytesWritten, err := pager.fileDescriptor.Write(pager.pages[pageNum][:PageSize])
	if err != nil {
		fmt.Printf("Error writing file: %v\n", err)
		os.Exit(1)
	}
	if bytesWritten != PageSize {
		fmt.Printf("Error: partial write. Expected %d, got %d\n", PageSize, bytesWritten)
		os.Exit(1)
	}
}

type Pager struct {
	fileDescriptor *os.File
	fileLength     int64
	pages          [TableMaxPages][]byte
	numPages       uint32
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
	pager.numPages = uint32(fileLength / PageSize)
	if fileLength%PageSize != 0 {
		fmt.Println("Db file is not a whole number of pages. Corrupt file.\n")
		os.Exit(1)
	}
	for i := 0; i < TableMaxPages; i++ {
		pager.pages[i] = nil
	}
	return pager
}

func newTable() *Table {
	return &Table{
		RootPageNum: 0,
		Pages:       [TableMaxPages][]byte{},
		pager:       nil,
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
			} else if result == ExecuteDuplicateKey {
				fmt.Println("Error: Duplicate Key")
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