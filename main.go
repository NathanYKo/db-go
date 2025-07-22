package main

import (
	"bufio"
	"fmt"
	"io"
	"unsafe"
	"os"
	"strings"
	"syscall"
	"math"
)

const (
	invalidPageNum = math.MaxUint32
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
	LeafNodeNextLeafSize = 4
	LeafNodeNextLeafOffset = LeafNodeNumCellsOffset + LeafNodeNumCellsSize
	LeafNodeHeaderSize = CommonNodeHeaderSize + LeafNodeNumCellsSize + LeafNodeNextLeafSize
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
	internalNodeMaxCells = 3 //for testing
	internalNodeMaxKeys = internalNodeMaxCells
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
	*leafNodeNextLeaf(node) = 0 //means no siblings

}

func initializeInternalNode(node unsafe.Pointer) { 
	setNodeType(node, NodeInternal)
	setNodeRoot(node, false)
	*internalNodeNumKeys(node) = 0
	// may end up with node right child as 0 which make it the parent of the root
	*internalNodeRightChild(node) = invalidPageNum
}
// create new cursors
func tableStart(table *Table) *Cursor {
	cursor := tableFind(table, 0)

	node := getPage(table.pager, int(table.RootPageNum))
	numCells := *leafNodeNumCells(unsafe.Pointer(&node[0]))
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

// access new field
func leafNodeNextLeaf(node unsafe.Pointer) *uint32 {
	return (*uint32)(unsafe.Add(node, LeafNodeNextLeafOffset))
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
		//advance to next leaf node
		nextPageNum := *leafNodeNextLeaf(unsafe.Pointer(&page[0]))
		if nextPageNum == 0 {
			// rightmost leaf
			cursor.endOfTable = true
		} else {
			cursor.pageNum = nextPageNum
			cursor.cellNum = 0
		}
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

func printTree(table *Table, pageNum uint32, depth int) {
	node := getPage(table.pager, int(pageNum))
	nodePtr := unsafe.Pointer(&node[0])
	
	indent := strings.Repeat("  ", depth)
	
	switch getNodeType(nodePtr) {
	case NodeInternal:
		numKeys := *internalNodeNumKeys(nodePtr)
		fmt.Printf("%s- internal (size %d)\n", indent, numKeys)
		
		// Print all children except the rightmost
		for i := uint32(0); i < numKeys; i++ {
			childNum := internalNodeChild(nodePtr, i)
			printTree(table, childNum, depth+1)
			
			// Print the key after the child
			key := *internalNodeKey(nodePtr, i)
			fmt.Printf("%s- key %d\n", indent, key)
		}
		
		// Print the rightmost child
		rightChildNum := *internalNodeRightChild(nodePtr)
		printTree(table, rightChildNum, depth+1)
		
	case NodeLeaf:
		numCells := *leafNodeNumCells(nodePtr)
		fmt.Printf("%s- leaf (size %d)\n", indent, numCells)
		
		// Print all keys in the leaf
		for i := uint32(0); i < numCells; i++ {
			key := *leafNodeKey(nodePtr, i)
			fmt.Printf("%s  - %d\n", indent, key)
		}
	}
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
	} else if trimmed == ".btree" {
		fmt.Println("Tree:")
		printTree(table, table.RootPageNum, 0)
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

// Search parent by max key
func updateInternalNodeKey(node unsafe.Pointer, oldKey uint32, newKey uint32) {
	oldChildIndex := internalNodeFindChild(node, oldKey)
	*internalNodeKey(node, oldChildIndex) = newKey
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
	*nodeParent(unsafe.Pointer(&leftChild[0])) = table.RootPageNum
	// Get the right child page and set its parent
	rightChild := getPage(table.pager, int(rightChildPageNum))
	*nodeParent(unsafe.Pointer(&rightChild[0])) = table.RootPageNum

	if getNodeType(unsafe.Pointer(&root[0])) == NodeInternal {
		initializeInternalNode(unsafe.Pointer(&rightChild[0]))
		initializeInternalNode(unsafe.Pointer(&leftChild[0]))
	}
	// reuse old root page
	copy(leftChild[:PageSize], root[:PageSize])
	setNodeRoot(unsafe.Pointer(&leftChild[0]), false)
	
	if getNodeType(unsafe.Pointer(&leftChild[0])) == NodeInternal { 
		for i := uint32(0); i < *internalNodeNumKeys(unsafe.Pointer(&leftChild[0])); i++ {
			childPageNum := internalNodeChild(unsafe.Pointer(&leftChild[0]), i)
			child := getPage(table.pager, int(childPageNum))
			*nodeParent(unsafe.Pointer(&child[0])) = leftChildPageNum
		}
	}

	// root node is a new internal node with one key and two children
	initializeInternalNode(unsafe.Pointer(&root[0]))
	setNodeRoot(unsafe.Pointer(&root[0]), true)
	*internalNodeNumKeys(unsafe.Pointer(&root[0])) = 1
	*(*uint32)(unsafe.Add(unsafe.Pointer(&root[0]), internalNodeHeaderSize)) = leftChildPageNum
	leftChildMaxKey := getNodeMaxKey(table.pager, unsafe.Pointer(&leftChild[0]))
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
		rightChild := internalNodeRightChild(node)
		if (*rightChild == invalidPageNum){
			fmt.Printf("Tried to access right child of node, but was invalid page\n")
			os.Exit(1)
		}
		return *rightChild
		} else {
			childPtr := internalNodeCell(node, childNum)
			child := *(*uint32)(childPtr)
			if child == invalidPageNum { 
				fmt.Printf("Tried to access child %d of node, but was invalid page\n", childNum)
				os.Exit(1)
			}
		return child
		}
	}


func getNodeMaxKey(pager *Pager, node unsafe.Pointer) uint32 {
	if getNodeType(node) == NodeLeaf { 
		return *leafNodeKey(node, *leafNodeNumCells(node) -1)
	}
	rightChildPageNum := *internalNodeRightChild(node)
	rightChild := getPage(pager, int(rightChildPageNum))
	return getNodeMaxKey(pager, unsafe.Pointer(&rightChild[0]))
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

func internalNodeFindChild(node unsafe.Pointer, key uint32) uint32 {
	numKeys := *internalNodeNumKeys(node)

	// Binary search 
	minIndex := uint32(0)
	maxIndex := numKeys // One more child than keys
	for minIndex != maxIndex {
		index := (minIndex + maxIndex) / 2
		keyToRight := *internalNodeKey(node, index)
		if keyToRight >= key {
			maxIndex = index
		} else {
			minIndex = index + 1
		}
	}
	// When we find correct child, call the correct search func
	return minIndex
}
func internalNodeFind(table *Table, pageNum uint32, key uint32) *Cursor {
	node := getPage(table.pager, int(pageNum))
	nodePtr := unsafe.Pointer(&node[0])
	childIndex := internalNodeFindChild(nodePtr, key)
	childNum := internalNodeChild(nodePtr, childIndex)
	child := getPage(table.pager, int(childNum))
	switch getNodeType(unsafe.Pointer(&child[0])) { 
	case NodeLeaf:
		return leafNodeFind(table, childNum, key)
	case NodeInternal:
		return internalNodeFind(table, childNum, key)
	default:
		fmt.Printf("Unknown node type %d\n", getNodeType(unsafe.Pointer(&child[0])))
		os.Exit(1)
		return nil
	}
}

// Record node parent
func nodeParent(node unsafe.Pointer) *uint32 {
	return (*uint32)(unsafe.Add(node, ParentPointerOffset))
}
func internalNodeSplitAndInsert(table *Table, parentPageNum uint32, childPageNum uint32) {
	oldPageNum := parentPageNum
	oldNode := getPage(table.pager, int(parentPageNum))
	oldMax := getNodeMaxKey(table.pager, unsafe.Pointer(&oldNode[0]))

	child := getPage(table.pager, int(childPageNum))
	childMax := getNodeMaxKey(table.pager, unsafe.Pointer(&child[0]))

	newPageNum := getUnusedPageNum(table.pager)
	
	// Check if we're splitting the root
	splittingRoot := isNoderoot(unsafe.Pointer(&oldNode[0]))
	var parent []byte
	var newNode []byte

	if splittingRoot {
		// Create new root and insert both children
		createNewRoot(table, newPageNum)
		parent = getPage(table.pager, int(table.RootPageNum))
		// if split root, need to update oldnode to new root's left child
		// newPageNum will already point to new root's right child
		oldPageNum = internalNodeChild(unsafe.Pointer(&parent[0]), 0)
		oldNode = getPage(table.pager, int(oldPageNum))
	} else {
		// Split the parent node
		parentPageNum := *nodeParent(unsafe.Pointer(&oldNode[0]))
		parent = getPage(table.pager, int(parentPageNum))
		newNode = getPage(table.pager, int(newPageNum))
		initializeInternalNode(unsafe.Pointer(&newNode[0]))
	}

	oldNumKeys := internalNodeNumKeys(unsafe.Pointer(&oldNode[0]))
	curPageNum := *internalNodeRightChild(unsafe.Pointer(&oldNode[0]))
	cur := getPage(table.pager, int(curPageNum))

	// put right child into new node and set right child of old node to invalidPageNum
	internalNodeInsert(table, newPageNum, curPageNum)
	*nodeParent(unsafe.Pointer(&cur[0])) = newPageNum

	// for each key until you get to middle key, move key and child to new node
	for i := int(internalNodeMaxCells) - 1; i > int(internalNodeMaxCells) / 2; i-- {
		curPageNum = internalNodeChild(unsafe.Pointer(&oldNode[0]), uint32(i))
		cur = getPage(table.pager, int(curPageNum))

		internalNodeInsert(table, newPageNum, curPageNum)
		*nodeParent(unsafe.Pointer(&cur[0])) = newPageNum

		*oldNumKeys--
	}
	// set child before middle key which is now the highest key to be node's right child
	//decrement # of keys
	*internalNodeRightChild(unsafe.Pointer(&oldNode[0])) = internalNodeChild(unsafe.Pointer(&oldNode[0]), *oldNumKeys - 1)
	*oldNumKeys--
	
	// determine which of two nodes after split should contain child to be inserted and insert it 
	maxAfterSplit := getNodeMaxKey(table.pager, unsafe.Pointer(&oldNode[0]))
	var destinationPageNum uint32
	if childMax < maxAfterSplit {
		destinationPageNum = oldPageNum
	} else {
		destinationPageNum = newPageNum
	}
	internalNodeInsert(table, destinationPageNum, childPageNum)
	*nodeParent(unsafe.Pointer(&child[0])) = destinationPageNum

	updateInternalNodeKey(unsafe.Pointer(&parent[0]), oldMax, getNodeMaxKey(table.pager, unsafe.Pointer(&oldNode[0])))

	if !splittingRoot {
		parentPageNum := *nodeParent(unsafe.Pointer(&oldNode[0]))
		internalNodeInsert(table, parentPageNum, newPageNum)
		*nodeParent(unsafe.Pointer(&newNode[0])) = parentPageNum
	}
	}

// Insert func 
func internalNodeInsert(table *Table, parentPageNum uint32, childPageNum uint32) {

	// add new child/key pair to parent that corresponds to child
	parent := getPage(table.pager, int(parentPageNum))
	child := getPage(table.pager, int(childPageNum))
	childMaxKey := getNodeMaxKey(table.pager, unsafe.Pointer(&child[0]))
	index := internalNodeFindChild(unsafe.Pointer(&parent[0]), childMaxKey)

	originalNumKeys := *internalNodeNumKeys(unsafe.Pointer(&parent[0]))

	if originalNumKeys >= internalNodeMaxKeys {
		internalNodeSplitAndInsert(table, parentPageNum, childPageNum)
		return
	}
	rightChildPageNum := *internalNodeRightChild(unsafe.Pointer(&parent[0]))
	// internal Node w right child of invalidPageNum is empty
	if rightChildPageNum == invalidPageNum{
		*internalNodeRightChild(unsafe.Pointer(&parent[0])) = childPageNum
		return 
	}

	rightChild := getPage(table.pager, int(rightChildPageNum))
	
	if childMaxKey > getNodeMaxKey(table.pager, unsafe.Pointer(&rightChild[0])) {
		//replace right child
		*(*uint32)(unsafe.Add(unsafe.Pointer(&parent[0]), internalNodeHeaderSize + int(originalNumKeys) * internalNodeCellSize)) = rightChildPageNum
		*internalNodeKey(unsafe.Pointer(&parent[0]), originalNumKeys) = getNodeMaxKey(table.pager, unsafe.Pointer(&rightChild[0]))
		*internalNodeRightChild(unsafe.Pointer(&parent[0])) = childPageNum
	} else {
		// make room for the new cell
		for i := originalNumKeys; i > index; i--{
			destination := internalNodeCell(unsafe.Pointer(&parent[0]), i)
			source := internalNodeCell(unsafe.Pointer(&parent[0]), i - 1)
			copy(
				(*[internalNodeCellSize]byte)(destination)[:],
				(*[internalNodeCellSize]byte)(source)[:],
			)
		}
		*(*uint32)(unsafe.Add(unsafe.Pointer(&parent[0]), internalNodeHeaderSize + int(index) * internalNodeCellSize)) = childPageNum
		*internalNodeKey(unsafe.Pointer(&parent[0]), index) = childMaxKey
	}
}


// Creates a new node and moves half cells over. insert new val in one of two node. update parent or make new parent
func leafNodeSplitAndInsert(cursor *Cursor, key uint32, value *Row) {
	// new node
	oldNode := getPage(cursor.table.pager, int(cursor.pageNum))
	oldMax := getNodeMaxKey(cursor.table.pager, unsafe.Pointer(&oldNode[0]))
	newPageNum := getUnusedPageNum(cursor.table.pager)
	newNode := getPage(cursor.table.pager, int(newPageNum))
	initializeLeafNode(unsafe.Pointer(&newNode[0]))
	*nodeParent(unsafe.Pointer(&newNode[0])) = *nodeParent(unsafe.Pointer(&oldNode[0]))
	*leafNodeNextLeaf(unsafe.Pointer(&newNode[0])) = *leafNodeNextLeaf(unsafe.Pointer(&oldNode[0]))
	*leafNodeNextLeaf(unsafe.Pointer(&oldNode[0])) = newPageNum
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
		parentPageNum := *nodeParent(unsafe.Pointer(&oldNode[0]))
		newMax := getNodeMaxKey(cursor.table.pager, unsafe.Pointer(&oldNode[0]))
		parent := getPage(cursor.table.pager, (int)(parentPageNum))
		updateInternalNodeKey(unsafe.Pointer(&parent[0]), oldMax, newMax)
		internalNodeInsert(cursor.table, parentPageNum, newPageNum)
		return 
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