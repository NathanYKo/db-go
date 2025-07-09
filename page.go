package main

import (
	"encoding/binary"
	"unsafe"
)

const PAGE_SIZE = 4096
const HEADER_SIZE = 64 

type PageType uint8

const (
	PAGE_TYPE_ROOT PageType = iota
	PAGE_TYPE_LEAF
	PAGE_TYPE_INTERNAL
	PAGE_TYPE_OVERFLOW
)

// Page flags
const (
	FLAG_CAN_COMPACT = 1 << 0
)

// CellPointer points to a cell within the page
type CellPointer struct {
	CellLocation uint16 // Offset from start of page to cell data
	CellSize     uint16 // Size of the cell data
}

// PageHeader represents the header structure at the beginning of each page
type PageHeader struct {
	ID         uint32    // Unique identifier for the page
	Type       PageType  // Type of page
	FreeStart  uint16    // Where cell pointers start (grows right)
	FreeEnd    uint16    // Where cells start (grows left from end)
	TotalFree  uint16    // Total available space
	Flags      uint8     // Status flags
	Reserved   [32]byte  // Reserved for future use
}

type Page struct {
	bytes []byte
}

func new_page(size int) *Page {
	return &Page{
		bytes: make([]byte, size),
	}
}

func new_page_with_header(pageType PageType, id uint32) *Page {
	page := new_page(PAGE_SIZE)
	page.init_header(id, pageType)
	return page
}

func (p *Page) init_header(pageID uint32, pageType PageType) {
	header := &PageHeader{
		ID:        pageID,
		Type:      pageType,
		FreeStart: HEADER_SIZE,                   
		FreeEnd:   PAGE_SIZE - 1,                  // End at last byte
		TotalFree: PAGE_SIZE - 1 - HEADER_SIZE,    // Available space
		Flags:     0,
	}
	
	p.write_header(header)
}

// Write header to the page bytes
func (p *Page) write_header(header *PageHeader) {
	offset := 0
	
	// ID (4 bytes)
	binary.LittleEndian.PutUint32(p.bytes[offset:], header.ID)
	offset += 4
	
	// Type (1 byte)
	p.bytes[offset] = uint8(header.Type)
	offset += 1
	
	// FreeStart (2 bytes)
	binary.LittleEndian.PutUint16(p.bytes[offset:], header.FreeStart)
	offset += 2
	
	// FreeEnd (2 bytes)
	binary.LittleEndian.PutUint16(p.bytes[offset:], header.FreeEnd)
	offset += 2
	
	// TotalFree (2 bytes)
	binary.LittleEndian.PutUint16(p.bytes[offset:], header.TotalFree)
	offset += 2
	
	// Flags (1 byte)
	p.bytes[offset] = header.Flags
	offset += 1
	
	// Reserved (32 bytes)
	copy(p.bytes[offset:], header.Reserved[:])
}

// Read header from the page bytes
func (p *Page) read_header() *PageHeader {
	header := &PageHeader{}
	offset := 0
	
	// ID (4 bytes)
	header.ID = binary.LittleEndian.Uint32(p.bytes[offset:])
	offset += 4
	
	// Type (1 byte)
	header.Type = PageType(p.bytes[offset])
	offset += 1
	
	// FreeStart (2 bytes)
	header.FreeStart = binary.LittleEndian.Uint16(p.bytes[offset:])
	offset += 2
	
	// FreeEnd (2 bytes)
	header.FreeEnd = binary.LittleEndian.Uint16(p.bytes[offset:])
	offset += 2
	
	// TotalFree (2 bytes)
	header.TotalFree = binary.LittleEndian.Uint16(p.bytes[offset:])
	offset += 2
	
	// Flags (1 byte)
	header.Flags = p.bytes[offset]
	offset += 1
	
	// Reserved (32 bytes)
	copy(header.Reserved[:], p.bytes[offset:offset+32])
	
	return header
}

// Get cell pointer at index
func (p *Page) get_cell_pointer(idx int) *CellPointer {
	offset := p.cell_pointer_offset(idx)
	if int(offset) >= len(p.bytes) {
		return nil
	}
	
	ptr := &CellPointer{}
	ptr.CellLocation = binary.LittleEndian.Uint16(p.bytes[offset:])
	ptr.CellSize = binary.LittleEndian.Uint16(p.bytes[offset+2:])
	return ptr
}

// Convert cell pointer offset to index
func cell_pointer_offset_to_idx(offset uint16) int {
	return int((offset - HEADER_SIZE) / 4) // 4 bytes per CellPointer
}

// Convert cell pointer index to offset
func (p *Page) cell_pointer_offset(idx int) uint16 {
	return uint16(idx*4 + HEADER_SIZE) // 4 bytes per CellPointer
}

// Add a cell to the page (similar to C version)
func (p *Page) add_cell(cell []byte) int {
	header := p.read_header()
	cellSize := uint16(len(cell))
	
	// Check if we have enough space
	if header.TotalFree < cellSize+4 { // 4 bytes for CellPointer
		return -1 // Not enough space
	}
	
	// Create cell pointer
	cellPtr := &CellPointer{
		CellLocation: header.FreeEnd - cellSize,
		CellSize:     cellSize,
	}
	
	// Add cell data at the end
	copy(p.bytes[cellPtr.CellLocation:], cell)
	
	// Add cell pointer at the beginning
	pointerOffset := header.FreeStart
	p.write_cell_pointer(pointerOffset, cellPtr)
	
	// Update header
	header.FreeEnd -= cellSize
	header.FreeStart += 4 // Size of CellPointer
	header.TotalFree = header.FreeEnd - header.FreeStart
	p.write_header(header)
	
	return cell_pointer_offset_to_idx(pointerOffset)
}

// Write cell pointer at specific offset
func (p *Page) write_cell_pointer(offset uint16, ptr *CellPointer) {
	binary.LittleEndian.PutUint16(p.bytes[offset:], ptr.CellLocation)
	binary.LittleEndian.PutUint16(p.bytes[offset+2:], ptr.CellSize)
}

// Remove a cell (mark for compaction)
func (p *Page) remove_cell(idx int) {
	header := p.read_header()
	pointerOffset := p.cell_pointer_offset(idx)
	
	// Mark for compaction
	header.Flags |= FLAG_CAN_COMPACT
	p.write_header(header)
	
	// Mark cell pointer as invalid
	p.write_cell_pointer(pointerOffset, &CellPointer{CellLocation: 0, CellSize: 0})
}

// Get cell data by index
func (p *Page) get_cell(idx int) []byte {
	ptr := p.get_cell_pointer(idx)
	if ptr == nil || ptr.CellLocation == 0 {
		return nil
	}
	
	return p.bytes[ptr.CellLocation : ptr.CellLocation+ptr.CellSize]
}

// Compact the page (remove dead cells)
func (p *Page) compact() {
	header := p.read_header()
	
	if header.Flags&FLAG_CAN_COMPACT == 0 {
		return // Nothing to compact
	}
	
	// Create a temporary page for compaction
	tempPage := new_page_with_header(header.Type, 0)
	
	// Copy all live cells
	numPointers := (header.FreeStart - HEADER_SIZE) / 4
	for i := 0; i < int(numPointers); i++ {
		ptr := p.get_cell_pointer(i)
		if ptr != nil && ptr.CellLocation != 0 {
			cellData := p.bytes[ptr.CellLocation : ptr.CellLocation+ptr.CellSize]
			tempPage.add_cell(cellData)
		}
	}
	
	// Copy compacted data back
	tempHeader := tempPage.read_header()
	header.FreeStart = tempHeader.FreeStart
	header.FreeEnd = tempHeader.FreeEnd
	header.TotalFree = tempHeader.TotalFree
	header.Flags &= ^uint8(FLAG_CAN_COMPACT)
	
	p.write_header(header)
	
	// Copy the data area
	copy(p.bytes[HEADER_SIZE:], tempPage.bytes[HEADER_SIZE:])
}

// Get a pointer to the data area (after the header)
func (p *Page) data_ptr() unsafe.Pointer {
	return unsafe.Pointer(&p.bytes[HEADER_SIZE])
}

// Get the data area as a slice
func (p *Page) data() []byte {
	return p.bytes[HEADER_SIZE:]
}