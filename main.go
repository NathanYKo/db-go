package main

import (
	"bufio"
	"fmt"
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
)

type StatementType int 

const (
	StatementInsert StatementType = iota
	StatementSelect
)

type Statement struct { 
	Type StatementType
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

func PrepareStatement(input *InputBuffer, statement *Statement) PrepareResult {
	if strings.HasPrefix(input.buffer, "insert"){
		statement.Type = StatementInsert
		return PrepareSuccess
	}
	if strings.HasPrefix(input.buffer, "select"){
		statement.Type = StatementSelect
		return PrepareSuccess
	}
	return PrepareUnrecognized
}

func executeStatement(statement *Statement){
	switch statement.Type {
	case StatementInsert:
		fmt.Println("This is where we would do an insert.")
	case StatementSelect:
		fmt.Println("This is where we would do a select.")
	} 
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
	scanner := bufio.NewScanner(os.Stdin)
	if scanner.Scan() {
		line := scanner.Text()
		inputBuffer.buffer = line
		inputBuffer.bufferLength = len(line)
		inputBuffer.inputLength = len(line)
	} else {
		if err := scanner.Err(); err != nil {
			fmt.Println("Error reading input:", err)
			os.Exit(1)
		}
		// EOF reached
		fmt.Println("Error reading input")
		os.Exit(1)
	}
}

func main() {
	inputBuffer := newInputBuffer()
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
			executeStatement(&statement)
			fmt.Println("Executed.")
		case PrepareUnrecognized:
			fmt.Printf("Unrecognized keyword at start of '%s'.\n", inputBuffer.buffer)
			continue
		}
	}
}