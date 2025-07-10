package main

import (
	"os/exec"
	"strings"
	"testing"
)

func runScript(t *testing.T, commands []string) []string {
	cmd := exec.Command("./db-go")

	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("failed to open stdin: %v", err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("failed to open stdout: %v", err)
	}

	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start command: %v", err)
	}

	for _, command := range commands {
		_, _ = stdin.Write([]byte(command + "\n"))
	}
	stdin.Close()

	// Read all output until EOF
	var output strings.Builder
	buffer := make([]byte, 1024)
	for {
		n, err := stdout.Read(buffer)
		if n > 0 {
			output.Write(buffer[:n])
		}
		if err != nil {
			break
		}
	}

	_ = cmd.Wait()

	return strings.Split(strings.TrimSpace(output.String()), "\n")
}

func TestInsertAndSelect(t *testing.T) {
	result := runScript(t, []string{
		"insert 1 user1 person1@example.com",
		"select",
		".exit",
	})

	expected := []string{
		"db > Executed.",
		"db > (1, user1, person1@example.com)",
		"Executed.",
		"db > Goodbye!",
	}

	if len(result) != len(expected) {
		t.Fatalf("output length mismatch\nExpected: %v\nGot: %v", expected, result)
	}

	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("line %d mismatch\nExpected: %q\nGot: %q", i, expected[i], result[i])
		}
	}
} 