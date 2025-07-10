package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Test utilities
func runScript(t *testing.T, dbFile string, commands []string) []string {
	cmd := exec.Command("./db-go", dbFile)

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

func cleanupTestFile(filename string) {
	os.Remove(filename)
}

// Helper function to check if output contains expected lines
func containsLines(output []string, expected []string) bool {
	for _, expectedLine := range expected {
		found := false
		for _, outputLine := range output {
			if strings.Contains(outputLine, expectedLine) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// Basic functionality tests
func TestBasicInsertAndSelect(t *testing.T) {
	filename := "test_basic.db"
	cleanupTestFile(filename)
	defer cleanupTestFile(filename)

	result := runScript(t, filename, []string{
		"insert 1 user1 person1@example.com",
		"select",
		".exit",
	})

	expected := []string{
		"Executed.",
		"(1, user1, person1@example.com)",
		"Goodbye!",
	}

	if !containsLines(result, expected) {
		t.Errorf("Expected output to contain %v, got %v", expected, result)
	}
}

func TestMultipleInserts(t *testing.T) {
	filename := "test_multiple.db"
	cleanupTestFile(filename)
	defer cleanupTestFile(filename)

	result := runScript(t, filename, []string{
		"insert 1 alice alice@example.com",
		"insert 2 bob bob@example.com",
		"insert 3 charlie charlie@example.com",
		"select",
		".exit",
	})

	expected := []string{
		"(1, alice, alice@example.com)",
		"(2, bob, bob@example.com)",
		"(3, charlie, charlie@example.com)",
	}

	if !containsLines(result, expected) {
		t.Errorf("Expected output to contain %v, got %v", expected, result)
	}
}

// Persistence tests
func TestDataPersistence(t *testing.T) {
	filename := "test_persistence.db"
	cleanupTestFile(filename)
	defer cleanupTestFile(filename)

	// First session: insert data
	result1 := runScript(t, filename, []string{
		"insert 1 user1 person1@example.com",
		"insert 2 user2 person2@example.com",
		".exit",
	})

	if !containsLines(result1, []string{"Executed.", "Goodbye!"}) {
		t.Errorf("First session failed: %v", result1)
	}

	// Second session: verify data persisted
	result2 := runScript(t, filename, []string{
		"select",
		".exit",
	})

	expected := []string{
		"(1, user1, person1@example.com)",
		"(2, user2, person2@example.com)",
	}

	if !containsLines(result2, expected) {
		t.Errorf("Data persistence failed. Expected %v, got %v", expected, result2)
	}
}

func TestPersistenceWithLargeDataset(t *testing.T) {
	filename := "test_large_persistence.db"
	cleanupTestFile(filename)
	defer cleanupTestFile(filename)

	// Insert many rows
	commands := []string{}
	for i := 1; i <= 50; i++ {
		commands = append(commands, fmt.Sprintf("insert %d user%d user%d@example.com", i, i, i))
	}
	commands = append(commands, ".exit")

	result1 := runScript(t, filename, commands)
	if !containsLines(result1, []string{"Goodbye!"}) {
		t.Errorf("Large insert failed: %v", result1)
	}

	// Verify all data persisted
	result2 := runScript(t, filename, []string{
		"select",
		".exit",
	})

	// Check that we have the expected number of rows
	rowCount := 0
	for _, line := range result2 {
		if strings.Contains(line, "user") && strings.Contains(line, "@example.com") {
			rowCount++
		}
	}

	if rowCount != 50 {
		t.Errorf("Expected 50 rows, got %d", rowCount)
	}
}

// Error handling tests
func TestNegativeID(t *testing.T) {
	filename := "test_negative_id.db"
	cleanupTestFile(filename)
	defer cleanupTestFile(filename)

	result := runScript(t, filename, []string{
		"insert -1 user1 person1@example.com",
		"select",
		".exit",
	})

	if !containsLines(result, []string{"ID must be positive."}) {
		t.Errorf("Expected negative ID error, got %v", result)
	}

	// Verify no data was inserted
	if containsLines(result, []string{"user1"}) {
		t.Errorf("Data was inserted despite negative ID")
	}
}

func TestSyntaxError(t *testing.T) {
	filename := "test_syntax.db"
	cleanupTestFile(filename)
	defer cleanupTestFile(filename)

	testCases := []struct {
		command string
		desc    string
	}{
		{"insert", "missing arguments"},
		{"insert 1", "missing email"},
		{"insert 1 user1", "missing email"},
		{"insert abc user1 person1@example.com", "non-numeric ID"},
		{"insert 1 2 3 4", "too many arguments"},
		{"select 1", "select with arguments"},
		{"insert 1 user1 person1@example.com extra", "extra arguments"},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			result := runScript(t, filename, []string{
				tc.command,
				"select",
				".exit",
			})

			if !containsLines(result, []string{"Syntax error"}) {
				t.Errorf("Expected syntax error for '%s', got %v", tc.command, result)
			}
		})
	}
}

func TestStringTooLong(t *testing.T) {
	filename := "test_long_string.db"
	cleanupTestFile(filename)
	defer cleanupTestFile(filename)

	// Create strings that exceed the column limits
	longUsername := strings.Repeat("a", 33) // 33 chars > 32 limit
	longEmail := strings.Repeat("a", 256)   // 256 chars > 255 limit

	testCases := []struct {
		command string
		desc    string
	}{
		{fmt.Sprintf("insert 1 %s person1@example.com", longUsername), "username too long"},
		{fmt.Sprintf("insert 1 user1 %s", longEmail), "email too long"},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			result := runScript(t, filename, []string{
				tc.command,
				"select",
				".exit",
			})

			if !containsLines(result, []string{"String is too long."}) {
				t.Errorf("Expected string too long error for '%s', got %v", tc.command, result)
			}
		})
	}
}

// Meta command tests
func TestMetaCommands(t *testing.T) {
	filename := "test_meta.db"
	cleanupTestFile(filename)
	defer cleanupTestFile(filename)

	result := runScript(t, filename, []string{
		"insert 1 user1 person1@example.com",
		".invalid",
		"select",
		".exit",
	})

	if !containsLines(result, []string{"Unrecognized command"}) {
		t.Errorf("Expected unrecognized command error, got %v", result)
	}

	// Verify the database still works after invalid meta command
	if !containsLines(result, []string{"(1, user1, person1@example.com)"}) {
		t.Errorf("Database should still work after invalid meta command")
	}
}

// Edge cases
func TestEmptyInput(t *testing.T) {
	filename := "test_empty.db"
	cleanupTestFile(filename)
	defer cleanupTestFile(filename)

	result := runScript(t, filename, []string{
		"",
		"   ",
		"select",
		".exit",
	})

	// Should handle empty input gracefully
	if !containsLines(result, []string{"Executed."}) {
		t.Errorf("Expected successful execution after empty input, got %v", result)
	}
}

func TestWhitespaceHandling(t *testing.T) {
	filename := "test_whitespace.db"
	cleanupTestFile(filename)
	defer cleanupTestFile(filename)

	result := runScript(t, filename, []string{
		"  insert  1  user1  person1@example.com  ",
		"  select  ",
		"  .exit  ",
	})

	expected := []string{
		"(1, user1, person1@example.com)",
		"Goodbye!",
	}

	if !containsLines(result, expected) {
		t.Errorf("Expected proper whitespace handling, got %v", result)
	}
}

func TestSpecialCharacters(t *testing.T) {
	filename := "test_special_chars.db"
	cleanupTestFile(filename)
	defer cleanupTestFile(filename)

	result := runScript(t, filename, []string{
		"insert 1 user-name user.name@example.com",
		"insert 2 user_name user_name@example.com",
		"insert 3 user.name user-name@example.com",
		"select",
		".exit",
	})

	expected := []string{
		"(1, user-name, user.name@example.com)",
		"(2, user_name, user_name@example.com)",
		"(3, user.name, user-name@example.com)",
	}

	if !containsLines(result, expected) {
		t.Errorf("Expected special characters to work, got %v", result)
	}
}

// Performance and stress tests
func TestConcurrentAccess(t *testing.T) {
	filename := "test_concurrent.db"
	cleanupTestFile(filename)
	defer cleanupTestFile(filename)

	// This is a basic test - in a real scenario you'd want actual concurrency
	result := runScript(t, filename, []string{
		"insert 1 user1 person1@example.com",
		"select",
		"insert 2 user2 person2@example.com",
		"select",
		"insert 3 user3 person3@example.com",
		"select",
		".exit",
	})

	// Verify all operations completed successfully
	expected := []string{
		"(1, user1, person1@example.com)",
		"(1, user1, person1@example.com)",
		"(2, user2, person2@example.com)",
		"(1, user1, person1@example.com)",
		"(2, user2, person2@example.com)",
		"(3, user3, person3@example.com)",
	}

	if !containsLines(result, expected) {
		t.Errorf("Concurrent access test failed, got %v", result)
	}
}

func TestLargeDatasetPerformance(t *testing.T) {
	filename := "test_performance.db"
	cleanupTestFile(filename)
	defer cleanupTestFile(filename)

	start := time.Now()

	// Insert 100 rows
	commands := []string{}
	for i := 1; i <= 100; i++ {
		commands = append(commands, fmt.Sprintf("insert %d user%d user%d@example.com", i, i, i))
	}
	commands = append(commands, "select", ".exit")

	result := runScript(t, filename, commands)
	duration := time.Since(start)

	// Verify all data was inserted
	rowCount := 0
	for _, line := range result {
		if strings.Contains(line, "user") && strings.Contains(line, "@example.com") {
			rowCount++
		}
	}

	if rowCount != 100 {
		t.Errorf("Expected 100 rows, got %d", rowCount)
	}

	// Performance should be reasonable (adjust threshold as needed)
	if duration > 5*time.Second {
		t.Errorf("Performance test took too long: %v", duration)
	}

	t.Logf("Inserted 100 rows in %v", duration)
}

// File handling tests
func TestMissingFilename(t *testing.T) {
	cmd := exec.Command("./db-go")
	output, err := cmd.CombinedOutput()

	if err == nil {
		t.Errorf("Expected error when no filename provided")
	}

	if !strings.Contains(string(output), "Must supply a db filename") {
		t.Errorf("Expected filename error message, got %s", string(output))
	}
}

func TestInvalidFilename(t *testing.T) {
	// Test with a filename that can't be created (directory)
	cmd := exec.Command("./db-go", "/dev/null")
	output, err := cmd.CombinedOutput()

	// This might fail or succeed depending on system, but shouldn't crash
	if err != nil && !strings.Contains(string(output), "Unable to openfile") {
		t.Errorf("Expected file open error, got %s", string(output))
	}
}

// Data integrity tests
func TestDataIntegrity(t *testing.T) {
	filename := "test_integrity.db"
	cleanupTestFile(filename)
	defer cleanupTestFile(filename)

	// Insert data with various edge cases
	result := runScript(t, filename, []string{
		"insert 1 a a@b.com",
		"insert 2 very_long_username_but_under_limit very_long_email_but_under_limit@example.com",
		"insert 3 123 456@789.com",
		"insert 4 user4 user4@example.com",
		"select",
		".exit",
	})

	expected := []string{
		"(1, a, a@b.com)",
		"(2, very_long_username_but_under_limit, very_long_email_but_under_limit@example.com)",
		"(3, 123, 456@789.com)",
		"(4, user4, user4@example.com)",
	}

	if !containsLines(result, expected) {
		t.Errorf("Data integrity test failed, got %v", result)
	}
}

// Boundary tests
func TestBoundaryValues(t *testing.T) {
	filename := "test_boundary.db"
	cleanupTestFile(filename)
	defer cleanupTestFile(filename)

	// Test maximum allowed values
	maxUsername := strings.Repeat("a", 32) // Exactly 32 chars
	maxEmail := strings.Repeat("a", 255)   // Exactly 255 chars

	result := runScript(t, filename, []string{
		fmt.Sprintf("insert 1 %s %s", maxUsername, maxEmail),
		"select",
		".exit",
	})

	expected := []string{
		fmt.Sprintf("(1, %s, %s)", maxUsername, maxEmail),
	}

	if !containsLines(result, expected) {
		t.Errorf("Boundary test failed, got %v", result)
	}
}

// Integration tests
func TestFullWorkflow(t *testing.T) {
	filename := "test_workflow.db"
	cleanupTestFile(filename)
	defer cleanupTestFile(filename)

	// Simulate a complete workflow
	result := runScript(t, filename, []string{
		"insert 1 alice alice@example.com",
		"select",
		"insert 2 bob bob@example.com",
		"select",
		"insert 3 charlie charlie@example.com",
		"select",
		".exit",
	})

	// Verify final state
	finalExpected := []string{
		"(1, alice, alice@example.com)",
		"(2, bob, bob@example.com)",
		"(3, charlie, charlie@example.com)",
	}

	if !containsLines(result, finalExpected) {
		t.Errorf("Full workflow test failed, got %v", result)
	}
}

// Benchmark tests
func BenchmarkInsert(b *testing.B) {
	filename := "benchmark.db"
	cleanupTestFile(filename)
	defer cleanupTestFile(filename)

	// Warm up
	runScript(nil, filename, []string{
		"insert 1 user1 user1@example.com",
		".exit",
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runScript(nil, filename, []string{
			fmt.Sprintf("insert %d user%d user%d@example.com", i+2, i+2, i+2),
			".exit",
		})
	}
}

func BenchmarkSelect(b *testing.B) {
	filename := "benchmark_select.db"
	cleanupTestFile(filename)
	defer cleanupTestFile(filename)

	// Prepare data
	commands := []string{}
	for i := 1; i <= 100; i++ {
		commands = append(commands, fmt.Sprintf("insert %d user%d user%d@example.com", i, i, i))
	}
	commands = append(commands, ".exit")
	runScript(nil, filename, commands)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runScript(nil, filename, []string{
			"select",
			".exit",
		})
	}
} 