#!/bin/bash

# Test script for db-go implementation

echo "Building database..."
go build -o db-go main.go

if [ $? -ne 0 ]; then
    echo "Build failed!"
    exit 1
fi

echo "Running basic functionality tests..."

# Test 1: Basic insert and select
echo "Test 1: Basic insert and select"
echo -e "insert 1 user1 email1@test.com\ninsert 2 user2 email2@test.com\nselect\n.exit" | ./db-go test.db

echo -e "\nTest 2: Duplicate key detection"
echo -e "insert 1 user1 email1@test.com\ninsert 1 user1_duplicate email1_duplicate@test.com\n.exit" | ./db-go test.db

echo -e "\nTest 3: String length validation"
echo -e "insert 3 this_username_is_way_too_long_for_the_column_size_limit email3@test.com\n.exit" | ./db-go test.db

echo -e "\nTest 4: Negative ID"
echo -e "insert -1 user4 email4@test.com\n.exit" | ./db-go test.db

echo -e "\nTest 5: Syntax errors"
echo -e "insert abc user5 email5@test.com\n.exit" | ./db-go test.db

echo -e "\nTest 6: Constants display"
echo -e ".constants\n.exit" | ./db-go test.db

echo -e "\nAll tests completed!" 