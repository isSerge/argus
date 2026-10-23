#!/bin/bash

# This script iterates through all numbered example directories, extracts the
# dry-run command from their README.md, and executes it, verifying that the output
# contains the expected dry-run report format. If any command fails or doesn't
# produce the expected output format, the script exits with an error.

export RUST_LOG=warn
set -e

EXAMPLES_DIR="examples"

# Find all numbered example directories
for example_dir in $(find "$EXAMPLES_DIR" -mindepth 1 -maxdepth 1 -type d -name "[0-9]*" | sort); do
    echo "========================================================================"
    echo "Running example: $example_dir"
    echo "========================================================================"

    readme_path="$example_dir/README.md"
    if [ ! -f "$readme_path" ]; then
        echo "WARNING: README.md not found in $example_dir. Skipping."
        continue
    fi

    # Extract the full dry-run command from the README.md
    command=$(grep -o 'cargo run --release -- dry-run .*' "$readme_path" | head -n 1)

    if [ -z "$command" ]; then
        echo "WARNING: Could not find dry-run command in $readme_path. Skipping."
        continue
    fi

    echo "Executing command: $command"
    echo "---"
    
    # Check for a docker-compose.yml file in the example directory
    docker_compose_file="$example_dir/docker-compose.yml"
    if [ -f "$docker_compose_file" ]; then
        echo "--- Found docker-compose.yml, setting up services for $example_dir ---"
        docker compose -f "$docker_compose_file" up -d
        # Give services some time to start up
        sleep 15
    fi
    
    # Execute the command safely by parsing it into an array and running it directly.
    read -r -a cmd_array <<< "$command"
    output=$("${cmd_array[@]}")
    exit_code=$?

    # Teardown Docker Compose services if they were started, regardless of the outcome
    if [ -f "$docker_compose_file" ]; then
        echo "--- Tearing down services for $example_dir ---"
        docker compose -f "$docker_compose_file" down
    fi

    if [ $exit_code -ne 0 ]; then
        echo "ERROR: Command failed with exit code $exit_code"
        echo "$output"
        exit 1
    fi

    # Check if the output contains the expected dry-run report format
    if ! echo "$output" | grep -q "Dry Run Report"; then
        echo "ERROR: Example did not produce expected dry-run report format."
        echo "$output"
        exit 1
    fi

    # Check if any matches were found - all examples should have at least one match
    if echo "$output" | grep -q "Total Matches Found: 0"; then
        echo "ERROR: Example found 0 matches. All examples should be configured to find at least one match."
        echo "$output"
        exit 1
    fi

    # If all checks pass, print the output
    echo "$output"
    echo
done

echo "========================================================================"
echo "All examples have been successfully verified."
echo "========================================================================"

exit 0
