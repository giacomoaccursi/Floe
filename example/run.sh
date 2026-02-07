#!/bin/bash
#
# Script to run the ETL Framework ingestion example
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "=========================================="
echo "ETL Framework - Ingestion Example"
echo "=========================================="
echo ""
echo "Project root: $PROJECT_ROOT"
echo "Example dir:  $SCRIPT_DIR"
echo ""

cd "$PROJECT_ROOT"

# Clean previous output
if [ -d "$SCRIPT_DIR/output/validated" ]; then
    echo "Cleaning previous output..."
    rm -rf "$SCRIPT_DIR/output/validated/"*
    rm -rf "$SCRIPT_DIR/output/rejected/"*
    rm -rf "$SCRIPT_DIR/output/metadata/"*
    echo "✓ Output cleaned"
    echo ""
fi

# Run the example
echo "Running ingestion example..."
echo ""
sbt "runMain com.etl.framework.example.IngestionExample"
