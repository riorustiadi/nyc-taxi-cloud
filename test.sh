#!/bin/bash

# NYC Taxi Cloud Pipeline Testing Script

echo "=== NYC Taxi Cloud Pipeline Testing ==="

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

print_status() {
    echo -e "${GREEN}✅${NC} $1"
}

print_info() {
    echo -e "${YELLOW}ℹ️${NC} $1"
}

print_error() {
    echo -e "${RED}❌${NC} $1"
}

# Build image first
print_info "Building Docker image..."
if docker-compose build; then
    print_status "Build successful"
else
    print_error "Build failed"
    exit 1
fi

# Run tests
print_info "Running tests..."
if docker-compose --profile testing run --rm nyc-taxi-tests; then
    print_status "All tests passed!"
    
    # Option to run pipeline
    echo ""
    read -p "Run the actual pipeline? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_info "Starting pipeline..."
        docker-compose up -d
        print_status "Pipeline started. Use 'docker-compose logs -f' to view progress"
        echo ""
        echo "📊 Monitor progress:"
        echo "   Logs: docker-compose logs -f"
        echo "   Data: ls -la /home/riorustiadi/docker/volumes/nyc-taxi-cloud/data/"
    fi
else
    print_error "Tests failed. Please fix issues before deploying."
    exit 1
fi