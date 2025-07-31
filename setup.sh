#!/bin/bash

echo "=== Setting up NYC Taxi Cloud Pipeline ==="
echo "User: $(whoami) (UID: $(id -u), GID: $(id -g))"
echo ""

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function untuk print colored output
print_status() {
    echo -e "${GREEN}✅${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠️${NC} $1"
}

print_error() {
    echo -e "${RED}❌${NC} $1"
}

# Check prerequisites
echo "Checking prerequisites..."

# Check Docker
if ! command -v docker &> /dev/null; then
    print_error "Docker not found. Please install Docker first."
    echo "Ubuntu install: sudo apt update && sudo apt install docker.io"
    exit 1
fi
print_status "Docker found"

# Check Docker Compose
if ! command -v docker-compose &> /dev/null; then
    print_error "Docker Compose not found. Please install Docker Compose first."
    echo "Ubuntu install: sudo apt install docker-compose"
    exit 1
fi
print_status "Docker Compose found"

# Check Docker daemon
if ! docker info &> /dev/null; then
    print_error "Docker daemon not running. Please start Docker service."
    echo "Ubuntu: sudo systemctl start docker"
    exit 1
fi
print_status "Docker daemon running"

# Create volume directories
echo ""
echo "Creating volume directories..."
mkdir -p /home/riorustiadi/docker/volumes/nyc-taxi-cloud/data
mkdir -p /home/riorustiadi/docker/volumes/nyc-taxi-cloud/logs
print_status "Volume directories created"

# Set proper permissions
echo "Setting permissions..."
chmod -R 755 /home/riorustiadi/docker/volumes/nyc-taxi-cloud/
print_status "Permissions set"

# Test Docker access
echo ""
echo "Testing Docker access..."
if docker ps &> /dev/null; then
    print_status "Docker access confirmed"
else
    print_warning "Docker access requires sudo. Adding user to docker group..."
    sudo usermod -aG docker $(whoami)
    print_warning "Please log out and log back in, then run this script again"
    exit 1
fi

echo ""
echo "=== Setup Completed Successfully! ==="
echo ""
echo "📁 Data storage: /home/riorustiadi/docker/volumes/nyc-taxi-cloud/data"
echo "📋 Log storage:  /home/riorustiadi/docker/volumes/nyc-taxi-cloud/logs"
echo ""
echo "🚀 Next steps:"
echo "1. Build image:     docker-compose build"
echo "2. Run pipeline:    docker-compose up -d"
echo "3. View logs:       docker-compose logs -f"
echo "4. Run tests:       docker-compose --profile testing run --rm nyc-taxi-tests"
echo "5. Stop pipeline:   docker-compose down"
echo ""
echo "🧪 Testing commands:"
echo "   Run all tests:   docker-compose --profile testing run --rm nyc-taxi-tests"
echo "   Interactive:     docker-compose run --rm nyc-taxi-pipeline bash"
echo "   Check health:    docker-compose ps"
echo ""
echo "📊 Monitoring:"
echo "   View data:       ls -la /home/riorustiadi/docker/volumes/nyc-taxi-cloud/data/"
echo "   View logs:       ls -la /home/riorustiadi/docker/volumes/nyc-taxi-cloud/logs/"