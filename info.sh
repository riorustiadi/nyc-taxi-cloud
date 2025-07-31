#!/bin/bash

# NYC Taxi Cloud Pipeline - Complete Information Guide

echo "=== NYC Taxi Cloud Pipeline - Information Guide ==="
echo ""

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'

print_header() {
    echo -e "${BLUE}📋 $1${NC}"
    echo "----------------------------------------"
}

print_command() {
    echo -e "${GREEN}   $1${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ️  $1${NC}"
}

print_path() {
    echo -e "${CYAN}📁 $1${NC}"
}

# 1. BUILD COMMANDS
print_header "BUILD IMAGE"
print_command "docker-compose build"
print_command "docker-compose build --no-cache  # Force rebuild"
print_info "Builds production Docker image without tests"
echo ""

# 2. DEPLOYMENT COMMANDS
print_header "DEPLOYMENT COMMANDS"
print_command "docker-compose up -d           # Start in background"
print_command "docker-compose up              # Start with logs visible"
print_command "docker-compose down            # Stop and remove containers"
print_command "docker-compose restart         # Restart containers"
echo ""

# 3. MONITORING COMMANDS
print_header "MONITORING & LOGS"
print_command "docker-compose ps              # Check container status"
print_command "docker-compose logs -f         # Follow real-time logs"
print_command "docker-compose logs --tail=50  # Last 50 log lines"
print_command "docker inspect nyc-taxi-cloud --format='{{.State.Health.Status}}'"
print_info "Check container health status"
echo ""

# 4. DATA & FILES COMMANDS
print_header "DATA & FILES MONITORING"
print_path "/home/riorustiadi/docker/volumes/nyc-taxi-cloud/data/"
print_command "ls -la /home/riorustiadi/docker/volumes/nyc-taxi-cloud/data/"
print_command "watch 'ls -la /home/riorustiadi/docker/volumes/nyc-taxi-cloud/data/'"
print_path "/home/riorustiadi/docker/volumes/nyc-taxi-cloud/logs/"
print_command "ls -la /home/riorustiadi/docker/volumes/nyc-taxi-cloud/logs/"
print_command "tail -f /home/riorustiadi/docker/volumes/nyc-taxi-cloud/logs/pipeline_*.log"
echo ""

# 5. DEBUG COMMANDS
print_header "DEBUG & TROUBLESHOOTING"
print_command "docker-compose run --rm nyc-taxi-pipeline bash"
print_info "Interactive shell inside container"
print_command "docker images | grep nyc-taxi"
print_info "Check built images"
print_command "docker system df"
print_info "Check Docker disk usage"
print_command "docker stats nyc-taxi-cloud"
print_info "Real-time resource usage"
echo ""

# 6. MAINTENANCE COMMANDS
print_header "MAINTENANCE & CLEANUP"
print_command "docker system prune -f"
print_info "Clean unused containers, networks, images"
print_command "docker-compose down --rmi all"
print_info "Stop and remove all images"
print_command "docker volume ls"
print_info "List all volumes"
print_command "docker network ls"
print_info "List all networks"
echo ""

# 7. UPDATE WORKFLOW
print_header "UPDATE WORKFLOW"
print_command "git pull"
print_info "Pull latest code changes"
print_command "docker-compose down"
print_info "Stop current pipeline"
print_command "docker-compose build --no-cache"
print_info "Rebuild with latest changes"
print_command "docker-compose up -d"
print_info "Start updated pipeline"
echo ""

# 8. QUICK WORKFLOW
print_header "QUICK WORKFLOW EXAMPLES"
echo -e "${YELLOW}🚀 First Time Setup:${NC}"
print_command "./setup.sh"
print_command "docker-compose build"
print_command "docker-compose up -d"
echo ""

echo -e "${YELLOW}🔄 Daily Operations:${NC}"
print_command "docker-compose logs -f          # Monitor logs"
print_command "docker-compose ps               # Check status"
print_command "ls -la /home/riorustiadi/docker/volumes/nyc-taxi-cloud/data/"
echo ""

echo -e "${YELLOW}🛠️ Troubleshooting:${NC}"
print_command "docker-compose down"
print_command "docker-compose build --no-cache"
print_command "docker-compose up"
echo ""

# 9. FILE LOCATIONS
print_header "IMPORTANT FILE LOCATIONS"
print_path "Project: /home/riorustiadi/nyc-taxi-cloud/"
print_path "Data:    /home/riorustiadi/docker/volumes/nyc-taxi-cloud/data/"
print_path "Logs:    /home/riorustiadi/docker/volumes/nyc-taxi-cloud/logs/"
print_path "Config:  ./docker-compose.yml"
echo ""

# 10. USEFUL ALIASES
print_header "USEFUL ALIASES (Optional)"
print_info "Add to ~/.bashrc for shortcuts:"
print_command "alias dcp='docker-compose'"
print_command "alias dcl='docker-compose logs -f'"
print_command "alias dcs='docker-compose ps'"
print_command "alias nycdata='ls -la /home/riorustiadi/docker/volumes/nyc-taxi-cloud/data/'"
print_command "alias nyclogs='ls -la /home/riorustiadi/docker/volumes/nyc-taxi-cloud/logs/'"
echo ""

echo "=== End of Information Guide ==="
echo ""
echo -e "${GREEN}💡 Tip: Keep this guide handy for daily operations!${NC}"
echo -e "${YELLOW}📖 Usage: ./info.sh to display this guide anytime${NC}"