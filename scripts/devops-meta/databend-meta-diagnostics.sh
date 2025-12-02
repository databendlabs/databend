#!/bin/bash

# Databend Meta Server Diagnostic Script
# Purpose: Identify root causes for node failures, OOM errors, and query log issues
# Author: Generated for databend-meta troubleshooting

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Global variables
REPORT_FILE="databend-meta-diagnostic-$(date +%Y%m%d-%H%M%S).txt"
TEMP_DIR="/tmp/databend-meta-diag-$$"
DATABEND_PROCESSES=""

# Helper functions
log_step() {
    echo -e "${BLUE}[$(date '+%H:%M:%S')] STEP: $1${NC}"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] STEP: $1" >> "$REPORT_FILE"
}

log_result() {
    echo -e "${GREEN}[$(date '+%H:%M:%S')] RESULT: $1${NC}"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] RESULT: $1" >> "$REPORT_FILE"
}

log_warning() {
    echo -e "${YELLOW}[$(date '+%H:%M:%S')] WARNING: $1${NC}"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] WARNING: $1" >> "$REPORT_FILE"
}

log_error() {
    echo -e "${RED}[$(date '+%H:%M:%S')] ERROR: $1${NC}"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $1" >> "$REPORT_FILE"
}

create_temp_dir() {
    mkdir -p "$TEMP_DIR"
    trap "rm -rf $TEMP_DIR" EXIT
}

# Initialize diagnostic report
init_report() {
    log_step "Initializing databend-meta diagnostic report"
    
    cat > "$REPORT_FILE" << EOF
================================================================================
DATABEND META SERVER DIAGNOSTIC REPORT
Generated: $(date '+%Y-%m-%d %H:%M:%S')
Hostname: $(hostname)
================================================================================

EOF
    
    log_result "Report initialized: $REPORT_FILE"
}

# Check system basic information
check_system_info() {
    log_step "Collecting system information"
    
    {
        echo "=== SYSTEM INFORMATION ==="
        echo "Hostname: $(hostname)"
        echo "Uptime: $(uptime)"
        echo "Kernel: $(uname -r)"
        echo "Distribution: $(cat /etc/os-release | grep PRETTY_NAME | cut -d'"' -f2 2>/dev/null || echo 'Unknown')"
        echo "Architecture: $(uname -m)"
        echo "CPU cores: $(nproc)"
        echo ""
    } >> "$REPORT_FILE"
    
    log_result "System information collected"
}

# Check memory and OOM killer activity
check_memory_oom() {
    log_step "Checking memory status and OOM killer activity"
    
    {
        echo "=== MEMORY STATUS ==="
        free -h
        echo ""
        
        echo "=== SWAP USAGE ==="
        swapon --show 2>/dev/null || echo "No swap configured"
        echo ""
        
        echo "=== OOM KILLER ACTIVITY (last 100 entries) ==="
        dmesg | grep -i "killed process\|out of memory\|oom-killer\|memory: usage" | tail -100 || echo "No OOM killer activity found in dmesg"
        echo ""
        
        echo "=== RECENT OOM KILLS IN SYSTEM LOG ==="
        journalctl --since "7 days ago" | grep -i "killed process\|out of memory\|oom" | tail -50 || echo "No recent OOM kills found in journal"
        echo ""
    } >> "$REPORT_FILE"
    
    # Check if databend processes were killed by OOM
    local oom_databend=$(dmesg | grep -i "killed process" | grep -i databend | wc -l)
    if [ "$oom_databend" -gt 0 ]; then
        log_warning "Found $oom_databend databend processes killed by OOM killer"
        {
            echo "=== DATABEND PROCESSES KILLED BY OOM ==="
            dmesg | grep -i "killed process" | grep -i databend
            echo ""
        } >> "$REPORT_FILE"
    else
        log_result "No databend processes found in OOM killer logs"
    fi
}

# Check databend-meta processes
check_databend_processes() {
    log_step "Analyzing databend-meta processes"
    
    # Find running databend processes
    DATABEND_PROCESSES=$(pgrep -f "databend.*meta" || true)
    
    {
        echo "=== DATABEND META PROCESSES ==="
        if [ -n "$DATABEND_PROCESSES" ]; then
            echo "Running databend-meta processes:"
            ps aux | grep -E "databend.*meta" | grep -v grep
            echo ""
            
            echo "=== PROCESS RESOURCE USAGE ==="
            for pid in $DATABEND_PROCESSES; do
                if [ -d "/proc/$pid" ]; then
                    echo "PID $pid:"
                    echo "  Command: $(cat /proc/$pid/cmdline | tr '\0' ' ')"
                    echo "  Memory (VmRSS): $(grep VmRSS /proc/$pid/status 2>/dev/null || echo 'N/A')"
                    echo "  Memory (VmSize): $(grep VmSize /proc/$pid/status 2>/dev/null || echo 'N/A')"
                    echo "  Threads: $(grep Threads /proc/$pid/status 2>/dev/null || echo 'N/A')"
                    echo "  File descriptors: $(ls /proc/$pid/fd 2>/dev/null | wc -l || echo 'N/A')"
                    echo ""
                fi
            done
        else
            echo "No databend-meta processes currently running"
        fi
        echo ""
    } >> "$REPORT_FILE"
    
    if [ -n "$DATABEND_PROCESSES" ]; then
        log_result "Found $(echo $DATABEND_PROCESSES | wc -w) databend-meta processes"
    else
        log_warning "No databend-meta processes currently running"
    fi
}

# Check system resource limits and usage
check_system_resources() {
    log_step "Checking system resource usage and limits"
    
    {
        echo "=== CPU USAGE ==="
        top -bn1 | head -20
        echo ""
        
        echo "=== LOAD AVERAGE ==="
        cat /proc/loadavg
        echo ""
        
        echo "=== DISK USAGE ==="
        df -h
        echo ""
        
        echo "=== INODE USAGE ==="
        df -i
        echo ""
        
        echo "=== MEMORY USAGE BY PROCESS ==="
        ps aux --sort=-%mem | head -20
        echo ""
        
        echo "=== SYSTEM LIMITS ==="
        echo "Max open files (system): $(cat /proc/sys/fs/file-max)"
        echo "Current open files: $(cat /proc/sys/fs/file-nr | cut -f1)"
        echo "Max processes: $(cat /proc/sys/kernel/pid_max)"
        echo "Max memory map areas: $(cat /proc/sys/vm/max_map_count)"
        echo ""
        
        if [ -n "$DATABEND_PROCESSES" ]; then
            echo "=== DATABEND PROCESS LIMITS ==="
            for pid in $DATABEND_PROCESSES; do
                if [ -d "/proc/$pid" ]; then
                    echo "PID $pid limits:"
                    cat /proc/$pid/limits 2>/dev/null | grep -E "open files|processes|address space" || echo "  Unable to read limits"
                    echo ""
                fi
            done
        fi
    } >> "$REPORT_FILE"
    
    log_result "System resource information collected"
}

# Check databend-meta logs
check_databend_logs() {
    log_step "Analyzing databend-meta logs"
    
    {
        echo "=== DATABEND META LOG ANALYSIS ==="
        
        # Common log locations to check
        local log_paths=(
            "/var/log/databend"
            "/opt/databend/logs"
            "/usr/local/databend/logs"
            "/home/*/databend/logs"
            "$(pwd)/logs"
            "./logs"
        )
        
        local found_logs=false
        
        for log_path in "${log_paths[@]}"; do
            if [ -d "$log_path" ] && [ "$(find "$log_path" -name "*meta*log*" -o -name "*databend*log*" 2>/dev/null | wc -l)" -gt 0 ]; then
                echo "Found logs in: $log_path"
                find "$log_path" -name "*meta*log*" -o -name "*databend*log*" | head -10
                echo ""
                found_logs=true
                
                # Analyze recent errors in logs
                echo "=== RECENT ERRORS IN LOGS (last 100 lines) ==="
                find "$log_path" -name "*meta*log*" -o -name "*databend*log*" | while read -r logfile; do
                    if [ -f "$logfile" ]; then
                        echo "Analyzing: $logfile"
                        tail -100 "$logfile" | grep -i -E "error|panic|fatal|oom|memory|fail" | tail -20 || echo "No recent errors found"
                        echo ""
                    fi
                done
                break
            fi
        done
        
        if [ "$found_logs" = false ]; then
            echo "No databend-meta log files found in standard locations"
            echo "Checked locations: ${log_paths[*]}"
            echo ""
            
            # Try to find logs using systemd if service is running
            echo "=== CHECKING SYSTEMD LOGS ==="
            journalctl -u databend-meta --since "24 hours ago" --no-pager | tail -100 2>/dev/null || echo "No systemd logs found for databend-meta service"
            echo ""
        fi
    } >> "$REPORT_FILE"
    
    log_result "Log analysis completed"
}

# Check network and connectivity
check_network() {
    log_step "Checking network configuration and connectivity"
    
    {
        echo "=== NETWORK CONFIGURATION ==="
        ss -tlnp | grep -E ":9191|:8080|:3307|:8000" || echo "No databend-related ports found listening"
        echo ""
        
        echo "=== NETWORK CONNECTIONS ==="
        if [ -n "$DATABEND_PROCESSES" ]; then
            for pid in $DATABEND_PROCESSES; do
                echo "Connections for PID $pid:"
                lsof -p "$pid" -i 2>/dev/null | head -20 || echo "Unable to check connections for PID $pid"
                echo ""
            done
        fi
        
        echo "=== FIREWALL STATUS ==="
        if command -v ufw >/dev/null; then
            ufw status 2>/dev/null || echo "UFW not active"
        elif command -v firewall-cmd >/dev/null; then
            firewall-cmd --state 2>/dev/null || echo "Firewalld not active"
        elif command -v iptables >/dev/null; then
            iptables -L -n | head -20 2>/dev/null || echo "Unable to check iptables"
        else
            echo "No common firewall tools found"
        fi
        echo ""
    } >> "$REPORT_FILE"
    
    log_result "Network analysis completed"
}

# Generate final diagnostic report
generate_final_report() {
    log_step "Generating final diagnostic summary"
    
    {
        echo ""
        echo "================================================================================"
        echo "DIAGNOSTIC SUMMARY"
        echo "================================================================================"
        echo ""
        
        echo "=== CRITICAL FINDINGS ==="
        
        # Check for OOM issues
        local oom_count=$(dmesg | grep -i "killed process" | grep -i databend | wc -l)
        if [ "$oom_count" -gt 0 ]; then
            echo "❌ CRITICAL: $oom_count databend processes killed by OOM killer"
        fi
        
        # Check memory pressure
        local mem_available=$(free -m | awk 'NR==2{printf "%.0f", $7/$2*100}')
        if [ "$mem_available" -lt 10 ]; then
            echo "❌ CRITICAL: Very low available memory (${mem_available}%)"
        elif [ "$mem_available" -lt 20 ]; then
            echo "⚠️  WARNING: Low available memory (${mem_available}%)"
        fi
        
        # Check if processes are running
        if [ -z "$DATABEND_PROCESSES" ]; then
            echo "❌ CRITICAL: No databend-meta processes currently running"
        else
            echo "✅ INFO: $(echo $DATABEND_PROCESSES | wc -w) databend-meta processes running"
        fi
        
        # Check disk space
        local disk_usage=$(df / | awk 'NR==2{print $5}' | sed 's/%//')
        if [ "$disk_usage" -gt 90 ]; then
            echo "❌ CRITICAL: Root filesystem ${disk_usage}% full"
        elif [ "$disk_usage" -gt 80 ]; then
            echo "⚠️  WARNING: Root filesystem ${disk_usage}% full"
        fi
        
        echo ""
        echo "=== RECOMMENDATIONS ==="
        echo "1. Check system logs for OOM killer activity"
        echo "2. Monitor memory usage during peak loads"
        echo "3. Consider increasing system memory or optimizing databend-meta configuration"
        echo "4. Verify databend-meta service configuration and startup scripts"
        echo "5. Check application logs for query execution patterns"
        echo ""
        
        echo "=== NEXT STEPS ==="
        echo "1. Review the detailed findings above"
        echo "2. Share this report with databend support team"
        echo "3. Consider implementing monitoring for memory usage"
        echo "4. Set up log rotation if not already configured"
        echo ""
        
        echo "Report generated: $(date '+%Y-%m-%d %H:%M:%S')"
        echo "Report location: $(pwd)/$REPORT_FILE"
        
    } >> "$REPORT_FILE"
    
    log_result "Final diagnostic report generated"
}

# Main execution
main() {
    echo -e "${BLUE}Databend Meta Server Diagnostic Tool${NC}"
    echo "======================================"
    
    create_temp_dir
    init_report
    
    check_system_info
    check_memory_oom
    check_databend_processes  
    check_system_resources
    check_databend_logs
    check_network
    generate_final_report
    
    echo ""
    echo -e "${GREEN}Diagnostic completed successfully!${NC}"
    echo -e "Report saved to: ${YELLOW}$REPORT_FILE${NC}"
    echo ""
    echo -e "${BLUE}To view the report:${NC}"
    echo -e "  cat $REPORT_FILE"
    echo ""
    echo -e "${BLUE}To share with support:${NC}"
    echo -e "  Send the file: $REPORT_FILE"
}

# Run main function
main "$@"