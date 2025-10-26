#!/bin/bash

# ARKitScenes Batch Processing Monitor
# Monitor running batch processing jobs

set -e

# Function to show usage
show_usage() {
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  status          Show status of running batch jobs"
    echo "  logs            Show recent log files"
    echo "  tail [FILE]     Tail a specific log file (or latest if not specified)"
    echo "  stop [PID]      Stop a specific process"
    echo "  stopall         Stop all batch processing jobs"
    echo "  clean           Remove old log files (>7 days)"
    echo "  help            Show this help"
    echo ""
    echo "Examples:"
    echo "  $0 status"
    echo "  $0 tail"
    echo "  $0 tail arkitscenes_batch_20241026_143022.log"
    echo "  $0 stop 12345"
}

# Function to show status
show_status() {
    echo "🔍 Checking for running ARKitScenes batch processes..."
    echo ""
    
    # Find running processes
    PROCESSES=$(ps aux | grep "python.*batch_download.py" | grep -v grep || true)
    
    if [[ -z "$PROCESSES" ]]; then
        echo "❌ No batch processing jobs are currently running"
    else
        echo "✅ Found running batch processes:"
        echo ""
        echo "PID    USER     CPU% MEM%  START   COMMAND"
        echo "----------------------------------------------------"
        echo "$PROCESSES" | awk '{printf "%-6s %-8s %-4s %-4s  %-7s %s\n", $2, $1, $3, $4, $9, substr($0, index($0,$11))}'
    fi
    
    echo ""
    echo "📝 Recent log files:"
    ls -lt arkitscenes_batch_*.log 2>/dev/null | head -5 || echo "   No log files found"
}

# Function to show recent logs
show_logs() {
    echo "📝 Recent ARKitScenes batch log files:"
    echo ""
    
    if ! ls arkitscenes_batch_*.log >/dev/null 2>&1; then
        echo "❌ No log files found"
        return
    fi
    
    echo "DATE       TIME     SIZE    FILE"
    echo "----------------------------------------"
    ls -lt arkitscenes_batch_*.log | head -10 | while read -r line; do
        FILE=$(echo "$line" | awk '{print $NF}')
        SIZE=$(echo "$line" | awk '{print $5}')
        DATE=$(echo "$line" | awk '{print $6, $7, $8}')
        printf "%-10s %-8s %s\n" "$DATE" "$(numfmt --to=iec-i --suffix=B $SIZE)" "$FILE"
    done
}

# Function to tail log file
tail_log() {
    if [[ -n "$1" ]]; then
        LOG_FILE="$1"
    else
        # Find the most recent log file
        LOG_FILE=$(ls -t arkitscenes_batch_*.log 2>/dev/null | head -1)
    fi
    
    if [[ -z "$LOG_FILE" || ! -f "$LOG_FILE" ]]; then
        echo "❌ Log file not found: $LOG_FILE"
        echo "Available log files:"
        ls arkitscenes_batch_*.log 2>/dev/null || echo "  No log files found"
        return 1
    fi
    
    echo "📋 Monitoring log file: $LOG_FILE"
    echo "   Press Ctrl+C to stop monitoring"
    echo ""
    
    tail -f "$LOG_FILE"
}

# Function to stop process
stop_process() {
    if [[ -z "$1" ]]; then
        echo "❌ Please specify a PID to stop"
        echo "Use '$0 status' to see running processes"
        return 1
    fi
    
    PID="$1"
    
    if ! ps -p "$PID" > /dev/null; then
        echo "❌ Process $PID not found"
        return 1
    fi
    
    echo "🛑 Stopping process $PID gracefully..."
    kill "$PID"
    
    # Wait a bit and check
    sleep 3
    if ps -p "$PID" > /dev/null; then
        echo "⏳ Process still running, waiting..."
        sleep 5
        if ps -p "$PID" > /dev/null; then
            echo "⚠️  Process still running after 8 seconds"
            echo "   Use 'kill -9 $PID' to force stop if needed"
        else
            echo "✅ Process stopped successfully"
        fi
    else
        echo "✅ Process stopped successfully"
    fi
}

# Function to stop all processes
stop_all() {
    echo "🛑 Stopping all ARKitScenes batch processes..."
    
    PIDS=$(ps aux | grep "python.*batch_download.py" | grep -v grep | awk '{print $2}' || true)
    
    if [[ -z "$PIDS" ]]; then
        echo "❌ No batch processing jobs found"
        return
    fi
    
    echo "Found PIDs: $PIDS"
    
    for pid in $PIDS; do
        echo "Stopping PID $pid..."
        kill "$pid" 2>/dev/null || echo "  Failed to stop $pid"
    done
    
    echo "⏳ Waiting for processes to stop..."
    sleep 5
    
    # Check what's still running
    REMAINING=$(ps aux | grep "python.*batch_download.py" | grep -v grep | awk '{print $2}' || true)
    if [[ -n "$REMAINING" ]]; then
        echo "⚠️  Some processes still running: $REMAINING"
        echo "   Use 'kill -9 PID' to force stop if needed"
    else
        echo "✅ All processes stopped successfully"
    fi
}

# Function to clean old logs
clean_logs() {
    echo "🧹 Cleaning old log files (>7 days)..."
    
    OLD_LOGS=$(find . -name "arkitscenes_batch_*.log" -mtime +7 2>/dev/null || true)
    
    if [[ -z "$OLD_LOGS" ]]; then
        echo "✅ No old log files found"
        return
    fi
    
    echo "Found old log files:"
    echo "$OLD_LOGS"
    echo ""
    read -p "Delete these files? (y/N): " -n 1 -r
    echo ""
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "$OLD_LOGS" | xargs rm -f
        echo "✅ Old log files deleted"
    else
        echo "❌ Cancelled"
    fi
}

# Main command handling
case "${1:-status}" in
    status)
        show_status
        ;;
    logs)
        show_logs
        ;;
    tail)
        tail_log "$2"
        ;;
    stop)
        stop_process "$2"
        ;;
    stopall)
        stop_all
        ;;
    clean)
        clean_logs
        ;;
    help|--help|-h)
        show_usage
        ;;
    *)
        echo "❌ Unknown command: $1"
        echo ""
        show_usage
        exit 1
        ;;
esac