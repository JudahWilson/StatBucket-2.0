"""Common utilities for script operations"""

import subprocess
import sys
import os
from pathlib import Path

def run_command(cmd, capture_output=True, check=True):
    """Execute a shell command and return result"""
    try:
        result = subprocess.run(cmd, check=check, capture_output=capture_output, text=True)
        return result
    except subprocess.CalledProcessError as e:
        print(f"❌ Command failed: {' '.join(cmd)}")
        if e.stdout:
            print("stdout:", e.stdout)
        if e.stderr:
            print("stderr:", e.stderr)
        if check:
            sys.exit(1)
        return e

def ensure_directory_exists(path):
    """Create directory if it doesn't exist"""
    Path(path).mkdir(parents=True, exist_ok=True)

def print_success(message):
    """Print success message with emoji"""
    print(f"[SUCCESS] {message}")

def print_error(message):
    """Print error message with emoji"""
    print(f"[ERROR] {message}")

def print_info(message):
    """Print info message with emoji"""
    print(f"[INFO] {message}")