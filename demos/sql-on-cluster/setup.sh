#!/usr/bin/env bash
# ============================================================
# Set up the SQL demo environment on an EC2 instance.
#
# What this does:
#   1. Installs uv (fast Python package manager)
#   2. Creates a virtual env and installs packages (duckdb, boto3, etc.)
#   3. Registers a Jupyter kernel so VS Code can find it
#   4. Creates a spill directory for DuckDB temp files
#
# Usage:
#   ssh -i ~/.ssh/your-key.pem ubuntu@<ec2-ip>
#   git clone <repo-url> sql-demo && cd sql-demo
#   bash setup.sh
#
# To use on your own account:
#   - Works on any Ubuntu EC2 instance with 16+ GB RAM
#   - The instance needs an IAM instance profile for S3 access
#     (see textbook: Instance Profiles section)
#   - If using Amazon Linux (EMR), change "sudo" commands as needed
# ============================================================

set -euo pipefail

echo "=== SQL-on-the-cluster demo setup ==="

# ---------- 1. Install uv if not present ----------
if ! command -v uv &>/dev/null; then
    echo "Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    # shellcheck disable=SC1091
    source "$HOME/.local/bin/env"
fi
echo "uv $(uv --version)"

# ---------- 2. Create env and install packages ----------
uv sync
echo "Packages installed."

# ---------- 3. Register Jupyter kernel ----------
# This makes the env visible to VS Code's kernel picker
# when connected via Remote SSH. After connecting, open any
# .py file with # %% markers and select "SQL Demo (uv)" as
# the kernel.
uv run python -m ipykernel install \
    --user \
    --name sql-demo \
    --display-name "SQL Demo (uv)"
echo "Kernel 'SQL Demo (uv)' registered."

# ---------- 4. Ensure spill directory exists ----------
# DuckDB writes temp files here when a query exceeds memory_limit.
# SET temp_directory = '/tmp/duckdb'; in the Python scripts.
sudo mkdir -p /tmp/duckdb
sudo chmod 777 /tmp/duckdb

echo ""
echo "Done. Next steps:"
echo "  1. In VS Code, connect via Remote SSH to this machine"
echo "  2. Open the sql-demo/ folder"
echo "  3. Open 01_duckdb_s3.py and click 'Run Cell' on any # %% line"
echo "  4. When prompted for kernel, select 'SQL Demo (uv)'"
echo ""
echo "VS Code extensions needed (install locally, not on remote):"
echo "  - Remote - SSH (ms-vscode.remote-ssh)"
echo "  - Python (ms-python.python)"
