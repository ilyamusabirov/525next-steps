#!/bin/bash
# Bootstrap action for the SparkSQL demo cluster.
# Runs on ALL nodes (primary + core) before Spark starts.
# Installs numpy (required by pyspark.ml) and git (for cloning the demo repo).
set -e
sudo yum install -y git
sudo python3 -m pip install numpy -q
