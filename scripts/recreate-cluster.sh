#!/usr/bin/env bash
set -euo pipefail
"$(dirname "$0")/teardown-cluster.sh"
"$(dirname "$0")/bootstrap-cluster.sh"
