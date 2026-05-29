#!/usr/bin/env bash
# Bootstraps a venv, generates gRPC stubs, and runs the server test.
# Mirrors FastestServerFetcher from the Zcash Android Wallet SDK.

set -euo pipefail
cd "$(dirname "$0")"

if ! command -v python3 >/dev/null 2>&1; then
    echo "python3 is required" >&2
    exit 1
fi

if [ ! -d .venv ]; then
    echo "Creating virtualenv..."
    python3 -m venv .venv
fi

# shellcheck disable=SC1091
source .venv/bin/activate

if [ ! -f .venv/.deps-installed ]; then
    echo "Installing dependencies..."
    pip install --quiet --upgrade pip
    pip install --quiet -r requirements.txt
    touch .venv/.deps-installed
fi

mkdir -p generated
if [ ! -f generated/service_pb2.py ] || [ proto/service.proto -nt generated/service_pb2.py ]; then
    echo "Generating gRPC stubs..."
    python -m grpc_tools.protoc \
        --proto_path=proto \
        --python_out=generated \
        --grpc_python_out=generated \
        proto/service.proto proto/compact_formats.proto
fi

# Force gRPC's name resolution through the system resolver (getaddrinfo)
# rather than the bundled c-ares. On Linux, c-ares stalls on AAAA queries
# for IPv4-only hosts whose authoritative nameservers don't promptly
# answer with NODATA..
export GRPC_DNS_RESOLVER=${GRPC_DNS_RESOLVER:-native}

exec python server_test.py "$@"
