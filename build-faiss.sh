#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="${SCRIPT_DIR}/output"
IMAGE_NAME="faiss-deb-builder"

echo "=== Building Faiss .deb package ==="
echo "Output directory: ${OUTPUT_DIR}"

# Create output directory
mkdir -p "${OUTPUT_DIR}"

# Build the Docker image (BuildKit is required for RUN --mount=type=cache in Dockerfile)
echo "=== Building Docker image ==="
DOCKER_BUILDKIT=1 docker build -t "${IMAGE_NAME}" "${SCRIPT_DIR}"

# Mount output directory and copy .deb file and debug log from container
echo "=== Extracting .deb package ==="
docker run --rm \
    -v "${OUTPUT_DIR}:/output:rw" \
    "${IMAGE_NAME}" \
    bash -c 'find /workspace/faiss/build -maxdepth 1 -name "*.deb" -type f -exec cp {} /output/ \; 2>/dev/null; cp /tmp/debug.log /output/debug.log 2>/dev/null || true'

# List the results
echo ""
echo "=== Build complete ==="
ls -lh "${OUTPUT_DIR}/"

# Find and display the .deb file name
DEB_FILE=$(find "${OUTPUT_DIR}" -name "*.deb" -type f | head -n 1)
if [ -n "${DEB_FILE}" ]; then
    echo ""
    echo "Generated .deb package: ${DEB_FILE}"
    echo ""
    echo "To install on the host system, run:"
    echo "  sudo dpkg -i ${DEB_FILE}"
else
    echo "ERROR: No .deb file was generated!"
    exit 1
fi
