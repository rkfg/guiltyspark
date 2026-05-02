# Build Faiss C library and generate .deb package with checkinstall
# Requires Docker BuildKit: export DOCKER_BUILDKIT=1
FROM debian:bookworm-slim

# Use Yandex mirror for faster apt operations
# Debian 12+ uses DEB822 format in /etc/apt/sources.list.d/debian.sources
RUN sed -i 's|deb.debian.org|mirror.yandex.ru|g' /etc/apt/sources.list.d/debian.sources

# Keep downloaded .deb packages in apt cache and allow cache mounts
RUN rm -f /etc/apt/apt.conf.d/docker-clean && \
    echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache

# Install build dependencies with apt cache mounted
RUN --mount=type=cache,target=/var/cache/apt \
    --mount=type=cache,target=/var/lib/apt \
    apt-get update && apt-get install -y --no-install-recommends \
    bash \
    ca-certificates \
    cmake \
    dpkg-dev \
    g++ \
    make \
    git \
    libblas-dev \
    libgflags-dev \
    liblapack-dev \
    patchelf \
    pkg-config && \
    update-ca-certificates

WORKDIR /workspace

# Clone blevesearch/faiss (fork with C API enabled)
RUN git clone --depth 1 --branch bleve https://github.com/blevesearch/faiss.git

# Build Faiss with C API and shared libraries (Python bindings disabled)
WORKDIR /workspace/faiss
RUN cmake -B build \
        -DCMAKE_INSTALL_PREFIX=/usr \
        -DFAISS_ENABLE_GPU=OFF \
        -DFAISS_ENABLE_C_API=ON \
        -DFAISS_ENABLE_PYTHON=OFF \
        -DBUILD_SHARED_LIBS=ON \
        -DBUILD_TESTING=OFF \
        -DCMAKE_CXX_FLAGS="-I/workspace/faiss" \
        -DCMAKE_C_FLAGS="-I/workspace/faiss" \
        . && \
    make -C build -j$(nproc) faiss faiss_c

# Create .deb package manually using dpkg-deb
WORKDIR /workspace/faiss/build
RUN mkdir -p /tmp/staging/usr/lib/x86_64-linux-gnu && \
    # Install to staging
    cmake --install . --prefix /tmp/staging/usr && \
    # Copy all .so files from build directory (libfaiss.so, libfaiss_c.so, etc.)
    cp -a /workspace/faiss/build/c_api/libfaiss_c.so /tmp/staging/usr/lib/x86_64-linux-gnu/ && \
    cp -a /workspace/faiss/build/faiss/libfaiss.so /tmp/staging/usr/lib/x86_64-linux-gnu/ && \
    # Add DT_NEEDED dependency so libfaiss_c.so links to libfaiss.so automatically
    # This is required because blevesearch/faiss CMake doesn't set proper DT_NEEDED
    patchelf --add-needed libfaiss.so /tmp/staging/usr/lib/x86_64-linux-gnu/libfaiss_c.so && \
    # Copy all files from staging to deb-build
    mkdir -p /tmp/deb-build/usr/include /tmp/deb-build/usr/lib /tmp/deb-build/usr/bin /tmp/deb-build/usr/share && \
    cp -a /tmp/staging/usr/include/. /tmp/deb-build/usr/include/ && \
    cp -a /tmp/staging/usr/lib/. /tmp/deb-build/usr/lib/ && \
    cp -a /tmp/staging/usr/bin/. /tmp/deb-build/usr/bin/ 2>/dev/null || true && \
    cp -a /tmp/staging/usr/share/. /tmp/deb-build/usr/share/ 2>/dev/null || true && \
    # Create .deb package metadata
    mkdir -p /tmp/deb-build/DEBIAN && \
    printf 'Package: libfaiss-c\nVersion: 1.13.2-bleve-1\nSection: libs\nPriority: optional\nArchitecture: amd64\nMaintainer: root@buildkitsandbox\nDescription: Faiss C Library - Built from bleve fork\n' > /tmp/deb-build/DEBIAN/control && \
    printf '#!/bin/sh\n/sbin/ldconfig\n' > /tmp/deb-build/DEBIAN/postinst && \
    chmod +x /tmp/deb-build/DEBIAN/postinst && \
    dpkg-deb --build /tmp/deb-build /workspace/faiss/build/libfaiss-c_1.13.2-bleve-1_amd64.deb
