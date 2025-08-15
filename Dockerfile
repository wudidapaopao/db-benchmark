FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive

# Install required system dependencies with minimal extras
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-venv python3-pip \
    r-base r-cran-remotes \
    make vim build-essential virtualenv \
    git curl bash locales \
    libcurl4-openssl-dev libssl-dev libxml2-dev \
    ca-certificates \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /benchmark

COPY . /benchmark

RUN chmod +x run.sh _setup_utils/*.sh

ENV SRC_DATANAME=G1_1e7_1e2_0_0
