
#!/bin/bash
set -e

source ./clickhouse/ch.sh # clickhouse helper scripts

ch_installed && clickhouse-client --version-clean > clickhouse/VERSION && echo "" > clickhouse/REVISION

sudo chown ubuntu:ubuntu clickhouse/VERSION
sudo chown ubuntu:ubuntu clickhouse/REVISION