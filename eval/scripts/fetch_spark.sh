#!/usr/bin/env bash
# Downloads Spark 3.3.1 into docker/ and verifies it against the SHA-512 published by Apache.
set -eu
f=spark-3.3.1-bin-hadoop3.tgz
cd "$(dirname "$0")/../docker"
[ -f "$f" ] || curl -fsSL -o "$f" "https://repo.huaweicloud.com/apache/spark/spark-3.3.1/$f"
curl -fsS "https://archive.apache.org/dist/spark/spark-3.3.1/$f.sha512" | shasum -a 512 -c -
