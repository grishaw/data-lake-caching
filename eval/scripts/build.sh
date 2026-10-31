#!/usr/bin/env bash
# Builds the unchanged original into its own jar, then the controlled jar. Inside the container only.
set -eu
rm -rf /tmp/original && cp -r /work-original /tmp/original
rm -f /tmp/original/.git
cd /tmp/original
JAVA_HOME=$(ls -d /usr/lib/jvm/java-11-openjdk-*) mvn -q -B -DskipTests install
mkdir -p /work-original-build && cp target/caching-1.0-SNAPSHOT.jar /work-original-build/
sha256sum /work-original-build/caching-1.0-SNAPSHOT.jar
cd /work/eval
mvn -q -B -DskipTests package
sha256sum target/pcc-eval.jar
