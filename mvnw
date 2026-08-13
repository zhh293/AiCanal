#!/bin/sh
set -eu
MAVEN_VERSION=3.9.9
BASE_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
MAVEN_BIN="$BASE_DIR/.mvn/wrapper/apache-maven-$MAVEN_VERSION/bin/mvn"
if [ ! -x "$MAVEN_BIN" ]; then
  echo "Maven $MAVEN_VERSION is not installed in $(dirname "$MAVEN_BIN")" >&2
  exit 1
fi
exec "$MAVEN_BIN" "$@"
