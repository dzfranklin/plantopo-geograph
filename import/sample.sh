#!/usr/bin/env bash
set -euox pipefail
gunzip <./out/meta.ndjson.gz | head -n 100000 | gzip --best > ../sample.ndjson.gz
