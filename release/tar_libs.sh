#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/../" && pwd )"
pushd $DIR
VERSION=$(./release/get_version.sh)
tar -C target/KinesisConnector-${VERSION}-libs -czvf kafka-connect-kinesis-${TRAVIS_TAG}.tar.gz .
popd