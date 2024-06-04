#!/bin/bash

set -e

BUILD_PROFILE="${BUILD_PROFILE:-debug}"

curl --proto '=https' --tlsv1.2 -sSfL https://sh.vector.dev | sudo bash -s -- -y --prefix /usr/local
