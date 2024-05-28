#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

BUILD_PROFILE=${BUILD_PROFILE:-debug}

echo "setting up meta chaos.."
./scripts/ci/ci-setup-chaos-meta.sh
