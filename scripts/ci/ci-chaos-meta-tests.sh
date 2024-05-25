#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

echo "setting up meta chaos with target $1.."
./scripts/ci/ci-setup-chaos-meta.sh $1