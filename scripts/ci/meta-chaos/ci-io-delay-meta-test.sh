#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

BUILD_PROFILE=${BUILD_PROFILE:-debug}

echo "setting up meta chaos.."
./scripts/ci/ci-setup-chaos-meta.sh

HTTP_ADDR="test-databend-meta-0.test-databend-meta.databend.svc.cluster.local:28002,test-databend-meta-1.test-databend-meta.databend.svc.cluster.local:28002,test-databend-meta-2.test-databend-meta.databend.svc.cluster.local:28002"
python3 tests/metaverifier/chaos-meta.py --mode=io/delay/delay=50ms,percent=10 --namespace=databend --nodes=${HTTP_ADDR} --total=800 --apply_second=5 --recover_second=10