// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::LazyLock;

use prometheus_client::encoding::EncodeLabelSet;

use crate::register_counter_family;
use crate::Counter;
use crate::Family;

#[derive(Clone, Debug, EncodeLabelSet, Hash, PartialEq, Eq)]
struct CacheLabels {
    cache_name: String,
}
static METRIC_COPY_INTO_TIMINGS_MS: LazyLock<Family<CacheLabels, Counter>> =
    LazyLock::new(|| register_counter_family("copy_into_table_timings_ms"));

pub fn metrics_inc_copy_into_timings_ms_bind(c: u64) {
    METRIC_COPY_INTO_TIMINGS_MS
        .get_or_create(&CacheLabels {
            cache_name: "bind".to_owned(),
        })
        .inc_by(c);
}

pub fn metrics_inc_copy_into_timings_ms_list_files(c: u64) {
    METRIC_COPY_INTO_TIMINGS_MS
        .get_or_create(&CacheLabels {
            cache_name: "list_files".to_owned(),
        })
        .inc_by(c);
}

pub fn metrics_inc_copy_into_timings_ms_filter_files(c: u64) {
    METRIC_COPY_INTO_TIMINGS_MS
        .get_or_create(&CacheLabels {
            cache_name: "filter_files".to_owned(),
        })
        .inc_by(c);
}

pub fn metrics_inc_copy_into_timings_ms_build_physical_plan(c: u64) {
    METRIC_COPY_INTO_TIMINGS_MS
        .get_or_create(&CacheLabels {
            cache_name: "build_physical_plan".to_owned(),
        })
        .inc_by(c);
}

pub fn metrics_inc_copy_into_timings_ms_purge_files(c: u64) {
    METRIC_COPY_INTO_TIMINGS_MS
        .get_or_create(&CacheLabels {
            cache_name: "purge_files".to_owned(),
        })
        .inc_by(c);
}
