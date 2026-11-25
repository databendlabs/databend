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

use databend_common_base::runtime::metrics::FamilyCounter;
use databend_common_base::runtime::metrics::Gauge;
use databend_common_base::runtime::metrics::register_counter_family;
use databend_common_base::runtime::metrics::register_gauge;

use crate::VecLabels;

static REJECTED: LazyLock<FamilyCounter<VecLabels>> =
    LazyLock::new(|| register_counter_family("table_maintenance_rejected"));
static STOPPED: LazyLock<FamilyCounter<VecLabels>> =
    LazyLock::new(|| register_counter_family("table_maintenance_stopped"));

static MAILBOX_TABLES: LazyLock<Gauge> =
    LazyLock::new(|| register_gauge("table_maintenance_mailbox_tables"));
static RESERVED_MEMORY: LazyLock<Gauge> =
    LazyLock::new(|| register_gauge("table_maintenance_reserved_memory_bytes"));
static UNITS: LazyLock<FamilyCounter<VecLabels>> =
    LazyLock::new(|| register_counter_family("table_maintenance_units"));

pub fn set_mailbox_tables(tables: usize) {
    MAILBOX_TABLES.set(tables as i64);
}

pub fn add_reserved_memory(bytes: i64) {
    RESERVED_MEMORY.inc_by(bytes);
}

pub fn record_unit(action: &'static str, outcome: &'static str) {
    UNITS
        .get_or_create(&vec![
            ("action", action.to_string()),
            ("outcome", outcome.to_string()),
        ])
        .inc();
}

// Only bounded action/reason values belong in labels. Table/query identities go in logs.
pub fn record_rejected(reason: &'static str) {
    REJECTED
        .get_or_create(&vec![("reason", reason.to_string())])
        .inc();
}

pub fn record_stopped(action: &'static str, reason: &'static str) {
    STOPPED
        .get_or_create(&vec![
            ("action", action.to_string()),
            ("reason", reason.to_string()),
        ])
        .inc();
}
