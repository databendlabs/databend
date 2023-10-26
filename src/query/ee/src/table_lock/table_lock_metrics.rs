// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use common_metrics::register_counter_family;
use common_metrics::Counter;
use common_metrics::Family;
use common_metrics::VecLabels;
use common_pipeline_core::TableLockLevel;
use lazy_static::lazy_static;

const METRIC_TABLE_LOCK_NUMS: &str = "table_lock_nums";

lazy_static! {
    static ref TABLE_LEVEL_LOCK_NUMS: Family<VecLabels, Counter> =
        register_counter_family(METRIC_TABLE_LOCK_NUMS);
}

const LABEL_LEVEL: &str = "level";
const LABEL_TABLE_ID: &str = "table_id";

pub fn record_table_lock_nums(level: TableLockLevel, table_id: u64, c: u64) {
    let labels = &vec![
        (LABEL_LEVEL, level.to_string()),
        (LABEL_TABLE_ID, table_id.to_string()),
    ];
    TABLE_LEVEL_LOCK_NUMS.get_or_create(labels).inc_by(c);
}
