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

use std::collections::HashMap;

use databend_common_expression::ColumnId;
use databend_storages_common_table_meta::meta::AdditionalStatsMeta;
use databend_storages_common_table_meta::meta::BlockHLL;
use databend_storages_common_table_meta::meta::encode_column_hll;

#[derive(Clone, Default)]
pub struct TableStatsGenerator {
    prev_stats_meta: Option<AdditionalStatsMeta>,
    prev_stats_location: Option<String>,
    row_count: u64,
    unstats_rows: u64,
    hll: BlockHLL,
}

impl TableStatsGenerator {
    pub fn new(
        prev_stats_meta: Option<AdditionalStatsMeta>,
        prev_stats_location: Option<String>,
        row_count: u64,
        unstats_rows: u64,
        hll: BlockHLL,
    ) -> Self {
        Self {
            prev_stats_meta,
            prev_stats_location,
            row_count,
            unstats_rows,
            hll,
        }
    }

    pub fn table_statistics_location(&self) -> Option<String> {
        self.prev_stats_location.clone()
    }

    pub fn additional_stats_meta(self) -> Option<AdditionalStatsMeta> {
        if self.hll.is_empty() {
            return self.prev_stats_meta;
        }

        let hll = encode_column_hll(&self.hll).ok();
        Some(AdditionalStatsMeta {
            hll,
            row_count: self.row_count,
            unstats_rows: self.unstats_rows,
            ..Default::default()
        })
    }

    pub fn column_distinct_values(&self) -> HashMap<ColumnId, u64> {
        self.hll
            .iter()
            .map(|(id, hll)| (*id, hll.count() as u64))
            .collect()
    }
}
