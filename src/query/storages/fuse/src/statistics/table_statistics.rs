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
use databend_common_meta_app::schema::TableStatistics;
use databend_storages_common_table_meta::meta::AdditionalStatsMeta;
use databend_storages_common_table_meta::meta::BlockHLL;
use databend_storages_common_table_meta::meta::TableSnapshot;
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

pub fn gen_table_statistics(snapshot: &TableSnapshot) -> TableStatistics {
    let stats = &snapshot.summary;
    TableStatistics {
        number_of_rows: stats.row_count,
        data_bytes: stats.uncompressed_byte_size,
        compressed_data_bytes: stats.compressed_byte_size,
        index_data_bytes: stats.index_size,
        bloom_index_size: stats.bloom_index_size,
        ngram_index_size: stats.ngram_index_size,
        inverted_index_size: stats.inverted_index_size,
        vector_index_size: stats.vector_index_size,
        virtual_column_size: stats.virtual_column_size,
        number_of_segments: Some(snapshot.segments.len() as u64),
        number_of_blocks: Some(stats.block_count),
    }
}
