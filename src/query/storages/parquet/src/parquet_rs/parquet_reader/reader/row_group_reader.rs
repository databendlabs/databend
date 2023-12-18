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

use databend_common_exception::Result;
use databend_common_expression::TopKSorter;
use databend_common_metrics::storage::metrics_inc_omit_filter_rowgroups;
use databend_common_metrics::storage::metrics_inc_omit_filter_rows;
use opendal::Operator;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::arrow::arrow_reader::RowSelector;
use parquet::format::PageLocation;

use crate::parquet_rs::parquet_reader::policy::PolicyBuilders;
use crate::parquet_rs::parquet_reader::policy::PolicyType;
use crate::parquet_rs::parquet_reader::policy::ReadPolicyImpl;
use crate::parquet_rs::parquet_reader::policy::POLICY_PREDICATE_ONLY;
use crate::parquet_rs::parquet_reader::row_group::InMemoryRowGroup;
use crate::ParquetRSRowGroupPart;
use crate::ReadSettings;

/// The reader to read a row group.
pub struct ParquetRSRowGroupReader {
    pub(super) op: Operator,

    pub(super) default_policy: PolicyType,
    pub(super) policy_builders: PolicyBuilders,

    // Options
    pub(super) batch_size: usize,
}

impl ParquetRSRowGroupReader {
    pub fn operator(&self) -> Operator {
        self.op.clone()
    }

    /// Read a row group and return a reader with certain policy.
    /// If return [None], it means the whole row group is skipped (by eval push down predicate).
    pub async fn create_read_policy(
        &self,
        read_settings: &ReadSettings,
        part: &ParquetRSRowGroupPart,
        topk_sorter: &mut Option<TopKSorter>,
    ) -> Result<Option<ReadPolicyImpl>> {
        if let Some((sorter, min_max)) = topk_sorter.as_ref().zip(part.sort_min_max.as_ref()) {
            if sorter.never_match(min_max) {
                return Ok(None);
            }
        }
        let page_locations = part.page_locations.as_ref().map(|x| {
            x.iter()
                .map(|x| x.iter().map(PageLocation::from).collect())
                .collect::<Vec<Vec<_>>>()
        });
        let row_group = InMemoryRowGroup::new(
            &part.location,
            self.op.clone(),
            &part.meta,
            page_locations.as_deref(),
            read_settings.max_gap_size,
            read_settings.max_range_size,
        );
        let mut selection = part
            .selectors
            .as_ref()
            .map(|x| x.iter().map(RowSelector::from).collect::<Vec<_>>())
            .map(RowSelection::from);

        let mut policy = self.default_policy;
        if part.omit_filter {
            // Remove predicate.
            // PRED_ONLY (0b01) -> NO_PREFETCH (0b00)
            // PRED_AND_TOPK (0b11) -> TOPK_ONLY (0b10)
            policy &= !POLICY_PREDICATE_ONLY;
            selection = None;
            metrics_inc_omit_filter_rowgroups(1);
            metrics_inc_omit_filter_rows(row_group.row_count() as u64);
        }

        let builder = &self.policy_builders[policy as usize];
        builder
            .build(row_group, selection, topk_sorter, self.batch_size)
            .await
    }
}
