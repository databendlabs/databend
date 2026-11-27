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

use databend_common_catalog::plan::Filters;
use databend_common_exception::Result;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableSchemaRef;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_index::RangeIndex;
use databend_storages_common_pruner::RangeIndexInput;
use databend_storages_common_pruner::RangePruner;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;

/// Decides from statistics alone which blocks and segments COMPACT and RECLUSTER
/// may drop because every row in them expired by the table TTL, like ClickHouse
/// TTLDrop merges. Built from the MATERIALIZE TTL predicate, whose cutoff is a
/// constant, so every node and every round of a statement agree.
///
/// Only a proof of expiry drops: blocks with unexpired, NULL or unknown TTL
/// values are ordinary blocks to maintenance.
#[derive(Clone)]
pub struct TtlDrop {
    /// Built on NOT(predicate): a range that cannot keep a row has expired.
    expired: RangeIndex,
    /// Built on the predicate: a range that may keep a row may hold expired rows.
    may_expire: RangeIndex,
}

impl TtlDrop {
    pub fn try_create(
        func_ctx: FunctionContext,
        filters: &Filters,
        schema: TableSchemaRef,
    ) -> Result<Self> {
        let index = |expr: &databend_common_expression::RemoteExpr<String>| {
            RangeIndex::try_create(
                func_ctx.clone(),
                &expr.as_expr(&BUILTIN_FUNCTIONS),
                schema.clone(),
                StatisticsOfColumns::default(),
            )
        };
        Ok(Self {
            expired: index(&filters.inverted_filter)?,
            may_expire: index(&filters.filter)?,
        })
    }

    pub fn segment_expired(&self, summary: &Statistics) -> bool {
        !self
            .expired
            .should_keep(&Self::summary_input(summary), None)
    }

    pub fn segment_may_expire(&self, summary: &Statistics) -> bool {
        self.may_expire
            .should_keep(&Self::summary_input(summary), None)
    }

    pub fn block_expired(&self, block: &BlockMeta) -> bool {
        !self
            .expired
            .should_keep(&RangeIndexInput::from_block_meta(block, None, None), None)
    }

    fn summary_input(summary: &Statistics) -> RangeIndexInput<'_> {
        RangeIndexInput::new(&summary.col_stats, summary.spatial_stats.as_ref())
    }
}
