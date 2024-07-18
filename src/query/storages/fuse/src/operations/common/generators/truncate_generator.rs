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

use std::any::Any;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::TableSchema;
use databend_storages_common_table_meta::meta::ClusterKey;
use databend_storages_common_table_meta::meta::TableSnapshot;

use crate::operations::common::SnapshotGenerator;

#[derive(Clone)]
pub enum TruncateMode {
    // Truncate and keep the historical data.
    Normal,
    // Delete the data, used for delete operation.
    Delete,
    // Truncate and purge the historical data.
    Purge,
}

#[derive(Clone)]
pub struct TruncateGenerator {
    mode: TruncateMode,
    ctx: Arc<dyn TableContext>,
}

impl TruncateGenerator {
    pub fn new(mode: TruncateMode, ctx: Arc<dyn TableContext>) -> Self {
        TruncateGenerator { mode, ctx }
    }

    pub fn mode(&self) -> &TruncateMode {
        &self.mode
    }
}

#[async_trait::async_trait]
impl SnapshotGenerator for TruncateGenerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn do_generate_new_snapshot(
        &self,
        schema: TableSchema,
        cluster_key_meta: Option<ClusterKey>,
        previous: &Option<Arc<TableSnapshot>>,
        prev_table_seq: Option<u64>,
    ) -> Result<TableSnapshot> {
        let (prev_timestamp, prev_snapshot_id) = if let Some(prev_snapshot) = previous {
            (
                prev_snapshot.timestamp,
                Some((prev_snapshot.snapshot_id, prev_snapshot.format_version)),
            )
        } else {
            (None, None)
        };

        let new_snapshot = TableSnapshot::new(
            prev_table_seq,
            &prev_timestamp,
            prev_snapshot_id,
            &previous
                .as_ref()
                .and_then(|v| v.least_base_snapshot_timestamp),
            schema,
            Default::default(),
            vec![],
            cluster_key_meta,
            None,
            self.ctx
                .get_settings()
                .get_transaction_time_limit_in_hours()?,
        );
        Ok(new_snapshot)
    }
}
