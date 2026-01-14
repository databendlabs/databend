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

use databend_common_exception::Result;
use databend_common_expression::TableSchema;
use databend_common_meta_app::schema::TableIdent;
use databend_common_sql::plans::TruncateMode;
use databend_storages_common_table_meta::meta::ClusterKey;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;

use crate::operations::common::SnapshotGenerator;
use crate::statistics::TableStatsGenerator;

#[derive(Clone)]
pub struct TruncateGenerator {
    mode: TruncateMode,
}

impl TruncateGenerator {
    pub fn new(mode: TruncateMode) -> Self {
        TruncateGenerator { mode }
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
        table_ident: &TableIdent,
        table_schema: &TableSchema,
        cluster_key_meta: Option<ClusterKey>,
        previous: &Option<Arc<TableSnapshot>>,
        table_meta_timestamps: TableMetaTimestamps,
        _table_stats_gen: TableStatsGenerator,
    ) -> Result<TableSnapshot> {
        TableSnapshot::try_new(
            Some(table_ident.seq),
            previous.clone(),
            table_schema.clone(),
            Default::default(),
            vec![],
            cluster_key_meta,
            None,
            table_meta_timestamps,
        )
    }
}
