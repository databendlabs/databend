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
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::table::DistributionLevel;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableStatistics;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_meta_app::schema::UpsertTableCopiedFileReq;
use databend_common_pipeline::core::Pipeline;
use databend_common_storage::StorageMetrics;
use databend_storages_common_blocks::memory::IN_MEMORY_R_CTE_DATA;
use databend_storages_common_blocks::memory::InMemoryDataKey;
use databend_storages_common_table_meta::meta::SnapshotId;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::table::ChangeType;
use parking_lot::RwLock;

pub struct RecursiveCteMemoryTable {
    table_info: TableInfo,
    blocks: Arc<RwLock<HashMap<u64, Vec<DataBlock>>>>,
    data_metrics: Arc<StorageMetrics>,
}

impl RecursiveCteMemoryTable {
    pub fn try_create(table_info: TableInfo) -> Result<Box<dyn Table>> {
        let table_id = table_info.ident.table_id;
        let temp_prefix = table_info
            .options()
            .get(databend_storages_common_table_meta::table::OPT_KEY_TEMP_PREFIX)
            .cloned();
        let blocks = {
            let mut in_mem_data = IN_MEMORY_R_CTE_DATA.write();
            let key = InMemoryDataKey {
                temp_prefix,
                table_id,
            };
            in_mem_data
                .entry(key)
                .or_insert_with(|| Arc::new(RwLock::new(HashMap::new())))
                .clone()
        };

        Ok(Box::new(Self {
            table_info,
            blocks,
            data_metrics: Arc::new(StorageMetrics::default()),
        }))
    }

    pub fn update_with_id(&self, id: u64, blocks: Vec<DataBlock>) {
        self.blocks.write().insert(id, blocks);
    }

    pub fn take_by_id(&self, id: u64) -> Vec<DataBlock> {
        self.blocks.write().remove(&id).unwrap_or_default()
    }
}

#[async_trait::async_trait]
impl Table for RecursiveCteMemoryTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn distribution_level(&self) -> DistributionLevel {
        DistributionLevel::Cluster
    }

    fn support_column_projection(&self) -> bool {
        true
    }

    fn get_data_metrics(&self) -> Option<Arc<StorageMetrics>> {
        Some(self.data_metrics.clone())
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<databend_common_catalog::plan::PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(
        databend_common_catalog::plan::PartStatistics,
        databend_common_catalog::plan::Partitions,
    )> {
        Err(ErrorCode::Unimplemented(
            "RecursiveCteMemoryTable does not support table scans",
        ))
    }

    fn read_data(
        &self,
        _ctx: Arc<dyn TableContext>,
        _plan: &databend_common_catalog::plan::DataSourcePlan,
        _pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        Err(ErrorCode::Unimplemented(
            "RecursiveCteMemoryTable does not support table scans",
        ))
    }

    fn append_data(
        &self,
        _ctx: Arc<dyn TableContext>,
        _pipeline: &mut Pipeline,
        _table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<()> {
        Err(ErrorCode::Unimplemented(
            "RecursiveCteMemoryTable does not support append",
        ))
    }

    fn commit_insertion(
        &self,
        _ctx: Arc<dyn TableContext>,
        _pipeline: &mut Pipeline,
        _copied_files: Option<UpsertTableCopiedFileReq>,
        _update_stream_meta: Vec<UpdateStreamMetaReq>,
        _overwrite: bool,
        _prev_snapshot_id: Option<SnapshotId>,
        _deduplicated_label: Option<String>,
        _table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<()> {
        Err(ErrorCode::Unimplemented(
            "RecursiveCteMemoryTable does not support insertion",
        ))
    }

    #[async_backtrace::framed]
    async fn truncate(&self, _ctx: Arc<dyn TableContext>, _pipeline: &mut Pipeline) -> Result<()> {
        self.blocks.write().clear();
        Ok(())
    }

    async fn table_statistics(
        &self,
        _ctx: Arc<dyn TableContext>,
        _require_fresh: bool,
        _change_type: Option<ChangeType>,
    ) -> Result<Option<TableStatistics>> {
        let data = self.blocks.read();
        let mut num_rows = 0u64;
        let mut data_bytes = 0u64;
        for blocks in data.values() {
            for block in blocks {
                num_rows += block.num_rows() as u64;
                data_bytes += block.memory_size() as u64;
            }
        }
        Ok(Some(TableStatistics {
            num_rows: Some(num_rows),
            data_size: Some(data_bytes),
            data_size_compressed: Some(data_bytes),
            index_size: None,
            bloom_index_size: None,
            ngram_index_size: None,
            inverted_index_size: None,
            vector_index_size: None,
            virtual_column_size: None,
            number_of_blocks: None,
            number_of_segments: None,
        }))
    }
}
