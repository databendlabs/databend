// Copyright 2021 Datafuse Labs.
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
use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common_datablocks::DataBlock;
use common_datavalues::chrono;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::TableInfo;
use common_meta_types::MetaId;
use common_pipeline_core::Pipeline;
use common_planners::DeletePlan;
use common_planners::Expression;
use common_planners::Extras;
use common_planners::Partitions;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_planners::TruncateTablePlan;

use crate::table_context::TableContext;
use crate::table_mutator::TableMutator;

#[async_trait::async_trait]
pub trait Table: Sync + Send {
    fn name(&self) -> &str {
        &self.get_table_info().name
    }

    fn engine(&self) -> &str {
        self.get_table_info().engine()
    }

    fn schema(&self) -> DataSchemaRef {
        self.get_table_info().schema()
    }

    fn options(&self) -> &BTreeMap<String, String> {
        self.get_table_info().options()
    }

    fn field_comments(&self) -> &Vec<String> {
        self.get_table_info().field_comments()
    }

    fn get_id(&self) -> MetaId {
        self.get_table_info().ident.table_id
    }

    fn is_local(&self) -> bool {
        true
    }

    fn as_any(&self) -> &dyn Any;

    fn get_table_info(&self) -> &TableInfo;

    /// whether column prune(projection) can help in table read
    fn benefit_column_prune(&self) -> bool {
        false
    }

    /// whether table has the exact number of total rows
    fn has_exact_total_row_count(&self) -> bool {
        false
    }

    fn cluster_keys(&self) -> Vec<Expression> {
        vec![]
    }

    /// Whether the table engine supports prewhere optimization.
    /// only Fuse Engine supports this.
    fn support_prewhere(&self) -> bool {
        false
    }

    async fn alter_table_cluster_keys(
        &self,
        _ctx: Arc<dyn TableContext>,
        _catalog_name: &str,
        _cluster_key_str: String,
    ) -> Result<()> {
        Err(ErrorCode::UnsupportedEngineParams(format!(
            "Unsupported clustering keys for engine: {}",
            self.engine()
        )))
    }

    async fn drop_table_cluster_keys(
        &self,
        _ctx: Arc<dyn TableContext>,
        _catalog_name: &str,
    ) -> Result<()> {
        Err(ErrorCode::UnsupportedEngineParams(format!(
            "Unsupported clustering keys for engine: {}",
            self.engine()
        )))
    }

    // defaults to generate one single part and empty statistics
    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<Extras>,
    ) -> Result<(Statistics, Partitions)> {
        unimplemented!()
    }

    fn table_args(&self) -> Option<Vec<Expression>> {
        None
    }

    fn read2(
        &self,
        _: Arc<dyn TableContext>,
        _: &ReadDataSourcePlan,
        _: &mut Pipeline,
    ) -> Result<()> {
        Err(ErrorCode::UnImplement(format!(
            "read2 operation for table {} is not implemented, table engine is {}",
            self.name(),
            self.get_table_info().meta.engine
        )))
    }

    fn append2(&self, _: Arc<dyn TableContext>, _: &mut Pipeline) -> Result<()> {
        Err(ErrorCode::UnImplement(format!(
            "append2 operation for table {} is not implemented, table engine is {}",
            self.name(),
            self.get_table_info().meta.engine
        )))
    }

    async fn commit_insertion(
        &self,
        _ctx: Arc<dyn TableContext>,
        _catalog_name: &str,
        _operations: Vec<DataBlock>,
        _overwrite: bool,
    ) -> Result<()> {
        Ok(())
    }

    async fn truncate(
        &self,
        _ctx: Arc<dyn TableContext>,
        _truncate_plan: TruncateTablePlan,
    ) -> Result<()> {
        Err(ErrorCode::UnImplement(format!(
            "truncate for table {} is not implemented",
            self.name()
        )))
    }

    async fn optimize(&self, _ctx: Arc<dyn TableContext>, _keep_last_snapshot: bool) -> Result<()> {
        Ok(())
    }

    async fn statistics(&self, _ctx: Arc<dyn TableContext>) -> Result<Option<TableStatistics>> {
        Ok(None)
    }

    async fn navigate_to(
        &self,
        _ctx: Arc<dyn TableContext>,
        _instant: &NavigationPoint,
    ) -> Result<Arc<dyn Table>> {
        Err(ErrorCode::UnImplement(format!(
            "table {},  of engine type {}, does not support time travel",
            self.name(),
            self.get_table_info().engine(),
        )))
    }

    async fn delete(&self, _ctx: Arc<dyn TableContext>, _delete_plan: DeletePlan) -> Result<()> {
        Err(ErrorCode::UnImplement(format!(
            "table {},  of engine type {}, does not support DELETE FROM",
            self.name(),
            self.get_table_info().engine(),
        )))
    }

    async fn compact(
        &self,
        _ctx: Arc<dyn TableContext>,
        _catalog: String,
        _pipeline: &mut Pipeline,
    ) -> Result<Option<Arc<dyn TableMutator>>> {
        Err(ErrorCode::UnImplement(format!(
            "table {},  of engine type {}, does not support compact",
            self.name(),
            self.get_table_info().engine(),
        )))
    }

    async fn recluster(
        &self,
        _ctx: Arc<dyn TableContext>,
        _catalog: String,
        _pipeline: &mut Pipeline,
    ) -> Result<Option<Arc<dyn TableMutator>>> {
        Err(ErrorCode::UnImplement(format!(
            "table {},  of engine type {}, does not support recluster",
            self.name(),
            self.get_table_info().engine(),
        )))
    }
}

#[derive(Debug)]
pub enum NavigationPoint {
    SnapshotID(String),
    TimePoint(DateTime<Utc>),
}

#[derive(Debug, Copy, Clone)]
pub struct TableStatistics {
    pub num_rows: Option<u64>,
    pub data_size: Option<u64>,
    pub data_size_compressed: Option<u64>,
    pub index_size: Option<u64>,
}
