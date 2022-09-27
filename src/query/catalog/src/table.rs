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
use common_legacy_expression::LegacyExpression;
use common_legacy_planners::DeletePlan;
use common_legacy_planners::Extras;
use common_legacy_planners::Partitions;
use common_legacy_planners::ReadDataSourcePlan;
use common_legacy_planners::Statistics;
use common_meta_app::schema::TableInfo;
use common_meta_types::MetaId;
use common_pipeline_core::Pipeline;

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

    fn cluster_keys(&self) -> Vec<LegacyExpression> {
        vec![]
    }

    /// Whether the table engine supports prewhere optimization.
    /// only Fuse Engine supports this.
    fn support_prewhere(&self) -> bool {
        false
    }

    async fn alter_table_cluster_keys(
        &self,
        ctx: Arc<dyn TableContext>,
        cluster_key: String,
    ) -> Result<()> {
        let (_, _) = (ctx, cluster_key);

        Err(ErrorCode::UnsupportedEngineParams(format!(
            "Unsupported clustering keys for engine: {}",
            self.engine()
        )))
    }

    async fn drop_table_cluster_keys(&self, ctx: Arc<dyn TableContext>) -> Result<()> {
        let _ = ctx;

        Err(ErrorCode::UnsupportedEngineParams(format!(
            "Unsupported clustering keys for engine: {}",
            self.engine()
        )))
    }

    // defaults to generate one single part and empty statistics
    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<Extras>,
    ) -> Result<(Statistics, Partitions)> {
        let (_, _) = (ctx, push_downs);

        unimplemented!()
    }

    fn table_args(&self) -> Option<Vec<LegacyExpression>> {
        None
    }

    fn read2(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &ReadDataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let (_, _, _) = (ctx, plan, pipeline);

        Err(ErrorCode::UnImplement(format!(
            "read2 operation for table {} is not implemented, table engine is {}",
            self.name(),
            self.get_table_info().meta.engine
        )))
    }

    fn append2(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        need_output: bool,
    ) -> Result<()> {
        let (_, _, _) = (ctx, pipeline, need_output);

        Err(ErrorCode::UnImplement(format!(
            "append2 operation for table {} is not implemented, table engine is {}",
            self.name(),
            self.get_table_info().meta.engine
        )))
    }

    async fn commit_insertion(
        &self,
        ctx: Arc<dyn TableContext>,
        operations: Vec<DataBlock>,
        overwrite: bool,
    ) -> Result<()> {
        let (_, _, _) = (ctx, operations, overwrite);

        Ok(())
    }

    async fn truncate(&self, ctx: Arc<dyn TableContext>, purge: bool) -> Result<()> {
        let (_, _) = (ctx, purge);

        Err(ErrorCode::UnImplement(format!(
            "truncate for table {} is not implemented",
            self.name()
        )))
    }

    async fn optimize(&self, ctx: Arc<dyn TableContext>, keep_last_snapshot: bool) -> Result<()> {
        let (_, _) = (ctx, keep_last_snapshot);

        Ok(())
    }

    async fn statistics(&self, ctx: Arc<dyn TableContext>) -> Result<Option<TableStatistics>> {
        let _ = ctx;

        Ok(None)
    }

    async fn navigate_to(
        &self,
        ctx: Arc<dyn TableContext>,
        instant: &NavigationPoint,
    ) -> Result<Arc<dyn Table>> {
        let (_, _) = (ctx, instant);

        Err(ErrorCode::UnImplement(format!(
            "table {},  of engine type {}, does not support time travel",
            self.name(),
            self.get_table_info().engine(),
        )))
    }

    async fn delete(&self, ctx: Arc<dyn TableContext>, delete_plan: DeletePlan) -> Result<()> {
        let (_, _) = (ctx, delete_plan);

        Err(ErrorCode::UnImplement(format!(
            "table {},  of engine type {}, does not support DELETE FROM",
            self.name(),
            self.get_table_info().engine(),
        )))
    }

    async fn compact(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
    ) -> Result<Option<Arc<dyn TableMutator>>> {
        let (_, _) = (ctx, pipeline);

        Err(ErrorCode::UnImplement(format!(
            "table {},  of engine type {}, does not support compact",
            self.name(),
            self.get_table_info().engine(),
        )))
    }

    async fn recluster(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        push_downs: Option<Extras>,
    ) -> Result<Option<Arc<dyn TableMutator>>> {
        let (_, _, _) = (ctx, pipeline, push_downs);

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
