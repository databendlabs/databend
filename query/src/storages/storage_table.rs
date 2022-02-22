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
use std::collections::HashMap;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::MetaId;
use common_meta_types::TableInfo;
use common_planners::Expression;
use common_planners::Extras;
use common_planners::Part;
use common_planners::Partitions;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_planners::TruncateTablePlan;
use common_streams::SendableDataBlockStream;

use crate::pipelines::new::NewPipeline;
use crate::sessions::QueryContext;

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

    fn options(&self) -> &HashMap<String, String> {
        self.get_table_info().options()
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

    // defaults to generate one single part and empty statistics
    async fn read_partitions(
        &self,
        _ctx: Arc<QueryContext>,
        _push_downs: Option<Extras>,
    ) -> Result<(Statistics, Partitions)> {
        Ok((Statistics::default(), vec![Part {
            name: "".to_string(),
            version: 0,
        }]))
    }

    fn table_args(&self) -> Option<Vec<Expression>> {
        None
    }

    // Read block data from the underling.
    async fn read(
        &self,
        _ctx: Arc<QueryContext>,
        plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream>;

    fn read2(
        &self,
        _: Arc<QueryContext>,
        _: &ReadDataSourcePlan,
        _: &mut NewPipeline,
    ) -> Result<()> {
        Err(ErrorCode::UnImplement(""))
    }

    async fn append_data(
        &self,
        _ctx: Arc<QueryContext>,
        _stream: SendableDataBlockStream,
    ) -> Result<SendableDataBlockStream> {
        Err(ErrorCode::UnImplement(format!(
            "append operation for table {} is not implemented, table engine is {}",
            self.name(),
            self.get_table_info().meta.engine
        )))
    }

    async fn commit_insertion(
        &self,
        _ctx: Arc<QueryContext>,
        _operations: Vec<DataBlock>,
        _overwrite: bool,
    ) -> Result<()> {
        Ok(())
    }

    async fn truncate(
        &self,
        _ctx: Arc<QueryContext>,
        _truncate_plan: TruncateTablePlan,
    ) -> Result<()> {
        Err(ErrorCode::UnImplement(format!(
            "truncate for table {} is not implemented",
            self.name()
        )))
    }

    async fn optimize(&self, _ctx: Arc<QueryContext>, _keep_last_snapshot: bool) -> Result<()> {
        Ok(())
    }
}
