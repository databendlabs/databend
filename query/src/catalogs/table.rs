// Copyright 2020 Datafuse Labs.
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

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_metatypes::MetaId;
use common_planners::Extras;
use common_planners::InsertIntoPlan;
use common_planners::ReadDataSourcePlan;
use common_planners::TruncateTablePlan;
use common_streams::SendableDataBlockStream;

use crate::sessions::DatabendQueryContextRef;

#[async_trait::async_trait]
pub trait Table: Sync + Send {
    fn name(&self) -> &str;
    fn database(&self) -> &str;
    fn engine(&self) -> &str;
    fn as_any(&self) -> &dyn Any;
    fn schema(&self) -> Result<DataSchemaRef>;
    fn get_id(&self) -> MetaId;
    fn is_local(&self) -> bool;

    // Some tables may have internal states, like MemoryTable
    // their instances will be kept, instead of dropped after used
    fn is_stateful(&self) -> bool {
        false
    }

    // Get the read source plan.
    fn read_plan(
        &self,
        ctx: DatabendQueryContextRef,
        push_downs: Option<Extras>,
        partition_num_hint: Option<usize>,
    ) -> Result<ReadDataSourcePlan>;

    // Read block data from the underling.
    async fn read(
        &self,
        ctx: DatabendQueryContextRef,
        source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream>;

    // temporary added, pls feel free to rm it
    async fn append_data(
        &self,
        _ctx: DatabendQueryContextRef,
        _insert_plan: InsertIntoPlan,
    ) -> Result<()> {
        Err(ErrorCode::UnImplement(format!(
            "append data for local table {} is not implemented",
            self.name()
        )))
    }

    async fn truncate(
        &self,
        _ctx: DatabendQueryContextRef,
        _truncate_plan: TruncateTablePlan,
    ) -> Result<()> {
        Err(ErrorCode::UnImplement(format!(
            "truncate for local table {} is not implemented",
            self.name()
        )))
    }
}

pub type TablePtr = Arc<dyn Table>;
