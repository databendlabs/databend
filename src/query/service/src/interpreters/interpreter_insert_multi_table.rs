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

use std::str::FromStr;
use std::sync::Arc;

use databend_common_catalog::table::AppendMode;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_meta_app::principal::StageFileFormatType;
use databend_common_pipeline_sources::AsyncSourcer;
use databend_common_sql::executor::physical_plans::DistributedInsertSelect;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::executor::PhysicalPlanBuilder;
use databend_common_sql::plans::insert::InsertValue;
use databend_common_sql::plans::InsertInputSource;
use databend_common_sql::plans::InsertMultiTable;
use databend_common_sql::plans::Plan;
use databend_common_sql::NameResolutionContext;

use crate::interpreters::common::build_update_stream_meta_seq;
use crate::interpreters::common::check_deduplicate_label;
use crate::interpreters::HookOperator;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::pipelines::processors::transforms::TransformRuntimeCastSchema;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::PipelineBuilder;
use crate::pipelines::RawValueSource;
use crate::pipelines::ValueSource;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct InsertMultiTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: InsertMultiTable,
}

impl InsertMultiTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: InsertMultiTable) -> Result<InterpreterPtr> {
        Ok(Arc::new(Self { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for InsertMultiTableInterpreter {
    fn name(&self) -> &str {
        "InsertMultiTableInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        todo!()
    }
}
