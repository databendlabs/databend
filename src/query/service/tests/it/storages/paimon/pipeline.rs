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

use std::sync::Arc;

use databend_common_catalog::plan::Filters;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionID;
use databend_common_expression::RemoteExpr as Expr;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use databend_query::pipelines::executor::ExecutorSettings;
use databend_query::pipelines::executor::PipelinePullingExecutor;
use databend_query::sessions::QueryContext;

use super::common::paimon_it_lock;
use super::common::shared_fixture;

async fn query_ctx(ctx: Option<Arc<dyn TableContext>>) -> Result<Arc<dyn TableContext>> {
    match ctx {
        Some(ctx) => Ok(ctx),
        None => {
            let ctx: Arc<dyn TableContext> = shared_fixture().await?.new_query_ctx().await?;
            Ok(ctx)
        }
    }
}

fn set_max_threads(ctx: &Arc<dyn TableContext>, threads: usize) -> Result<()> {
    let query_ctx = ctx
        .as_any()
        .downcast_ref::<QueryContext>()
        .ok_or_else(|| ErrorCode::Internal("expected query context in paimon tests".to_string()))?;
    query_ctx
        .get_current_session()
        .get_settings()
        .set_max_threads(threads as u64)
}

pub fn projection_indices(indices: Vec<usize>) -> PushDownInfo {
    PushDownInfo {
        projection: Some(Projection::Columns(indices)),
        is_deterministic: true,
        ..Default::default()
    }
}

pub fn pushdown_residual_only_limit(limit: usize) -> PushDownInfo {
    let mut pushdown = projection_indices(vec![]);
    pushdown.filters = Some(Filters {
        filter: Expr::FunctionCall {
            span: None,
            id: Box::new(FunctionID::Builtin {
                name: "not".to_string(),
                id: 0,
            }),
            generics: vec![],
            args: vec![function("eq", vec![
                string_column("name"),
                string_literal("missing"),
            ])],
            return_type: DataType::Boolean,
        },
        inverted_filter: Expr::Constant {
            span: None,
            scalar: Scalar::Boolean(false),
            data_type: DataType::Boolean,
        },
    });
    pushdown.limit = Some(limit);
    pushdown
}

fn string_column(name: &str) -> Expr<String> {
    Expr::ColumnRef {
        span: None,
        id: name.to_string(),
        data_type: DataType::String,
        display_name: name.to_string(),
    }
}

fn string_literal(value: &str) -> Expr<String> {
    Expr::Constant {
        span: None,
        scalar: Scalar::String(value.to_string()),
        data_type: DataType::String,
    }
}

fn function(name: &str, args: Vec<Expr<String>>) -> Expr<String> {
    Expr::FunctionCall {
        span: None,
        id: Box::new(FunctionID::Builtin {
            name: name.to_string(),
            id: 0,
        }),
        generics: vec![],
        args,
        return_type: DataType::Boolean,
    }
}

pub async fn read_blocks_via_pipeline(
    table: Arc<dyn Table>,
    push_downs: Option<PushDownInfo>,
) -> Result<Vec<DataBlock>> {
    read_blocks_via_pipeline_with_threads(table, push_downs, None).await
}

pub async fn read_blocks_via_pipeline_with_threads(
    table: Arc<dyn Table>,
    push_downs: Option<PushDownInfo>,
    max_threads: Option<usize>,
) -> Result<Vec<DataBlock>> {
    read_blocks_via_pipeline_with_ctx(table, push_downs, max_threads, None).await
}

// paimon_it_lock 故意在整个 pipeline 执行期间（含 pull_data().await）持有，
// 用于串行化各测试对共享全局执行运行时的使用；此处持锁跨 await 是有意为之。
#[allow(clippy::await_holding_lock)]
pub async fn read_blocks_via_pipeline_with_ctx(
    table: Arc<dyn Table>,
    push_downs: Option<PushDownInfo>,
    max_threads: Option<usize>,
    ctx: Option<Arc<dyn TableContext>>,
) -> Result<Vec<DataBlock>> {
    let ctx = query_ctx(ctx).await?;
    let _guard = paimon_it_lock();
    if let Some(threads) = max_threads {
        set_max_threads(&ctx, threads)?;
    }
    let plan = table
        .read_plan(ctx.clone(), push_downs, None, false, false)
        .await?;
    let mut pipeline = databend_common_pipeline::core::Pipeline::create();
    table.read_data(ctx.clone(), &plan, &mut pipeline, false)?;
    let settings = ExecutorSettings::try_create(ctx)?;
    let mut executor = PipelinePullingExecutor::try_create(pipeline, settings)?;
    executor.start();
    let mut blocks = Vec::new();
    while let Some(block) = executor.pull_data().await? {
        blocks.push(block);
    }
    Ok(blocks)
}

pub fn collect_id_name_rows(blocks: &[DataBlock]) -> Vec<(i32, String)> {
    use databend_common_expression::ScalarRef;
    use databend_common_expression::types::NumberScalar;

    let mut rows = Vec::new();
    for block in blocks {
        if block.num_columns() < 2 {
            continue;
        }
        for i in 0..block.num_rows() {
            let id = match block.get_by_offset(0).index(i).unwrap() {
                ScalarRef::Number(NumberScalar::Int32(v)) => v,
                other => panic!("unexpected id scalar: {other:?}"),
            };
            let name = match block.get_by_offset(1).index(i).unwrap() {
                ScalarRef::String(v) => v.to_string(),
                other => panic!("unexpected name scalar: {other:?}"),
            };
            rows.push((id, name));
        }
    }
    rows
}

pub fn total_rows(blocks: &[DataBlock]) -> usize {
    blocks.iter().map(|block| block.num_rows()).sum()
}
