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

use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::FunctionContext;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;

use crate::executor::explain::PlanStatsInfo;
use crate::executor::PhysicalPlan;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::plans::Operator;
use crate::ColumnSet;
use crate::MetadataRef;

pub struct PhysicalPlanBuilder {
    pub(crate) metadata: MetadataRef,
    pub(crate) ctx: Arc<dyn TableContext>,
    pub(crate) func_ctx: FunctionContext,
    pub(crate) dry_run: bool,
    // DataMutation info, used to build MergeInto physical plan
    pub(crate) mutation_build_info: Option<MutationBuildInfo>,
}

impl PhysicalPlanBuilder {
    pub fn new(metadata: MetadataRef, ctx: Arc<dyn TableContext>, dry_run: bool) -> Self {
        let func_ctx = ctx.get_function_context().unwrap();
        Self {
            metadata,
            ctx,
            func_ctx,
            dry_run,
            mutation_build_info: None,
        }
    }

    pub(crate) fn build_plan_stat_info(&self, s_expr: &SExpr) -> Result<PlanStatsInfo> {
        let rel_expr = RelExpr::with_s_expr(s_expr);
        let stat_info = rel_expr.derive_cardinality()?;

        Ok(PlanStatsInfo {
            estimated_rows: stat_info.cardinality,
        })
    }

    pub async fn build(&mut self, s_expr: &SExpr, required: ColumnSet) -> Result<PhysicalPlan> {
        let mut plan = self.build_physical_plan(s_expr, required).await?;
        plan.adjust_plan_id(&mut 0);

        Ok(plan)
    }

    #[async_recursion::async_recursion(#[recursive::recursive])]
    pub async fn build_physical_plan(
        &mut self,
        s_expr: &SExpr,
        required: ColumnSet,
    ) -> Result<PhysicalPlan> {
        // Build stat info.
        let stat_info = self.build_plan_stat_info(s_expr)?;
        with_match_rel_op!(|TY| match s_expr.plan().rel_op() {
            crate::plans::RelOp::TY => TY::build(self, s_expr, required, stat_info).await,
            _ => Err(ErrorCode::Internal("Unsupported operator")),
        })
    }

    pub fn set_mutation_build_info(&mut self, mutation_build_info: MutationBuildInfo) {
        self.mutation_build_info = Some(mutation_build_info);
    }

    pub fn set_metadata(&mut self, metadata: MetadataRef) {
        self.metadata = metadata;
    }
}

#[derive(Clone)]
pub struct MutationBuildInfo {
    pub table_info: TableInfo,
    pub table_snapshot: Option<Arc<TableSnapshot>>,
    pub update_stream_meta: Vec<UpdateStreamMetaReq>,
    pub partitions: Partitions,
    pub statistics: PartStatistics,
    pub table_meta_timestamps: TableMetaTimestamps,
}

#[async_trait::async_trait]
pub trait BuildPhysicalPlan {
    async fn build(
        builder: &mut PhysicalPlanBuilder,
        s_expr: &SExpr,
        required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan>;
}

#[macro_export]
macro_rules! with_match_rel_op {
    (| $t:tt | $($tail:tt)*) => {
        match_template::match_template! {
            $t = [
                Scan => TableScan,
                DummyTableScan => DummyTableScan,
                ConstantTableScan => ConstantTableScan,
                ExpressionScan => ExpressionScan,
                CacheScan => CacheScan,
                Join => Join,
                EvalScalar => EvalScalar,
                Filter => Filter,
                Aggregate => Aggregate,
                Sort => Sort,
                Limit => Limit,
                Exchange => Exchange,
                UnionAll => UnionAll,
                Window => Window,
                ProjectSet => ProjectSet,
                Udf => Udf,
                AsyncFunction => AsyncFunction,
                RecursiveCteScan => RecursiveCteScan,
                MergeInto => MergeInto,
                CompactBlock => CompactBlock,
                MutationSource => MutationSource,
            ],
            $($tail)*
        }
    }
}
