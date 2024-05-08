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

use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::TableInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;

use crate::executor::explain::PlanStatsInfo;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::ColumnSet;
use crate::optimizer::SExpr;
use crate::plans::RemoteSubqueryMutation;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ModifyBySubquery {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub schema: DataSchemaRef,
    pub input: Box<PhysicalPlan>,
    pub predicate: String,
    pub typ: RemoteSubqueryMutation,
    pub filter: Box<PhysicalPlan>,
    pub table_info: TableInfo,
    pub catalog_info: CatalogInfo,
    pub snapshot: Arc<TableSnapshot>,
    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl ModifyBySubquery {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_modify_by_subquery(
        &mut self,
        s_expr: &SExpr,
        subquery: &crate::plans::ModifyBySubquery,
        required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        let child = s_expr.child(0)?;
        let input = self.build(child, required.clone()).await?;

        let input_schema = input.output_schema()?;
        let fields = input_schema.fields().clone();
        let schema = DataSchemaRefExt::create(fields);

        Ok(PhysicalPlan::ModifyBySubquery(ModifyBySubquery {
            plan_id: 0,
            schema,
            input: Box::new(input),
            stat_info: Some(stat_info),
            predicate: format!("{:?}", subquery.subquery_desc.predicate),
            typ: subquery.typ.clone(),
            filter: subquery.filter.clone(),
            snapshot: subquery.snapshot.clone(),
            table_info: subquery.table_info.clone(),
            catalog_info: subquery.catalog_info.clone(),
        }))
    }
}
