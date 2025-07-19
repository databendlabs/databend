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

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::Projection;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::ROW_ID_COL_NAME;

use crate::executor::explain::PlanStatsInfo;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::ir::SExpr;
use crate::ColumnEntry;
use crate::ColumnSet;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RowFetch {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub input: Box<PhysicalPlan>,
    // cloned from `input`.
    pub source: Box<DataSourcePlan>,
    // projection on the source table schema.
    pub cols_to_fetch: Projection,
    pub row_id_col_offset: usize,
    pub fetched_fields: Vec<DataField>,
    pub need_wrap_nullable: bool,

    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl RowFetch {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let mut fields = self.input.output_schema()?.fields().clone();
        fields.extend_from_slice(&self.fetched_fields);
        Ok(DataSchemaRefExt::create(fields))
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_row_fetch(
        &mut self,
        s_expr: &SExpr,
        row_fetch: &crate::plans::RowFetch,
        mut required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        // 1. Prune unused Columns.
        // Apply lazy.
        required = required
            .difference(&row_fetch.lazy_columns)
            .cloned()
            .collect::<ColumnSet>();

        required.insert(row_fetch.row_id_index);

        // 2. Build physical plan.
        let input_plan = self.build(s_expr.child(0)?, required).await?;
        let metadata = self.metadata.read().clone();

        // If `lazy_columns` is not empty, build a `RowFetch` plan on top of the `Limit` plan.
        let input_schema = input_plan.output_schema()?;

        // Lazy materialization is enabled.
        let row_id_col_index = metadata
            .columns()
            .iter()
            .position(|col| col.name() == ROW_ID_COL_NAME)
            .ok_or_else(|| ErrorCode::Internal("Internal column _row_id is not found"))?;

        let Ok(row_id_col_offset) = input_schema.index_of(&row_id_col_index.to_string()) else {
            return Err(ErrorCode::Internal("Internal column _row_id is not found"));
        };

        let lazy_columns = metadata
            .lazy_columns()
            .iter()
            .filter(|index| !input_schema.has_field(&index.to_string())) // If the column is already in the input schema, we don't need to fetch it.
            .cloned()
            .collect::<Vec<_>>();

        if lazy_columns.is_empty() {
            // If there is no lazy column, we don't need to build a `RowFetch` plan.
            return Ok(input_plan);
        }

        let mut has_inner_column = false;
        let fetched_fields = lazy_columns
            .iter()
            .map(|index| {
                let col = metadata.column(*index);
                if let ColumnEntry::BaseTableColumn(c) = col {
                    if c.path_indices.is_some() {
                        has_inner_column = true;
                    }
                }
                DataField::new(&index.to_string(), col.data_type())
            })
            .collect();

        let source = input_plan.try_find_single_data_source();
        debug_assert!(source.is_some());
        let source_info = source.cloned().unwrap();
        let table_schema = source_info.source_info.schema();
        let cols_to_fetch = Self::build_projection(
            &metadata,
            &table_schema,
            lazy_columns.iter(),
            has_inner_column,
            true,
            true,
            false,
        );

        Ok(PhysicalPlan::RowFetch(RowFetch {
            plan_id: 0,
            input: Box::new(input_plan),
            source: Box::new(source_info),
            row_id_col_offset,
            cols_to_fetch,
            fetched_fields,
            need_wrap_nullable: row_fetch.need_wrap_nullable,
            stat_info: Some(stat_info),
        }))
    }
}
