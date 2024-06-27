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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::ROW_ID_COL_NAME;
use itertools::Itertools;

use crate::executor::explain::PlanStatsInfo;
use crate::executor::physical_plans::physical_row_fetch::RowFetch;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::SExpr;
use crate::ColumnEntry;
use crate::ColumnSet;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Limit {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub input: Box<PhysicalPlan>,
    pub limit: Option<usize>,
    pub offset: usize,

    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl Limit {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        self.input.output_schema()
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_limit(
        &mut self,
        s_expr: &SExpr,
        limit: &crate::plans::Limit,
        mut required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        // 1. Prune unused Columns.
        // Apply lazy.
        let metadata = self.metadata.read().clone();
        let lazy_columns = metadata.lazy_columns();
        required = required
            .difference(lazy_columns)
            .cloned()
            .collect::<ColumnSet>();
        required.extend(metadata.row_id_indexes());

        // 2. Build physical plan.
        let input_plan = self.build(s_expr.child(0)?, required).await?;
        let metadata = self.metadata.read().clone();
        if limit.before_exchange || metadata.lazy_columns().is_empty() {
            return Ok(PhysicalPlan::Limit(Limit {
                plan_id: 0,
                input: Box::new(input_plan),
                limit: limit.limit,
                offset: limit.offset,
                stat_info: Some(stat_info),
            }));
        }

        // If `lazy_columns` is not empty, build a `RowFetch` plan on top of the `Limit` plan.

        let input_schema = input_plan.output_schema()?;

        // Lazy materialization is enabled.
        let row_id_col_index = metadata
            .columns()
            .iter()
            .position(|col| col.name() == ROW_ID_COL_NAME)
            .ok_or_else(|| ErrorCode::Internal("Internal column _row_id is not found"))?;

        if !input_schema.has_field(&row_id_col_index.to_string()) {
            return Ok(PhysicalPlan::Limit(Limit {
                plan_id: 0,
                input: Box::new(input_plan),
                limit: limit.limit,
                offset: limit.offset,
                stat_info: Some(stat_info),
            }));
        }

        let row_id_col_offset = input_schema.index_of(&row_id_col_index.to_string())?;

        // There may be more than one `LIMIT` plan, we don't need to fetch the same columns multiple times.
        // See the case in tests/sqllogictests/suites/crdb/limit:
        // SELECT * FROM (SELECT * FROM t_47283 ORDER BY k LIMIT 4) WHERE a > 5 LIMIT 1
        let lazy_columns = metadata
            .lazy_columns()
            .iter()
            .sorted() // Needs sort because we need to make the order deterministic.
            .filter(|index| !input_schema.has_field(&index.to_string())) // If the column is already in the input schema, we don't need to fetch it.
            .cloned()
            .collect::<Vec<_>>();

        if limit.before_exchange || lazy_columns.is_empty() {
            // If there is no lazy column, we don't need to build a `RowFetch` plan.
            return Ok(PhysicalPlan::Limit(Limit {
                plan_id: 0,
                input: Box::new(input_plan),
                limit: limit.limit,
                offset: limit.offset,
                stat_info: Some(stat_info),
            }));
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
            input: Box::new(PhysicalPlan::Limit(Limit {
                plan_id: 0,
                input: Box::new(input_plan),
                limit: limit.limit,
                offset: limit.offset,
                stat_info: Some(stat_info.clone()),
            })),
            source: Box::new(source_info),
            row_id_col_offset,
            cols_to_fetch,
            fetched_fields,
            stat_info: Some(stat_info),
        }))
    }
}
