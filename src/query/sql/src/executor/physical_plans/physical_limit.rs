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

use std::collections::HashMap;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::ROW_ID_COL_NAME;

use crate::executor::explain::PlanStatsInfo;
use crate::executor::physical_plan::PhysicalPlanDeriveHandle;
use crate::executor::physical_plans::physical_row_fetch::RowFetch;
use crate::executor::IPhysicalPlan;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::executor::PhysicalPlanMeta;
use crate::optimizer::ir::SExpr;
use crate::ColumnEntry;
use crate::ColumnSet;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Limit {
    meta: PhysicalPlanMeta,
    pub input: Box<dyn IPhysicalPlan>,
    pub limit: Option<usize>,
    pub offset: usize,

    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

#[typetag::serde]
impl IPhysicalPlan for Limit {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    #[recursive::recursive]
    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        self.input.try_find_single_data_source()
    }

    fn get_desc(&self) -> Result<String> {
        Ok(match self.limit {
            Some(limit) => format!("LIMIT {} OFFSET {}", limit, self.offset),
            None => format!("OFFSET {}", self.offset),
        })
    }

    fn get_labels(&self) -> Result<HashMap<String, Vec<String>>> {
        let mut labels = HashMap::with_capacity(2);
        labels.insert(String::from("Offset"), vec![self.offset.to_string()]);

        if let Some(limit) = self.limit {
            labels.insert(String::from("Number of rows"), vec![limit.to_string()]);
        }

        Ok(labels)
    }

    fn derive_with(
        &self,
        handle: &mut Box<dyn PhysicalPlanDeriveHandle>,
    ) -> Box<dyn IPhysicalPlan> {
        let derive_input = self.input.derive_with(handle);

        match handle.derive(self, vec![derive_input]) {
            Ok(v) => v,
            Err(children) => {
                let mut new_limit = self.clone();
                assert_eq!(children.len(), 1);
                new_limit.input = children[0];
                Box::new(new_limit)
            }
        }
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_limit(
        &mut self,
        s_expr: &SExpr,
        limit: &crate::plans::Limit,
        mut required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<Box<dyn IPhysicalPlan>> {
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
            return Ok(Box::new(Limit {
                input: Box::new(input_plan),
                limit: limit.limit,
                offset: limit.offset,
                stat_info: Some(stat_info),
                meta: PhysicalPlanMeta::new("Limit"),
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
            return Ok(Box::new(Limit {
                input: Box::new(input_plan),
                limit: limit.limit,
                offset: limit.offset,
                stat_info: Some(stat_info),
                meta: PhysicalPlanMeta::new("Limit"),
            }));
        }

        let row_id_col_offset = input_schema.index_of(&row_id_col_index.to_string())?;

        // There may be more than one `LIMIT` plan, we don't need to fetch the same columns multiple times.
        // See the case in tests/sqllogictests/suites/crdb/limit:
        // SELECT * FROM (SELECT * FROM t_47283 ORDER BY k LIMIT 4) WHERE a > 5 LIMIT 1
        let lazy_columns = metadata
            .lazy_columns()
            .iter()
            .filter(|index| !input_schema.has_field(&index.to_string())) // If the column is already in the input schema, we don't need to fetch it.
            .cloned()
            .collect::<Vec<_>>();

        if limit.before_exchange || lazy_columns.is_empty() {
            // If there is no lazy column, we don't need to build a `RowFetch` plan.
            return Ok(Box::new(Limit {
                input: Box::new(input_plan),
                limit: limit.limit,
                offset: limit.offset,
                stat_info: Some(stat_info),
                meta: PhysicalPlanMeta::new("Limit"),
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

        Ok(Box::new(RowFetch {
            meta: PhysicalPlanMeta::new("RowFetch"),
            input: Box::new(Limit {
                meta: PhysicalPlanMeta::new("RowFetch"),
                input: Box::new(input_plan),
                limit: limit.limit,
                offset: limit.offset,
                stat_info: Some(stat_info.clone()),
            }),
            source: Box::new(source_info),
            row_id_col_offset,
            cols_to_fetch,
            fetched_fields,
            need_wrap_nullable: false,
            stat_info: Some(stat_info),
        }))
    }
}
