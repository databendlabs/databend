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

use std::any::Any;
use std::collections::HashMap;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::ROW_ID_COL_NAME;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::filters::TransformLimit;
use databend_common_sql::ColumnEntry;
use databend_common_sql::ColumnSet;
use databend_common_sql::IndexType;
use databend_common_sql::optimizer::ir::SExpr;

use crate::physical_plans::PhysicalPlanBuilder;
use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::format::LimitFormatter;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanCast;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::physical_plans::physical_row_fetch::RowFetch;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Limit {
    meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub limit: Option<usize>,
    pub offset: usize,

    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

#[typetag::serde]
impl IPhysicalPlan for Limit {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(LimitFormatter::create(self))
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

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.pop().unwrap();
        PhysicalPlan::new(Limit {
            meta: self.meta.clone(),
            input,
            limit: self.limit,
            offset: self.offset,
            stat_info: self.stat_info.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        if self.limit.is_some() || self.offset != 0 {
            builder.main_pipeline.try_resize(1)?;
            return builder.main_pipeline.add_transform(|input, output| {
                Ok(ProcessorPtr::create(TransformLimit::try_create(
                    self.limit,
                    self.offset,
                    input,
                    output,
                )?))
            });
        }

        Ok(())
    }
}

impl PhysicalPlanBuilder {
    pub async fn build_limit(
        &mut self,
        s_expr: &SExpr,
        limit: &databend_common_sql::plans::Limit,
        mut required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        // 1. Prune unused Columns.
        // Apply lazy.
        let metadata = self.metadata.read().clone();

        let support_lazy_materialize = s_expr.child(0)?.support_lazy_materialize();
        if !limit.lazy_columns.is_empty() && support_lazy_materialize {
            required = required
                .difference(&limit.lazy_columns)
                .cloned()
                .collect::<ColumnSet>();

            required.extend(metadata.row_id_indexes());
        }

        // 2. Build physical plan.
        let input_plan = self.build(s_expr.child(0)?, required).await?;
        if limit.before_exchange || limit.lazy_columns.is_empty() || !support_lazy_materialize {
            return Ok(PhysicalPlan::new(Limit {
                input: input_plan,
                limit: limit.limit,
                offset: limit.offset,
                stat_info: Some(stat_info),
                meta: PhysicalPlanMeta::new("Limit"),
            }));
        }

        // If `lazy_columns` is not empty, build a `RowFetch` plan on top of the `Limit` plan.
        let mut plan = PhysicalPlan::new(Limit {
            meta: PhysicalPlanMeta::new("Limit"),
            input: input_plan,
            limit: limit.limit,
            offset: limit.offset,
            stat_info: Some(stat_info.clone()),
        });
        let input_schema = plan.output_schema()?;

        // Lazy materialization is enabled.
        let metadata = self.metadata.read();

        // There may be more than one `LIMIT` plan, we don't need to fetch the same columns multiple times.
        // See the case in tests/sqllogictests/suites/crdb/limit:
        // SELECT * FROM (SELECT * FROM t_47283 ORDER BY k LIMIT 4) WHERE a > 5 LIMIT 1
        let mut lazy_columns_by_table: HashMap<IndexType, Vec<IndexType>> = HashMap::new();
        for index in limit.lazy_columns.iter() {
            if input_schema.has_field(&index.to_string()) {
                continue;
            }
            let table_index = match metadata.column(*index) {
                ColumnEntry::BaseTableColumn(c) => c.table_index,
                _ => continue,
            };
            lazy_columns_by_table
                .entry(table_index)
                .or_default()
                .push(*index);
        }

        if lazy_columns_by_table.is_empty() {
            // If there is no lazy column, we don't need to build a `RowFetch` plan.
            return Ok(plan);
        }

        fn collect_table_sources(
            plan: &PhysicalPlan,
            sources: &mut HashMap<IndexType, DataSourcePlan>,
        ) {
            if let Some(scan) = crate::physical_plans::TableScan::from_physical_plan(plan) {
                if let Some(table_index) = scan.table_index {
                    sources
                        .entry(table_index)
                        .or_insert_with(|| (*scan.source).clone());
                }
            }
            for child in plan.children() {
                collect_table_sources(child, sources);
            }
        }

        let mut table_sources = HashMap::new();
        collect_table_sources(&plan, &mut table_sources);

        let mut table_indexes: Vec<IndexType> = lazy_columns_by_table.keys().copied().collect();
        table_indexes.sort_unstable();

        let mut row_id_col_offsets = HashMap::new();
        for table_index in table_indexes.iter() {
            let row_id_col_index = match metadata.row_id_index_by_table_index(*table_index) {
                Some(index) => index,
                None => return Ok(plan),
            };
            if !input_schema.has_field(&row_id_col_index.to_string()) {
                return Ok(plan);
            }
            let row_id_col_offset = input_schema.index_of(&row_id_col_index.to_string())?;
            row_id_col_offsets.insert(*table_index, row_id_col_offset);
        }

        for table_index in table_indexes {
            let lazy_columns: &[IndexType] = match lazy_columns_by_table.get(&table_index) {
                Some(columns) => columns,
                None => continue,
            };
            if lazy_columns.is_empty() {
                continue;
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

            let source_info = table_sources
                .get(&table_index)
                .cloned()
                .or_else(|| metadata.get_table_source(&table_index).cloned())
                .ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "Table source not found for table index {}",
                        table_index
                    ))
                })?;
            let table_schema = source_info.source_info.schema();
            let cols_to_fetch = Self::build_projection(
                &metadata,
                &table_schema,
                lazy_columns.iter(),
                has_inner_column,
                true,
                true,
            );

            let row_id_col_offset =
                row_id_col_offsets
                    .get(&table_index)
                    .copied()
                    .ok_or_else(|| {
                        ErrorCode::Internal(format!(
                            "Internal column {} is not found for table index {}",
                            ROW_ID_COL_NAME, table_index
                        ))
                    })?;

            plan = PhysicalPlan::new(RowFetch {
                meta: PhysicalPlanMeta::new("RowFetch"),
                input: plan,
                source: Box::new(source_info),
                row_id_col_offset,
                cols_to_fetch,
                fetched_fields,
                need_wrap_nullable: false,
                stat_info: Some(stat_info.clone()),
            });
        }

        Ok(plan)
    }
}
