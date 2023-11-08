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

use std::collections::BTreeMap;
use std::sync::Arc;

use common_catalog::plan::Filters;
use common_catalog::plan::Projection;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::type_check::check_function;
use common_expression::types::NumberDataType;
use common_expression::FieldIndex;
use common_expression::RemoteExpr;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use common_expression::ROW_ID_COL_NAME;
use common_functions::BUILTIN_FUNCTIONS;
use common_pipeline_core::Pipeline;
use common_sql::evaluator::BlockOperator;
use common_sql::executor::physical_plans::MutationKind;
use common_sql::plans::PREDICATE_COLUMN_NAME;
use log::info;
use storages_common_table_meta::meta::TableSnapshot;

use super::delete::MutationBlockPruningContext;
use crate::operations::common::TransformSerializeBlock;
use crate::operations::mutation::MutationAction;
use crate::operations::mutation::MutationSource;
use crate::pruning::create_segment_location_vector;
use crate::FuseTable;

impl FuseTable {
    /// UPDATE column = expression WHERE condition
    #[allow(clippy::too_many_arguments)]
    #[async_backtrace::framed]
    pub async fn do_update(
        &self,
        ctx: Arc<dyn TableContext>,
        filter: Option<RemoteExpr<String>>,
        col_indices: Vec<FieldIndex>,
        update_list: Vec<(FieldIndex, RemoteExpr<String>)>,
        computed_list: BTreeMap<FieldIndex, RemoteExpr<String>>,
        query_row_id_col: bool,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let snapshot_opt = self.read_table_snapshot().await?;

        // check if table is empty
        let snapshot = if let Some(val) = snapshot_opt {
            val
        } else {
            // no snapshot, no update
            return Ok(());
        };

        if snapshot.summary.row_count == 0 {
            // empty snapshot, no update
            return Ok(());
        }

        let mut filter = filter;
        if col_indices.is_empty() && filter.is_some() && !query_row_id_col {
            let filter_expr = filter.clone().unwrap();
            if !self.try_eval_const(ctx.clone(), &self.schema(), &filter_expr)? {
                // The condition is always false, do nothing.
                return Ok(());
            }
            // The condition is always true.
            filter = None;
        }

        self.try_add_update_source(
            ctx.clone(),
            filter,
            col_indices,
            update_list,
            computed_list,
            &snapshot,
            query_row_id_col,
            pipeline,
        )
        .await?;
        if pipeline.is_empty() {
            return Ok(());
        }

        let block_thresholds = self.get_block_thresholds();
        // sort
        let cluster_stats_gen =
            self.cluster_gen_for_append(ctx.clone(), pipeline, block_thresholds, None)?;

        pipeline.add_transform(|input, output| {
            let proc = TransformSerializeBlock::try_create(
                ctx.clone(),
                input,
                output,
                self,
                cluster_stats_gen.clone(),
            )?;
            proc.into_processor()
        })?;

        self.chain_mutation_pipes(&ctx, pipeline, snapshot, MutationKind::Update, false, false)
    }

    #[async_backtrace::framed]
    #[allow(clippy::too_many_arguments)]
    async fn try_add_update_source(
        &self,
        ctx: Arc<dyn TableContext>,
        filter: Option<RemoteExpr<String>>,
        col_indices: Vec<FieldIndex>,
        update_list: Vec<(FieldIndex, RemoteExpr<String>)>,
        computed_list: BTreeMap<FieldIndex, RemoteExpr<String>>,
        base_snapshot: &TableSnapshot,
        query_row_id_col: bool,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let all_column_indices = self.all_column_indices();
        let schema = self.schema();

        let mut offset_map = BTreeMap::new();
        let mut remain_reader = None;
        let mut pos = 0;
        let (projection, input_schema) = if col_indices.is_empty() {
            all_column_indices.iter().for_each(|&index| {
                offset_map.insert(index, pos);
                pos += 1;
            });

            (
                Projection::Columns(all_column_indices),
                Arc::new(schema.remove_virtual_computed_fields()),
            )
        } else {
            col_indices.iter().for_each(|&index| {
                offset_map.insert(index, pos);
                pos += 1;
            });

            let mut fields: Vec<TableField> = col_indices
                .iter()
                .map(|index| schema.fields()[*index].clone())
                .collect();

            fields.push(TableField::new(
                PREDICATE_COLUMN_NAME,
                TableDataType::Boolean,
            ));
            pos += 1;

            let remain_col_indices: Vec<FieldIndex> = all_column_indices
                .into_iter()
                .filter(|index| !col_indices.contains(index))
                .collect();
            if !remain_col_indices.is_empty() {
                remain_col_indices.iter().for_each(|&index| {
                    offset_map.insert(index, pos);
                    pos += 1;
                });

                let reader = self.create_block_reader(
                    ctx.clone(),
                    Projection::Columns(remain_col_indices),
                    false,
                    false,
                )?;
                fields.extend_from_slice(reader.schema().fields());
                remain_reader = Some((*reader).clone());
            }

            (
                Projection::Columns(col_indices.clone()),
                Arc::new(TableSchema::new(fields)),
            )
        };

        let mut cap = 2;
        if !computed_list.is_empty() {
            cap += 1;
        }
        let mut ops = Vec::with_capacity(cap);

        let mut exprs = Vec::with_capacity(update_list.len());
        for (id, remote_expr) in update_list.into_iter() {
            let expr = remote_expr
                .as_expr(&BUILTIN_FUNCTIONS)
                .project_column_ref(|name| input_schema.index_of(name).unwrap());
            exprs.push(expr);
            offset_map.insert(id, pos);
            pos += 1;
        }
        if !exprs.is_empty() {
            ops.push(BlockOperator::Map {
                exprs,
                projections: None,
            });
        }

        let mut computed_exprs = Vec::with_capacity(computed_list.len());
        for (id, remote_expr) in computed_list.into_iter() {
            let expr = remote_expr
                .as_expr(&BUILTIN_FUNCTIONS)
                .project_column_ref(|name| {
                    let id = schema.index_of(name).unwrap();
                    let pos = offset_map.get(&id).unwrap();
                    *pos as FieldIndex
                });
            computed_exprs.push(expr);
            offset_map.insert(id, pos);
            pos += 1;
        }
        // regenerate related stored computed columns.
        if !computed_exprs.is_empty() {
            ops.push(BlockOperator::Map {
                exprs: computed_exprs,
                projections: None,
            });
        }

        ops.push(BlockOperator::Project {
            projection: offset_map.values().cloned().collect(),
        });

        let block_reader =
            self.create_block_reader(ctx.clone(), projection.clone(), false, false)?;
        let mut schema = block_reader.schema().as_ref().clone();
        if query_row_id_col {
            schema.add_internal_field(
                ROW_ID_COL_NAME,
                TableDataType::Number(NumberDataType::UInt64),
                1,
            );
        }
        let remain_reader = Arc::new(remain_reader);
        let (filter_expr, filters) = if let Some(remote_expr) = filter {
            let reverted_expr = check_function(
                None,
                "not",
                &[],
                &[remote_expr.as_expr(&BUILTIN_FUNCTIONS)],
                &BUILTIN_FUNCTIONS,
            )?;

            (
                Arc::new(Some(
                    remote_expr
                        .as_expr(&BUILTIN_FUNCTIONS)
                        .project_column_ref(|name| schema.index_of(name).unwrap()),
                )),
                Some(Filters {
                    filter: remote_expr,
                    inverted_filter: reverted_expr.as_remote_expr(),
                }),
            )
        } else {
            (Arc::new(None), None)
        };

        let (parts, part_info) = self
            .do_mutation_block_pruning(
                ctx.clone(),
                filters,
                projection,
                MutationBlockPruningContext {
                    segment_locations: create_segment_location_vector(
                        base_snapshot.segments.clone(),
                        None,
                    ),
                    block_count: Some(base_snapshot.summary.block_count as usize),
                },
                false,
                false, // for update
            )
            .await?;
        ctx.set_partitions(parts)?;

        let total_tasks = part_info.total_tasks;
        if total_tasks != 0 {
            let max_threads =
                std::cmp::min(ctx.get_settings().get_max_threads()? as usize, total_tasks);
            // Add source pipe.
            pipeline.add_source(
                |output| {
                    MutationSource::try_create(
                        ctx.clone(),
                        MutationAction::Update,
                        output,
                        filter_expr.clone(),
                        block_reader.clone(),
                        remain_reader.clone(),
                        ops.clone(),
                        self.storage_format,
                        true,
                    )
                },
                max_threads,
            )?;

            // Status.
            {
                let status = format!(
                    "update: begin to run update tasks, total tasks: {}",
                    total_tasks
                );
                ctx.set_status_info(&status);
                info!("{}", status);
            }
        }

        Ok(())
    }
}
