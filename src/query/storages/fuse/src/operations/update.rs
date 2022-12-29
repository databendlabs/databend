// Copyright 2021 Datafuse Labs.
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
use std::collections::HashMap;
use std::sync::Arc;

use common_catalog::plan::Expression;
use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_catalog::plan::Projection;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_sql::evaluator::ChunkOperator;
use common_sql::evaluator::Evaluator;

use crate::operations::mutation::MutationPartInfo;
use crate::operations::mutation::MutationSink;
use crate::operations::mutation::UpdateSource;
use crate::pipelines::Pipeline;
use crate::pruning::BlockPruner;
use crate::FuseTable;

impl FuseTable {
    pub async fn do_update(
        &self,
        ctx: Arc<dyn TableContext>,
        filter: Option<Expression>,
        col_indices: Vec<usize>,
        update_list: HashMap<usize, Expression>,
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

        let all_col_ids = self.all_the_columns_ids();
        let schema = self.schema();
        let mut operators = Vec::with_capacity(update_list.len() + 2);
        let mut offset_map = BTreeMap::new();
        let mut remain_reader = None;
        let (projection, filters) = if col_indices.is_empty() {
            if filter.is_some() && !self.try_eval_const(&filter.unwrap())? {
                // do nothing.
                return Ok(());
            }

            let mut pos = 0;
            offset_map = all_col_ids.iter().fold(offset_map, |mut acc, id| {
                acc.insert(*id, pos);
                pos += 1;
                acc
            });

            for (id, expr) in update_list.into_iter() {
                let field = schema.field(id);
                let target = field.data_type();
                let new_expr = Expression::Cast {
                    input: Box::new(expr),
                    target: target.clone(),
                };
                operators.push(ChunkOperator::Map {
                    eval: Evaluator::eval_expression(&new_expr, &schema)?,
                    name: format!("new_{}", field.name()),
                });
                offset_map.insert(id, pos);
                pos += 1;
            }
            (Projection::Columns(all_col_ids), vec![])
        } else {
            let mut pos = 0;
            offset_map = col_indices.iter().fold(offset_map, |mut acc, id| {
                acc.insert(*id, pos);
                pos += 1;
                acc
            });

            let mut fields = schema.fields().clone();
            fields.push(DataField::new("_predicate", bool::to_data_type()));
            let input_schema = Arc::new(DataSchema::new(fields));
            pos += 1;

            let remain_col_ids: Vec<usize> = all_col_ids
                .into_iter()
                .filter(|id| !col_indices.contains(id))
                .collect();
            if !remain_col_ids.is_empty() {
                offset_map = remain_col_ids.iter().fold(offset_map, |mut acc, id| {
                    acc.insert(*id, pos);
                    pos += 1;
                    acc
                });

                remain_reader =
                    Some((*self.create_block_reader(Projection::Columns(remain_col_ids))?).clone());
            }

            for (id, expr) in update_list.into_iter() {
                let field = schema.field(id);
                let target = field.data_type();
                let new_expr = Expression::Function {
                    name: "if".to_string(),
                    args: vec![
                        Expression::IndexedVariable {
                            name: "_predicate".to_string(),
                            data_type: bool::to_data_type(),
                        },
                        Expression::Cast {
                            input: Box::new(expr),
                            target: target.clone(),
                        },
                        Expression::IndexedVariable {
                            name: field.name().clone(),
                            data_type: target.clone(),
                        },
                    ],
                    return_type: target.clone(),
                };
                operators.push(ChunkOperator::Map {
                    eval: Evaluator::eval_expression(&new_expr, &input_schema)?,
                    name: format!("new_{}", field.name()),
                });
                offset_map.insert(id, pos);
                pos += 1;
            }

            (Projection::Columns(col_indices.clone()), vec![
                filter.unwrap(),
            ])
        };

        let offsets = offset_map.values().cloned().collect::<Vec<_>>();
        operators.push(ChunkOperator::Project { offsets });
        operators.push(ChunkOperator::Rename {
            output_schema: schema,
        });

        let block_reader = self.create_block_reader(projection.clone())?;
        let eval_node = if filters.is_empty() {
            Arc::new(None)
        } else {
            Arc::new(Some(Evaluator::eval_expression(
                &filters[0],
                block_reader.schema().as_ref(),
            )?))
        };

        let remain_reader = Arc::new(remain_reader);

        let push_down = Some(PushDownInfo {
            projection: Some(projection),
            filters,
            ..PushDownInfo::default()
        });

        let segments_location = snapshot.segments.clone();
        let block_metas = BlockPruner::prune(
            &ctx,
            self.operator.clone(),
            self.table_info.schema(),
            &push_down,
            segments_location,
        )
        .await?;

        let mut indices = Vec::with_capacity(block_metas.len());
        let mut metas = Vec::with_capacity(block_metas.len());
        block_metas.into_iter().for_each(|(index, block_meta)| {
            indices.push(index);
            metas.push(block_meta);
        });

        let (_, inner_parts) = self.read_partitions_with_metas(
            ctx.clone(),
            self.table_info.schema(),
            push_down,
            metas,
            snapshot.summary.block_count as usize,
        )?;

        let parts = Partitions::create(
            PartitionsShuffleKind::Mod,
            indices
                .into_iter()
                .zip(inner_parts.partitions.into_iter())
                .map(|(a, b)| MutationPartInfo::create(a, None, b))
                .collect(),
        );
        ctx.try_set_partitions(parts)?;

        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        // Add source pipe.
        pipeline.add_source(
            |output| {
                UpdateSource::try_create(
                    ctx.clone(),
                    output,
                    self,
                    block_reader.clone(),
                    eval_node.clone(),
                    remain_reader.clone(),
                    operators.clone(),
                )
            },
            max_threads,
        )?;

        self.try_add_mutation_transform(ctx.clone(), snapshot.segments.clone(), pipeline)?;
        pipeline.add_sink(|input| {
            MutationSink::try_create(self, ctx.clone(), snapshot.clone(), input)
        })?;
        Ok(())
    }
}
