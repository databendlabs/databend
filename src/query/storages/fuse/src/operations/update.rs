// Copyright 2023 Datafuse Labs.
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

use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_catalog::plan::Projection;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::RemoteExpr;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use common_sql::evaluator::BlockOperator;
use storages_common_table_meta::meta::TableSnapshot;

use crate::operations::mutation::MutationAction;
use crate::operations::mutation::MutationPartInfo;
use crate::operations::mutation::MutationSink;
use crate::operations::mutation::MutationSource;
use crate::operations::mutation::SerializeDataTransform;
use crate::pipelines::Pipeline;
use crate::pruning::BlockPruner;
use crate::statistics::ClusterStatsGenerator;
use crate::FuseTable;

impl FuseTable {
    /// UPDATE column = expression WHERE condition
    /// The flow of Pipeline is the same as that of deletion.
    pub async fn do_update(
        &self,
        ctx: Arc<dyn TableContext>,
        filter: Option<RemoteExpr<String>>,
        col_indices: Vec<usize>,
        update_list: Vec<(usize, RemoteExpr<String>)>,
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

        if col_indices.is_empty() && filter.is_some() {
            let filter_expr = filter.clone().unwrap();
            if !self.try_eval_const(ctx.clone(), &self.schema(), &filter_expr)? {
                // The condition is always false, do nothing.
                return Ok(());
            }
        }

        self.try_add_update_source(
            ctx.clone(),
            filter,
            col_indices,
            update_list,
            &snapshot,
            pipeline,
        )
        .await?;

        pipeline.add_transform(|input, output| {
            SerializeDataTransform::try_create(
                ctx.clone(),
                input,
                output,
                self,
                ClusterStatsGenerator::default(),
            )
        })?;

        self.try_add_mutation_transform(ctx.clone(), snapshot.segments.clone(), pipeline)?;

        pipeline.add_sink(|input| {
            MutationSink::try_create(self, ctx.clone(), snapshot.clone(), input)
        })?;
        Ok(())
    }

    async fn try_add_update_source(
        &self,
        ctx: Arc<dyn TableContext>,
        filter: Option<RemoteExpr<String>>,
        col_indices: Vec<usize>,
        update_list: Vec<(usize, RemoteExpr<String>)>,
        base_snapshot: &TableSnapshot,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let all_col_ids = self.all_the_columns_ids();
        let schema = self.schema();
        let mut ops = Vec::with_capacity(update_list.len() + 1);
        let mut offset_map = BTreeMap::new();
        let mut remain_reader = None;
        let projection = if col_indices.is_empty() {
            let mut pos = 0;
            all_col_ids.iter().for_each(|&id| {
                offset_map.insert(id, pos);
                pos += 1;
            });

            for (id, remote_expr) in update_list.into_iter() {
                let expr = remote_expr
                    .as_expr(&BUILTIN_FUNCTIONS)
                    .unwrap()
                    .project_column_ref(|name| schema.index_of(name).unwrap());

                ops.push(BlockOperator::Map { expr });
                offset_map.insert(id, pos);
                pos += 1;
            }
            Projection::Columns(all_col_ids)
        } else {
            let mut pos = 0;
            col_indices.iter().for_each(|&id| {
                offset_map.insert(id, pos);
                pos += 1;
            });

            let mut fields: Vec<TableField> = col_indices
                .iter()
                .map(|idx| schema.fields()[*idx].clone())
                .collect();
            let remain_col_ids: Vec<usize> = all_col_ids
                .into_iter()
                .filter(|id| !col_indices.contains(id))
                .collect();
            if !remain_col_ids.is_empty() {
                remain_col_ids.iter().for_each(|&id| {
                    offset_map.insert(id, pos);
                    pos += 1;
                });

                let reader = self.create_block_reader(Projection::Columns(remain_col_ids))?;
                fields.extend_from_slice(reader.schema().fields());
                remain_reader = Some((*reader).clone());
            }

            fields.push(TableField::new("_predicate", TableDataType::Boolean));
            let input_schema = Arc::new(TableSchema::new(fields));
            pos += 1;

            for (id, remote_expr) in update_list.into_iter() {
                let expr = remote_expr
                    .as_expr(&BUILTIN_FUNCTIONS)
                    .unwrap()
                    .project_column_ref(|name| input_schema.index_of(name).unwrap());
                ops.push(BlockOperator::Map { expr });
                offset_map.insert(id, pos);
                pos += 1;
            }

            Projection::Columns(col_indices.clone())
        };

        ops.push(BlockOperator::Project {
            projection: offset_map.values().cloned().collect(),
        });

        let block_reader = self.create_block_reader(projection.clone())?;

        let (filter_expr, filters) = if let Some(filter) = filter {
            let schema = block_reader.schema();
            (
                Arc::new(filter.as_expr(&BUILTIN_FUNCTIONS).map(|expr| {
                    expr.project_column_ref(|name| schema.column_with_name(name).unwrap().0)
                })),
                vec![filter],
            )
        } else {
            (Arc::new(None), vec![])
        };

        let remain_reader = Arc::new(remain_reader);

        let push_down = Some(PushDownInfo {
            projection: Some(projection),
            filters,
            ..PushDownInfo::default()
        });

        let segments_location = base_snapshot.segments.clone();
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
            None,
            metas,
            base_snapshot.summary.block_count as usize,
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
                MutationSource::try_create(
                    ctx.clone(),
                    MutationAction::Update,
                    output,
                    filter_expr.clone(),
                    block_reader.clone(),
                    remain_reader.clone(),
                    ops.clone(),
                )
            },
            max_threads,
        )
    }
}
