//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use common_catalog::plan::Expression;
use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_catalog::plan::Projection;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_datavalues::DataField;
use common_datavalues::DataValue;
use common_datavalues::NullType;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::FunctionContext;
use common_sql::evaluator::EvalNode;
use common_sql::evaluator::Evaluator;
use common_storages_table_meta::meta::Location;
use common_storages_table_meta::meta::TableSnapshot;

use crate::operations::mutation::DeletionPartInfo;
use crate::operations::mutation::DeletionSource;
use crate::operations::mutation::DeletionTransform;
use crate::operations::mutation::MutationSink;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::Pipe;
use crate::pipelines::Pipeline;
use crate::pruning::BlockPruner;
use crate::statistics::ClusterStatsGenerator;
use crate::FuseTable;

impl FuseTable {
    pub async fn do_delete(
        &self,
        ctx: Arc<dyn TableContext>,
        filter: Option<Expression>,
        col_indices: Vec<usize>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let snapshot_opt = self.read_table_snapshot().await?;

        // check if table is empty
        let snapshot = if let Some(val) = snapshot_opt {
            val
        } else {
            // no snapshot, no deletion
            return Ok(());
        };

        if snapshot.summary.row_count == 0 {
            // empty snapshot, no deletion
            return Ok(());
        }

        // check if unconditional deletion
        if filter.is_none() {
            // deleting the whole table... just a truncate
            let purge = false;
            return self.do_truncate(ctx.clone(), purge).await;
        }

        let filter_expr = filter.unwrap();
        let eval_node = Arc::new(Evaluator::eval_expression(
            &filter_expr,
            self.table_info.schema().as_ref(),
        )?);
        if col_indices.is_empty() {
            // here the situation: filter_expr is not null, but col_indices in empty, which
            // indicates the expr being evaluated is unrelated to the value of rows:
            //   e.g.
            //       `delete from t where 1 = 1`, `delete from t where now()`,
            //       or `delete from t where RANDOM()::INT::BOOLEAN`
            // if the `filter_expr` is of "constant" nullary :
            //   for the whole block, whether all of the rows should be kept or dropped,
            //   we can just return from here, without accessing the block data
            return self.eval_const_for_delete(ctx.clone(), &eval_node).await;
        }

        self.try_add_deletion_source(
            ctx.clone(),
            filter_expr,
            col_indices,
            &snapshot,
            eval_node,
            pipeline,
        )
        .await?;

        self.try_add_deletion_transform(ctx.clone(), snapshot.segments.clone(), pipeline)?;

        pipeline.add_sink(|input| {
            MutationSink::try_create(self, ctx.clone(), snapshot.clone(), input)
        })?;
        Ok(())
    }

    async fn eval_const_for_delete(
        &self,
        ctx: Arc<dyn TableContext>,
        eval_node: &EvalNode,
    ) -> Result<()> {
        let func_ctx = FunctionContext::default();
        let dummy_column = DataValue::Null.as_const_column(&NullType::new_impl(), 1)?;
        let mut dummy_data_block = DataBlock::empty();
        dummy_data_block = dummy_data_block
            .add_column(dummy_column, DataField::new("dummy", NullType::new_impl()))?;
        let filter_result = eval_node.eval(&func_ctx, &dummy_data_block)?.vector;
        debug_assert!(filter_result.len() == 1);
        let filter_result = DataBlock::cast_to_nonull_boolean(&filter_result)?
            .get(0)
            .as_bool()?;
        if filter_result {
            // deleting the whole table... just a truncate
            let purge = false;
            return self.do_truncate(ctx.clone(), purge).await;
        }
        // do nothing.
        Ok(())
    }

    async fn try_add_deletion_source(
        &self,
        ctx: Arc<dyn TableContext>,
        filter_expr: Expression,
        col_indices: Vec<usize>,
        base_snapshot: &TableSnapshot,
        eval_node: Arc<EvalNode>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let projection = Projection::Columns(col_indices.clone());
        let push_down = Some(PushDownInfo {
            projection: Some(projection.clone()),
            filters: vec![filter_expr],
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

        let mut index_stats = Vec::with_capacity(block_metas.len());
        let mut metas = Vec::with_capacity(block_metas.len());
        for (index, block_meta) in block_metas.into_iter() {
            index_stats.push((index, block_meta.cluster_stats.clone()));
            metas.push(block_meta);
        }

        let (_, inner_parts) = self.read_partitions_with_metas(
            ctx.clone(),
            self.table_info.schema(),
            None,
            metas,
            base_snapshot.summary.block_count as usize,
        )?;

        let parts = Partitions::create(
            PartitionsShuffleKind::Mod,
            index_stats
                .into_iter()
                .zip(inner_parts.partitions.into_iter())
                .map(|((a, b), c)| DeletionPartInfo::create(a, b, c))
                .collect(),
        );
        ctx.try_set_partitions(parts)?;

        let block_reader = self.create_block_reader(projection.clone())?;

        let all_col_ids = self.all_the_columns_ids();
        let remain_col_ids: Vec<usize> = all_col_ids
            .into_iter()
            .filter(|id| !col_indices.contains(id))
            .collect();
        let remain_reader = if remain_col_ids.is_empty() {
            Arc::new(None)
        } else {
            Arc::new(Some(
                (*self.create_block_reader(Projection::Columns(remain_col_ids))?).clone(),
            ))
        };

        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        // Add source pipe.
        pipeline.add_source(
            |output| {
                DeletionSource::try_create(
                    ctx.clone(),
                    output,
                    self,
                    block_reader.clone(),
                    eval_node.clone(),
                    remain_reader.clone(),
                )
            },
            max_threads,
        )
    }

    fn try_add_deletion_transform(
        &self,
        ctx: Arc<dyn TableContext>,
        base_segments: Vec<Location>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        match pipeline.pipes.last() {
            None => Err(ErrorCode::Internal("Cannot resize empty pipe.")),
            Some(pipe) if pipe.output_size() == 0 => {
                Err(ErrorCode::Internal("Cannot resize empty pipe."))
            }
            Some(pipe) => {
                let input_size = pipe.output_size();
                let mut inputs_port = Vec::with_capacity(input_size);
                for _ in 0..input_size {
                    inputs_port.push(InputPort::create());
                }
                let output_port = OutputPort::create();
                let processor = DeletionTransform::try_create(
                    ctx,
                    inputs_port.clone(),
                    output_port.clone(),
                    self.get_operator(),
                    self.meta_location_generator().clone(),
                    base_segments,
                    self.get_block_compact_thresholds(),
                )?;
                pipeline.pipes.push(Pipe::ResizePipe {
                    inputs_port,
                    outputs_port: vec![output_port],
                    processor,
                });
                Ok(())
            }
        }
    }

    pub fn cluster_stats_gen(&self) -> Result<ClusterStatsGenerator> {
        if self.cluster_key_meta.is_none() {
            return Ok(ClusterStatsGenerator::default());
        }

        let input_schema = self.table_info.schema();
        let mut merged = input_schema.fields().clone();

        let cluster_keys = self.cluster_keys();
        let mut cluster_key_index = Vec::with_capacity(cluster_keys.len());
        let mut extra_key_index = Vec::with_capacity(cluster_keys.len());
        for expr in &cluster_keys {
            let cname = expr.column_name();
            let index = match merged.iter().position(|x| x.name() == &cname) {
                None => {
                    let field = DataField::new(&cname, expr.data_type());
                    merged.push(field);

                    extra_key_index.push(merged.len() - 1);
                    merged.len() - 1
                }
                Some(idx) => idx,
            };
            cluster_key_index.push(index);
        }

        Ok(ClusterStatsGenerator::new(
            self.cluster_key_meta.as_ref().unwrap().0,
            cluster_key_index,
            extra_key_index,
            0,
            self.get_block_compact_thresholds(),
        ))
    }

    pub fn all_the_columns_ids(&self) -> Vec<usize> {
        (0..self.table_info.schema().fields().len())
            .into_iter()
            .collect::<Vec<usize>>()
    }
}
