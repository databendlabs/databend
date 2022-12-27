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

use common_base::base::ProgressValues;
use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_catalog::plan::Projection;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::Chunk;
use common_expression::Column;
use common_expression::DataField;
use common_expression::DataSchema;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_expression::RemoteExpr;
use common_expression::TableSchema;
use common_expression::Value;
use common_functions_v2::scalars::BUILTIN_FUNCTIONS;
use common_sql::evaluator::ChunkOperator;
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
    /// The flow of Pipeline is as follows:
    /// +---------------+
    /// |DeletionSource1| ------
    /// +---------------+       |      +-----------------+      +------------+
    /// |     ...       | ...   | ---> |DeletionTransform| ---> |MutationSink|
    /// +---------------+       |      +-----------------+      +------------+
    /// |DeletionSourceN| ------
    /// +---------------+
    pub async fn do_delete(
        &self,
        ctx: Arc<dyn TableContext>,
        filter: Option<RemoteExpr<String>>,
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

        let scan_progress = ctx.get_scan_progress();
        // check if unconditional deletion
        if filter.is_none() {
            let progress_values = ProgressValues {
                rows: snapshot.summary.row_count as usize,
                bytes: snapshot.summary.uncompressed_byte_size as usize,
            };
            scan_progress.incr(&progress_values);
            // deleting the whole table... just a truncate
            let purge = false;
            return self.do_truncate(ctx.clone(), purge).await;
        }

        let filter_expr = filter.unwrap();
        if col_indices.is_empty() {
            // here the situation: filter_expr is not null, but col_indices in empty, which
            // indicates the expr being evaluated is unrelated to the value of rows:
            //   e.g.
            //       `delete from t where 1 = 1`, `delete from t where now()`,
            //       or `delete from t where RANDOM()::INT::BOOLEAN`
            // if the `filter_expr` is of "constant" nullary :
            //   for the whole block, whether all of the rows should be kept or dropped,
            //   we can just return from here, without accessing the block data
            if self.try_eval_const(&self.table_info.schema(), &filter_expr)? {
                let progress_values = ProgressValues {
                    rows: snapshot.summary.row_count as usize,
                    bytes: snapshot.summary.uncompressed_byte_size as usize,
                };
                scan_progress.incr(&progress_values);

                // deleting the whole table... just a truncate
                let purge = false;
                return self.do_truncate(ctx.clone(), purge).await;
            }
            // do nothing.
            return Ok(());
        }

        self.try_add_deletion_source(ctx.clone(), &filter_expr, col_indices, &snapshot, pipeline)
            .await?;

        self.try_add_deletion_transform(ctx.clone(), snapshot.segments.clone(), pipeline)?;

        pipeline.add_sink(|input| {
            MutationSink::try_create(self, ctx.clone(), snapshot.clone(), input)
        })?;
        Ok(())
    }

    fn try_eval_const(&self, schema: &TableSchema, filter: &RemoteExpr<String>) -> Result<bool> {
        let func_ctx = FunctionContext::default();

        let dummy_field = DataField::new("dummy", DataType::Null);
        let _dummy_schema = Arc::new(DataSchema::new(vec![dummy_field]));
        let dummy_value = Value::Column(Column::Null { len: 1 });
        let dummy_chunk = Chunk::new_from_sequence(vec![(dummy_value, DataType::Null)], 1);

        let filter_expr = filter
            .into_expr(&BUILTIN_FUNCTIONS)
            .unwrap()
            .project_column_ref(|name| schema.index_of(name).unwrap());
        let evaluator = Evaluator::new(&dummy_chunk, func_ctx, &BUILTIN_FUNCTIONS);
        let res = evaluator
            .run(&filter_expr)
            .map_err(|(_, e)| ErrorCode::Internal(format!("eval try eval const failed: {}.", e)))?;
        let predicates = Chunk::<String>::cast_to_nonull_boolean(&res).ok_or_else(|| {
            ErrorCode::BadArguments("Result of filter expression cannot be converted to boolean.")
        })?;

        let all_filtered = match &predicates {
            Value::Scalar(v) => !v,
            Value::Column(bitmap) => bitmap.unset_bits() == bitmap.len(),
        };

        Ok(all_filtered)
    }

    async fn try_add_deletion_source(
        &self,
        ctx: Arc<dyn TableContext>,
        filter: &RemoteExpr<String>,
        col_indices: Vec<usize>,
        base_snapshot: &TableSnapshot,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let projection = Projection::Columns(col_indices.clone());
        let push_down = Some(PushDownInfo {
            projection: Some(projection.clone()),
            filters: vec![filter.clone()],
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
                    Arc::new(filter.clone()),
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
            None => Err(ErrorCode::Internal("The pipeline is empty.")),
            Some(pipe) if pipe.output_size() == 0 => {
                Err(ErrorCode::Internal("The output of the last pipe is 0."))
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
                    self.get_chunk_compact_thresholds(),
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

    pub fn cluster_stats_gen(&self, ctx: Arc<dyn TableContext>) -> Result<ClusterStatsGenerator> {
        if self.cluster_key_meta.is_none() {
            return Ok(ClusterStatsGenerator::default());
        }

        let input_schema = self.table_info.schema();
        let mut merged: Vec<DataField> =
            input_schema.fields().iter().map(DataField::from).collect();

        let cluster_keys = self.cluster_keys(ctx);
        let mut cluster_key_index = Vec::with_capacity(cluster_keys.len());
        let mut extra_key_index = Vec::with_capacity(cluster_keys.len());
        let mut operators = Vec::with_capacity(cluster_keys.len());

        for remote_expr in &cluster_keys {
            let expr: Expr = remote_expr
                .into_expr(&BUILTIN_FUNCTIONS)
                .unwrap()
                .project_column_ref(|name| input_schema.index_of(name).unwrap());
            let index = match &expr {
                Expr::ColumnRef { id, .. } => *id,
                _ => {
                    let cname = format!("{}", expr);
                    merged.push(DataField::new(cname.as_str(), expr.data_type().clone()));
                    let index = merged.len() - 1;
                    extra_key_index.push(index);

                    operators.push(ChunkOperator::Map { index, expr });
                    index
                }
            };
            cluster_key_index.push(index);
        }

        Ok(ClusterStatsGenerator::new(
            self.cluster_key_meta.as_ref().unwrap().0,
            cluster_key_index,
            extra_key_index,
            0,
            self.get_chunk_compact_thresholds(),
            operators,
            merged,
        ))
    }

    pub fn all_the_columns_ids(&self) -> Vec<usize> {
        (0..self.table_info.schema().fields().len())
            .into_iter()
            .collect::<Vec<usize>>()
    }
}
