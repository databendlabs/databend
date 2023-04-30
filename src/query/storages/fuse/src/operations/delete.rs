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

use common_base::base::ProgressValues;
use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_catalog::plan::Projection;
use common_catalog::plan::PruningStatistics;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::DataField;
use common_expression::DataSchema;
use common_expression::Evaluator;
use common_expression::FieldIndex;
use common_expression::RemoteExpr;
use common_expression::TableSchema;
use common_expression::Value;
use common_functions::BUILTIN_FUNCTIONS;
use common_pipeline_core::pipe::Pipe;
use common_pipeline_core::pipe::PipeItem;
use common_sql::evaluator::BlockOperator;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::TableSnapshot;

use crate::operations::mutation::MutationAction;
use crate::operations::mutation::MutationPartInfo;
use crate::operations::mutation::MutationSink;
use crate::operations::mutation::MutationSource;
use crate::operations::mutation::MutationTransform;
use crate::operations::mutation::SerializeDataTransform;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::Pipeline;
use crate::pruning::FusePruner;
use crate::FuseTable;

impl FuseTable {
    /// The flow of Pipeline is as follows:
    /// +---------------+      +-----------------------+
    /// |MutationSource1| ---> |SerializeDataTransform1|   ------
    /// +---------------+      +-----------------------+         |      +-----------------+      +------------+
    /// |     ...       | ---> |          ...          |   ...   | ---> |MutationTransform| ---> |MutationSink|
    /// +---------------+      +-----------------------+         |      +-----------------+      +------------+
    /// |MutationSourceN| ---> |SerializeDataTransformN|   ------
    /// +---------------+      +-----------------------+
    #[async_backtrace::framed]
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

        // check if unconditional deletion
        if filter.is_none() {
            let progress_values = ProgressValues {
                rows: snapshot.summary.row_count as usize,
                bytes: snapshot.summary.uncompressed_byte_size as usize,
            };
            ctx.get_write_progress().incr(&progress_values);
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
            if self.try_eval_const(ctx.clone(), &self.schema(), &filter_expr)? {
                let progress_values = ProgressValues {
                    rows: snapshot.summary.row_count as usize,
                    bytes: snapshot.summary.uncompressed_byte_size as usize,
                };
                ctx.get_write_progress().incr(&progress_values);

                // deleting the whole table... just a truncate
                let purge = false;
                return self.do_truncate(ctx.clone(), purge).await;
            }
            // do nothing.
            return Ok(());
        }

        self.try_add_deletion_source(ctx.clone(), &filter_expr, col_indices, &snapshot, pipeline)
            .await?;

        let cluster_stats_gen =
            self.get_cluster_stats_gen(ctx.clone(), 0, self.get_block_thresholds())?;
        pipeline.add_transform(|input, output| {
            SerializeDataTransform::try_create(
                ctx.clone(),
                input,
                output,
                self,
                cluster_stats_gen.clone(),
            )
        })?;

        self.try_add_mutation_transform(ctx.clone(), snapshot.segments.clone(), pipeline)?;

        pipeline.add_sink(|input| {
            MutationSink::try_create(self, ctx.clone(), snapshot.clone(), input)
        })?;
        Ok(())
    }

    pub fn try_eval_const(
        &self,
        ctx: Arc<dyn TableContext>,
        schema: &TableSchema,
        filter: &RemoteExpr<String>,
    ) -> Result<bool> {
        let dummy_field = DataField::new("dummy", DataType::Null);
        let _dummy_schema = Arc::new(DataSchema::new(vec![dummy_field]));
        let dummy_value = Value::Column(Column::Null { len: 1 });
        let dummy_block = DataBlock::new(
            vec![BlockEntry {
                data_type: DataType::Null,
                value: dummy_value,
            }],
            1,
        );

        let filter = filter
            .as_expr(&BUILTIN_FUNCTIONS)
            .project_column_ref(|name| schema.index_of(name).unwrap());

        assert_eq!(filter.data_type(), &DataType::Boolean);

        let func_ctx = ctx.get_function_context()?;
        let evaluator = Evaluator::new(&dummy_block, &func_ctx, &BUILTIN_FUNCTIONS);
        let predicates = evaluator
            .run(&filter)
            .map_err(|e| e.add_message("eval try eval const failed:"))?
            .try_downcast::<BooleanType>()
            .unwrap();

        Ok(match &predicates {
            Value::Scalar(v) => *v,
            Value::Column(bitmap) => bitmap.unset_bits() == 0,
        })
    }

    #[async_backtrace::framed]
    async fn try_add_deletion_source(
        &self,
        ctx: Arc<dyn TableContext>,
        filter: &RemoteExpr<String>,
        col_indices: Vec<usize>,
        base_snapshot: &TableSnapshot,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let projection = Projection::Columns(col_indices.clone());
        self.mutation_block_pruning(
            ctx.clone(),
            Some(filter.clone()),
            projection.clone(),
            base_snapshot,
        )
        .await?;

        let block_reader = self.create_block_reader(projection, false, ctx.clone())?;
        let schema = block_reader.schema();
        let filter = Arc::new(Some(
            filter
                .as_expr(&BUILTIN_FUNCTIONS)
                .project_column_ref(|name| schema.index_of(name).unwrap()),
        ));

        let all_column_indices = self.all_column_indices();
        let remain_column_indices: Vec<usize> = all_column_indices
            .into_iter()
            .filter(|index| !col_indices.contains(index))
            .collect();
        let mut source_col_indices = col_indices;
        let remain_reader = if remain_column_indices.is_empty() {
            Arc::new(None)
        } else {
            source_col_indices.extend_from_slice(&remain_column_indices);
            Arc::new(Some(
                (*self.create_block_reader(
                    Projection::Columns(remain_column_indices),
                    false,
                    ctx.clone(),
                )?)
                .clone(),
            ))
        };

        // resort the block.
        let mut projection = (0..source_col_indices.len()).collect::<Vec<_>>();
        projection.sort_by_key(|&i| source_col_indices[i]);
        let ops = vec![BlockOperator::Project { projection }];

        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        // Add source pipe.
        pipeline.add_source(
            |output| {
                MutationSource::try_create(
                    ctx.clone(),
                    MutationAction::Deletion,
                    output,
                    filter.clone(),
                    block_reader.clone(),
                    remain_reader.clone(),
                    ops.clone(),
                    self.storage_format,
                )
            },
            max_threads,
        )
    }

    #[async_backtrace::framed]
    pub async fn mutation_block_pruning(
        &self,
        ctx: Arc<dyn TableContext>,
        filter: Option<RemoteExpr<String>>,
        projection: Projection,
        base_snapshot: &TableSnapshot,
    ) -> Result<()> {
        let push_down = Some(PushDownInfo {
            projection: Some(projection),
            filter,
            ..PushDownInfo::default()
        });

        let segment_locations = base_snapshot.segments.clone();
        let pruner = FusePruner::create(
            &ctx,
            self.operator.clone(),
            self.table_info.schema(),
            &push_down,
        )?;
        let block_metas = pruner.pruning(segment_locations, None, None).await?;

        let range_block_metas = block_metas
            .clone()
            .into_iter()
            .map(|(a, b)| (Some(a), b))
            .collect::<Vec<_>>();

        let (_, inner_parts) = self.read_partitions_with_metas(
            self.table_info.schema(),
            None,
            &range_block_metas,
            base_snapshot.summary.block_count as usize,
            PruningStatistics::default(),
        )?;

        let parts = Partitions::create_nolazy(
            PartitionsShuffleKind::Mod,
            block_metas
                .into_iter()
                .zip(inner_parts.partitions.into_iter())
                .map(|(a, c)| MutationPartInfo::create(a.0, a.1.cluster_stats.clone(), c))
                .collect(),
        );
        ctx.set_partitions(parts)
    }

    pub fn try_add_mutation_transform(
        &self,
        ctx: Arc<dyn TableContext>,
        base_segments: Vec<Location>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        if pipeline.is_empty() {
            return Err(ErrorCode::Internal("The pipeline is empty."));
        }

        match pipeline.output_len() {
            0 => Err(ErrorCode::Internal("The output of the last pipe is 0.")),
            last_pipe_size => {
                let mut inputs_port = Vec::with_capacity(last_pipe_size);
                for _ in 0..last_pipe_size {
                    inputs_port.push(InputPort::create());
                }
                let output_port = OutputPort::create();
                pipeline.add_pipe(Pipe::create(inputs_port.len(), 1, vec![PipeItem::create(
                    MutationTransform::try_create(
                        ctx,
                        self.schema(),
                        inputs_port.clone(),
                        output_port.clone(),
                        self.get_operator(),
                        self.meta_location_generator().clone(),
                        base_segments,
                        self.get_block_thresholds(),
                    )?,
                    inputs_port,
                    vec![output_port],
                )]));

                Ok(())
            }
        }
    }

    pub fn all_column_indices(&self) -> Vec<FieldIndex> {
        (0..self.table_info.schema().fields().len()).collect::<Vec<FieldIndex>>()
    }
}
