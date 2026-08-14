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

use std::str::FromStr;
use std::sync::Arc;

use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::ColumnRef;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::Expr;
use databend_common_expression::LimitType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::AccumulatingTransformer;
use databend_common_pipeline_transforms::BlockCompactBuilder;
use databend_common_pipeline_transforms::TransformCompactBlock;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_pipeline_transforms::blocks::CompoundBlockOperator;
use databend_common_pipeline_transforms::build_compact_block_pipeline;
use databend_common_pipeline_transforms::create_dummy_item;
use databend_common_pipeline_transforms::sorts::TransformSortPartial;
use databend_common_sql::ClusterKeys;
use databend_common_sql::bind_normalized_key_exprs;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::parse_cluster_keys;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;

use crate::FuseTable;
use crate::io::StreamBlockProperties;
use crate::operations::TransformBlockBuilder;
use crate::operations::TransformBlockWriter;
use crate::operations::TransformPartitionBy;
use crate::operations::TransformSerializeBlock;
use crate::operations::TransformVectorCluster;
use crate::statistics::ClusterStatsGenerator;
use crate::statistics::ClusterStatsKey;
use crate::statistics::ClusterStatsLayout;
use crate::statistics::VectorClusterOperator;
use crate::statistics::vector_cluster_info_from_column;

impl FuseTable {
    pub fn do_append_data(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<()> {
        // Stream block writing does not expose a partition-boundary split point.
        // Use the regular append pipeline for partitioned tables.
        let enable_stream_block_write =
            self.partition_key_count() == 0 && self.enable_stream_block_write(ctx.clone())?;
        if enable_stream_block_write {
            let properties = StreamBlockProperties::try_create(
                ctx.clone(),
                self,
                MutationKind::Insert,
                table_meta_timestamps,
            )?;

            pipeline.add_transform(|input, output| {
                TransformBlockBuilder::try_create(input, output, properties.clone())
            })?;

            pipeline.add_async_accumulating_transformer(|| {
                TransformBlockWriter::create(ctx.clone(), MutationKind::Insert, self, false)
            });
        } else {
            let block_thresholds = self.get_block_thresholds();
            if self.use_hash_write_distribution() {
                pipeline
                    .add_accumulating_transformer(|| BlockCompactBuilder::new(block_thresholds));
                pipeline.add_block_meta_transformer(|| TransformCompactBlock);
            } else {
                build_compact_block_pipeline(pipeline, block_thresholds)?;
            }

            let schema = DataSchema::from(self.schema()).into();
            let cluster_stats_gen =
                self.cluster_gen_for_append(ctx.clone(), pipeline, block_thresholds, Some(schema))?;
            pipeline.add_transform(|input, output| {
                let proc = TransformSerializeBlock::try_create(
                    ctx.clone(),
                    input,
                    output,
                    self,
                    cluster_stats_gen.clone(),
                    MutationKind::Insert,
                    table_meta_timestamps,
                )?;
                proc.into_processor()
            })?;
        }

        Ok(())
    }

    pub fn cluster_gen_for_append_with_specified_len(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        block_thresholds: BlockThresholds,
        transform_len: usize,
        need_match: bool,
    ) -> Result<ClusterStatsGenerator> {
        let input_schema = DataSchema::from(self.schema_with_stream()).into();
        let cluster_stats_gen =
            self.get_cluster_stats_gen(ctx.clone(), 0, block_thresholds, input_schema)?;

        if !cluster_stats_gen.eval_operators.is_empty() {
            let eval_operators = cluster_stats_gen.eval_operators.clone();
            let num_input_columns = self.schema().fields().len();
            let func_ctx2 = cluster_stats_gen.func_ctx.clone();
            let mut builder = pipeline.try_create_transform_pipeline_builder_with_len(
                move || {
                    Ok(CompoundBlockOperator::new(
                        eval_operators.clone(),
                        func_ctx2.clone(),
                        num_input_columns,
                    ))
                },
                transform_len,
            )?;
            if need_match {
                builder.add_items_prepend(vec![create_dummy_item()]);
            }
            pipeline.add_pipe(builder.finalize());
        }

        let partition_key_indices: Arc<[_]> = cluster_stats_gen.partition_key_index.clone().into();
        if !partition_key_indices.is_empty() {
            let mut builder = pipeline.add_transform_with_specified_len(
                move |input, output| {
                    Ok(ProcessorPtr::create(AccumulatingTransformer::create(
                        input,
                        output,
                        TransformPartitionBy::new(partition_key_indices.clone()),
                    )))
                },
                transform_len,
            )?;
            if need_match {
                builder.add_items_prepend(vec![create_dummy_item()]);
            }
            pipeline.add_pipe(builder.finalize());
        }

        if let Some(vector_operator) = cluster_stats_gen.vector_operator() {
            let rows_per_block = block_thresholds.max_rows_per_block;
            let vector_column_input_offset = vector_operator.vector_column_input_offset;
            let dimension = vector_operator.info.dimension;
            let distance_type = vector_operator.info.distance_type;
            let mut builder = pipeline.add_transform_with_specified_len(
                move |input, output| {
                    Ok(ProcessorPtr::create(AccumulatingTransformer::create(
                        input,
                        output,
                        TransformVectorCluster::new(
                            vector_column_input_offset,
                            dimension,
                            distance_type,
                            rows_per_block,
                        ),
                    )))
                },
                transform_len,
            )?;
            if need_match {
                builder.add_items_prepend(vec![create_dummy_item()]);
            }
            pipeline.add_pipe(builder.finalize());
        }

        let sort_desc: Arc<[_]> = cluster_stats_gen.sort_descs().into();
        if !sort_desc.is_empty() {
            let mut builder = pipeline.try_create_transform_pipeline_builder_with_len(
                || {
                    Ok(TransformSortPartial::new(
                        LimitType::None,
                        sort_desc.clone(),
                    ))
                },
                transform_len,
            )?;
            if need_match {
                builder.add_items_prepend(vec![create_dummy_item()]);
            }
            pipeline.add_pipe(builder.finalize());
        }
        Ok(cluster_stats_gen)
    }

    pub fn cluster_gen_for_append(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        block_thresholds: BlockThresholds,
        modified_schema: Option<Arc<DataSchema>>,
    ) -> Result<ClusterStatsGenerator> {
        self.cluster_gen_for_append_impl(ctx, pipeline, block_thresholds, modified_schema, false)
    }

    pub fn cluster_gen_for_update(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        block_thresholds: BlockThresholds,
        modified_schema: Option<Arc<DataSchema>>,
    ) -> Result<ClusterStatsGenerator> {
        self.cluster_gen_for_append_impl(ctx, pipeline, block_thresholds, modified_schema, true)
    }

    fn cluster_gen_for_append_impl(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        block_thresholds: BlockThresholds,
        modified_schema: Option<Arc<DataSchema>>,
        rewrite_replaced_block: bool,
    ) -> Result<ClusterStatsGenerator> {
        let input_schema =
            modified_schema.unwrap_or(DataSchema::from(self.schema_with_stream()).into());
        let cluster_stats_gen =
            self.get_cluster_stats_gen(ctx.clone(), 0, block_thresholds, input_schema)?;

        if !cluster_stats_gen.eval_operators.is_empty() {
            let eval_operators = cluster_stats_gen.eval_operators.clone();
            let num_input_columns = self.schema().fields().len();
            let func_ctx2 = cluster_stats_gen.func_ctx.clone();

            pipeline.add_transformer(move || {
                CompoundBlockOperator::new(
                    eval_operators.clone(),
                    func_ctx2.clone(),
                    num_input_columns,
                )
            });
        }

        let partition_key_indices: Arc<[_]> = cluster_stats_gen.partition_key_index.clone().into();
        if !rewrite_replaced_block && !partition_key_indices.is_empty() {
            pipeline.add_accumulating_transformer({
                let partition_key_indices = partition_key_indices.clone();
                move || TransformPartitionBy::new(partition_key_indices.clone())
            });
        }

        if let Some(vector_operator) = cluster_stats_gen.vector_operator() {
            let rows_per_block = block_thresholds.max_rows_per_block;
            let vector_column_input_offset = vector_operator.vector_column_input_offset;
            let dimension = vector_operator.info.dimension;
            let distance_type = vector_operator.info.distance_type;
            pipeline.add_accumulating_transformer(move || {
                TransformVectorCluster::new(
                    vector_column_input_offset,
                    dimension,
                    distance_type,
                    rows_per_block,
                )
            });
        }

        let sort_desc: Arc<[_]> = cluster_stats_gen.sort_descs().into();
        if !sort_desc.is_empty() {
            pipeline.add_transformer({
                let sort_desc = sort_desc.clone();
                move || TransformSortPartial::new(LimitType::None, sort_desc.clone())
            });
        }
        if rewrite_replaced_block && !partition_key_indices.is_empty() {
            pipeline.add_accumulating_transformer(move || {
                TransformPartitionBy::new_for_update(partition_key_indices.clone())
            });
        }
        Ok(cluster_stats_gen)
    }

    /// Build the evaluated partition/cluster-key layout and its statistics generator.
    ///
    /// Pure Hilbert keys are evaluated for two-dimensional MBR statistics but are not used as
    /// ordinary lexicographic sort keys.
    pub fn get_cluster_stats_gen(
        &self,
        ctx: Arc<dyn TableContext>,
        level: i32,
        block_thresholds: BlockThresholds,
        input_schema: Arc<DataSchema>,
    ) -> Result<ClusterStatsGenerator> {
        let table_meta: Arc<dyn Table> = Arc::new(self.clone());
        let partition_keys = self
            .resolve_partition_keys()
            .map(|keys| bind_normalized_key_exprs(ctx.clone(), table_meta.clone(), keys))
            .transpose()?
            .unwrap_or_default();
        let parsed_cluster_keys = self
            .resolve_cluster_keys()
            .map(|keys| parse_cluster_keys(ctx.clone(), table_meta, keys))
            .transpose()?;
        if partition_keys.is_empty() && parsed_cluster_keys.is_none() {
            return Ok(ClusterStatsGenerator::default());
        }

        let table_schema = self.schema();
        let (is_hilbert, vector_key_index, cluster_key_exprs) = match parsed_cluster_keys {
            Some(ClusterKeys::Linear(keys)) => (false, None, keys),
            Some(ClusterKeys::Vector { keys, vector_index }) => (false, Some(vector_index), keys),
            Some(ClusterKeys::Hilbert(dimensions)) => (true, None, dimensions),
            None => (false, None, Vec::new()),
        };
        let input_offset = |id: &usize| input_schema.index_of(table_schema.field(*id).name());
        let mut merged = input_schema.fields().clone();
        let mut partition_key_index = Vec::with_capacity(partition_keys.len());
        let mut stats_keys = Vec::with_capacity(cluster_key_exprs.len());
        let mut extra_key_num = 0;
        let mut exprs = Vec::with_capacity(partition_keys.len() + cluster_key_exprs.len());

        for partition_key in partition_keys {
            let expr = partition_key.project_column_ref(input_offset)?;
            let index = match expr {
                Expr::ColumnRef(ColumnRef { id, .. }) => id,
                expr => {
                    let name = format!("{}", expr);
                    merged.push(DataField::new(name.as_str(), expr.data_type().clone()));
                    exprs.push(expr);
                    extra_key_num += 1;
                    merged.len() - 1
                }
            };
            partition_key_index.push(index);
        }

        let mut vector_cluster_info = None;
        for (key_index, cluster_key_expr) in cluster_key_exprs.iter().enumerate() {
            let expr = cluster_key_expr.project_column_ref(input_offset)?;
            let is_vector_key = Some(key_index) == vector_key_index;
            if is_vector_key {
                let DataType::Vector(vector_ty) = expr.data_type().remove_nullable() else {
                    return Err(ErrorCode::InvalidClusterKeys(
                        "Vector cluster key must be vector type",
                    ));
                };
                let Expr::ColumnRef(ColumnRef { id, .. }) = &expr else {
                    return Err(ErrorCode::InvalidClusterKeys(
                        "Vector cluster key only supports direct column reference",
                    ));
                };
                let input_field = input_schema.field(*id);
                let field = table_schema.field_with_name(input_field.name())?;
                let dimension: usize = vector_ty.dimension().try_into().map_err(|_| {
                    ErrorCode::InvalidClusterKeys(
                        "Vector cluster key dimension is too large for kmeans",
                    )
                })?;
                if dimension == 0 {
                    return Err(ErrorCode::InvalidClusterKeys(
                        "Vector cluster key dimension must be greater than zero",
                    ));
                }
                let vector_info = vector_cluster_info_from_column(
                    &self.table_info.meta.indexes,
                    key_index,
                    field.column_id(),
                    field.name(),
                    dimension,
                )?;
                vector_cluster_info = Some((vector_info, *id));
            }
            let index = match expr {
                Expr::ColumnRef(ColumnRef { id, .. }) => id,
                expr => {
                    let name = format!("{}", expr);
                    merged.push(DataField::new(name.as_str(), expr.data_type().clone()));
                    exprs.push(expr);
                    extra_key_num += 1;
                    merged.len() - 1
                }
            };
            if !is_vector_key {
                stats_keys.push(ClusterStatsKey {
                    offset: index,
                    source_column_id: match cluster_key_expr {
                        Expr::ColumnRef(ColumnRef { id, .. }) => {
                            Some(table_schema.field(*id).column_id())
                        }
                        _ => None,
                    },
                });
            }
        }

        let eval_operators = if exprs.is_empty() {
            vec![]
        } else {
            vec![BlockOperator::Map {
                exprs,
                projections: None,
            }]
        };
        let vector_operator =
            if let Some((vector_info, vector_column_input_offset)) = vector_cluster_info {
                debug_assert!(vector_info.key_index <= stats_keys.len());
                let cluster_id_offset = merged.len();
                merged.push(DataField::new(
                    "_vector_cluster_sort_key",
                    DataType::Number(NumberDataType::UInt64),
                ));
                extra_key_num += 1;
                Some(VectorClusterOperator {
                    info: vector_info,
                    vector_column_input_offset,
                    vector_cluster_id_offset: cluster_id_offset,
                })
            } else {
                None
            };

        let layout = if let Some(vector_operator) = vector_operator {
            ClusterStatsLayout::Vector(vector_operator)
        } else if is_hilbert {
            ClusterStatsLayout::Hilbert
        } else {
            ClusterStatsLayout::Linear
        };

        let mut generator = ClusterStatsGenerator::new(
            self.cluster_key_id().unwrap_or(0),
            stats_keys,
            extra_key_num,
            level,
            block_thresholds,
            eval_operators,
            layout,
            merged,
            ctx.get_function_context()?,
        );
        generator.partition_key_index = partition_key_index;
        Ok(generator)
    }

    pub fn get_option<T: FromStr>(&self, opt_key: &str, default: T) -> T {
        self.table_info.get_option(opt_key, default)
    }
}
