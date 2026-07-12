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
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_pipeline_transforms::blocks::CompoundBlockOperator;
use databend_common_pipeline_transforms::build_compact_block_pipeline;
use databend_common_pipeline_transforms::create_dummy_item;
use databend_common_pipeline_transforms::sorts::TransformSortPartial;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::parse_cluster_keys;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::table::ClusterType;

use crate::FuseTable;
use crate::io::StreamBlockProperties;
use crate::operations::TransformBlockBuilder;
use crate::operations::TransformBlockWriter;
use crate::operations::TransformSerializeBlock;
use crate::operations::TransformVectorCluster;
use crate::statistics::ClusterStatsGenerator;
use crate::statistics::ClusterStatsOperator;
use crate::statistics::HilbertClusterOperator;
use crate::statistics::VectorClusterOperator;
use crate::statistics::vector_cluster_info_from_column;

impl FuseTable {
    pub fn do_append_data(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<()> {
        let enable_stream_block_write = self.enable_stream_block_write(ctx.clone())?;
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
            build_compact_block_pipeline(pipeline, block_thresholds)?;

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
        Ok(cluster_stats_gen)
    }

    /// Build a stats generator for recluster, adding the task-local Hilbert proxy sort key.
    ///
    /// `get_cluster_stats_gen` intentionally keeps Hilbert dimensions out of append sorting
    /// and only persists their original min/max stats. Recluster is the only path that
    /// appends `_hilbert_cluster_sort_key`, sorts by it, then drops it before writing.
    pub fn get_recluster_stats_gen(
        &self,
        ctx: Arc<dyn TableContext>,
        level: i32,
        block_thresholds: BlockThresholds,
        input_schema: Arc<DataSchema>,
    ) -> Result<ClusterStatsGenerator> {
        let mut cluster_stats_gen =
            self.get_cluster_stats_gen(ctx, level, block_thresholds, input_schema)?;
        if let Some(ClusterStatsOperator::Hilbert(operator)) =
            cluster_stats_gen.special_operator.as_mut()
        {
            let offset = cluster_stats_gen.out_fields.len();
            cluster_stats_gen.cluster_key_index.push(offset);
            cluster_stats_gen.extra_key_num += 1;
            cluster_stats_gen.out_fields.push(DataField::new(
                "_hilbert_cluster_sort_key",
                DataType::Binary,
            ));
            operator.hilbert_proxy_offset = Some(offset);
        }
        Ok(cluster_stats_gen)
    }

    /// Build a stats generator for append/mutation without Hilbert proxy sorting.
    ///
    /// For `CLUSTER BY (prefix..., hilbert(...))`, append only sorts by the scalar prefix.
    /// The Hilbert dimensions are still evaluated and stored in `ClusterStatistics` as a
    /// trailing tuple so later recluster tasks can choose overlap candidates from the
    /// original dimension bbox. They must not be added to `cluster_key_index` here.
    pub fn get_cluster_stats_gen(
        &self,
        ctx: Arc<dyn TableContext>,
        level: i32,
        block_thresholds: BlockThresholds,
        input_schema: Arc<DataSchema>,
    ) -> Result<ClusterStatsGenerator> {
        if !matches!(self.cluster_type(), Some(ClusterType::Linear)) {
            return Ok(ClusterStatsGenerator::default());
        }

        let table_schema = self.schema();
        let Some(ast_cluster_key_exprs) = self.resolve_cluster_keys() else {
            return Err(ErrorCode::Internal(
                "cluster stats generator requires cluster key expressions",
            ));
        };
        let Some(cluster_key_id) = self.cluster_key_id() else {
            return Err(ErrorCode::Internal(
                "cluster stats generator requires cluster key id",
            ));
        };
        let parsed_cluster_keys =
            parse_cluster_keys(ctx.clone(), Arc::new(self.clone()), ast_cluster_key_exprs)?;
        let kind = parsed_cluster_keys.kind;
        let hilbert_len = parsed_cluster_keys.hilbert_len();
        let hilbert_start = kind.hilbert_start();
        let vector_key_index = kind.vector_index();
        let cluster_key_exprs = parsed_cluster_keys.keys;
        let input_offset = |id: &usize| input_schema.index_of(table_schema.field(*id).name());
        let mut merged = input_schema.fields().clone();
        let mut cluster_key_index =
            Vec::with_capacity(cluster_key_exprs.len().saturating_sub(hilbert_len));
        let mut stats_key_offsets = Vec::with_capacity(cluster_key_exprs.len());
        let mut extra_key_num = 0;

        let mut exprs = Vec::with_capacity(cluster_key_exprs.len());
        let mut vector_cluster_info = None;
        let mut hilbert_dimension_offsets = Vec::with_capacity(hilbert_len);

        for (key_index, cluster_key_expr) in cluster_key_exprs.iter().enumerate() {
            let expr = cluster_key_expr.project_column_ref(input_offset)?;
            let is_vector_key = Some(key_index) == vector_key_index;
            let is_hilbert_dimension = hilbert_start.is_some_and(|start| key_index >= start);
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
            if is_hilbert_dimension {
                // Hilbert dimensions are persisted as bbox stats, but append does not sort
                // by them. Recluster injects the transient proxy sort key when it rewrites.
                hilbert_dimension_offsets.push(index);
                stats_key_offsets.push(index);
            } else {
                cluster_key_index.push(index);
                if !is_vector_key {
                    stats_key_offsets.push(index);
                }
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
                debug_assert!(vector_info.key_index < cluster_key_index.len());
                let cluster_id_offset = merged.len();
                merged.push(DataField::new(
                    "_vector_cluster_sort_key",
                    DataType::Number(NumberDataType::UInt64),
                ));
                extra_key_num += 1;
                // Keep the original CLUSTER BY order. For CLUSTER BY (a, embedding, b),
                // sorting should use (a, _vector_cluster_sort_key, b), not append the
                // vector sort key after all scalar keys.
                cluster_key_index[vector_info.key_index] = cluster_id_offset;
                Some(VectorClusterOperator {
                    info: vector_info,
                    vector_column_input_offset,
                    vector_cluster_id_offset: cluster_id_offset,
                })
            } else {
                None
            };

        let special_operator = if let Some(vector_operator) = vector_operator {
            if !hilbert_dimension_offsets.is_empty() {
                return Err(ErrorCode::InvalidClusterKeys(
                    "hilbert cluster marker cannot be used with vector cluster key",
                ));
            }
            Some(ClusterStatsOperator::Vector(vector_operator))
        } else if hilbert_dimension_offsets.is_empty() {
            None
        } else {
            Some(ClusterStatsOperator::Hilbert(HilbertClusterOperator {
                dimension_offsets: hilbert_dimension_offsets,
                hilbert_proxy_offset: None,
            }))
        };

        Ok(ClusterStatsGenerator::new(
            cluster_key_id,
            cluster_key_index,
            stats_key_offsets,
            extra_key_num,
            None,
            level,
            block_thresholds,
            eval_operators,
            special_operator,
            merged,
            ctx.get_function_context()?,
        ))
    }

    pub fn get_option<T: FromStr>(&self, opt_key: &str, default: T) -> T {
        self.table_info.get_option(opt_key, default)
    }
}
