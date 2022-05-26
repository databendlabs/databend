//  Copyright 2021 Datafuse Labs.
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
//

use std::str::FromStr;
use std::sync::Arc;

use async_stream::stream;
use common_cache::Cache;
use common_datablocks::SortColumnDescription;
use common_datavalues::DataSchemaRefExt;
use common_exception::Result;
use common_planners::Expression;
use common_streams::SendableDataBlockStream;
use futures::StreamExt;

use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::BlockCompactor;
use crate::pipelines::new::processors::ExpressionTransform;
use crate::pipelines::new::processors::TransformCompact;
use crate::pipelines::new::processors::TransformSortPartial;
use crate::pipelines::new::NewPipeline;
use crate::pipelines::new::SinkPipeBuilder;
use crate::pipelines::transforms::ExpressionExecutor;
use crate::sessions::QueryContext;
use crate::storages::fuse::io::BlockStreamWriter;
use crate::storages::fuse::operations::AppendOperationLogEntry;
use crate::storages::fuse::operations::FuseTableSink;
use crate::storages::fuse::FuseTable;
use crate::storages::fuse::DEFAULT_BLOCK_PER_SEGMENT;
use crate::storages::fuse::DEFAULT_ROW_PER_BLOCK;
use crate::storages::fuse::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;
use crate::storages::fuse::FUSE_OPT_KEY_ROW_PER_BLOCK;

pub type AppendOperationLogEntryStream =
    std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<AppendOperationLogEntry>> + Send>>;

impl FuseTable {
    #[inline]
    pub async fn append_chunks(
        &self,
        ctx: Arc<QueryContext>,
        stream: SendableDataBlockStream,
    ) -> Result<AppendOperationLogEntryStream> {
        let rows_per_block = self.get_option(FUSE_OPT_KEY_ROW_PER_BLOCK, DEFAULT_ROW_PER_BLOCK);

        let block_per_seg =
            self.get_option(FUSE_OPT_KEY_BLOCK_PER_SEGMENT, DEFAULT_BLOCK_PER_SEGMENT);

        let da = ctx.get_storage_operator()?;

        let mut segment_stream = BlockStreamWriter::write_block_stream(
            ctx.clone(),
            stream,
            rows_per_block,
            block_per_seg,
            self.meta_location_generator().clone(),
            self.table_info.schema().clone(),
            self.order_keys.clone(),
        )
        .await;

        let locs = self.meta_location_generator().clone();
        let segment_info_cache = ctx.get_storage_cache_manager().get_table_segment_cache();

        let log_entries = stream! {
            while let Some(segment) = segment_stream.next().await {
                let log_entry_res = match segment {
                    Ok(seg) => {
                        let seg_loc = locs.gen_segment_info_location();
                        let bytes = serde_json::to_vec(&seg)?;
                        da.object(&seg_loc)
                        .write(bytes)
                        .await?;
                        let seg = Arc::new(seg);
                        let log_entry = AppendOperationLogEntry::new(seg_loc.clone(), seg.clone());
                        if let Some(ref cache) = segment_info_cache {
                            let cache = &mut cache.write().await;
                            cache.put(seg_loc, seg);
                        }

                        Ok(log_entry)
                    },
                    Err(err) => Err(err),
                };
                yield(log_entry_res);
            }
        };

        Ok(Box::pin(log_entries))
    }

    pub fn do_append2(&self, ctx: Arc<QueryContext>, pipeline: &mut NewPipeline) -> Result<()> {
        let max_row_per_block = self.get_option(FUSE_OPT_KEY_ROW_PER_BLOCK, DEFAULT_ROW_PER_BLOCK);
        let min_rows_per_block = (max_row_per_block as f64 * 0.8) as usize;
        let block_per_seg =
            self.get_option(FUSE_OPT_KEY_BLOCK_PER_SEGMENT, DEFAULT_BLOCK_PER_SEGMENT);

        let da = ctx.get_storage_operator()?;

        pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformCompact::try_create(
                transform_input_port,
                transform_output_port,
                BlockCompactor::new(max_row_per_block, min_rows_per_block),
            )
        })?;

        let mut cluster_keys_index = Vec::with_capacity(self.order_keys.len());
        let mut expression_executor = None;
        if !self.order_keys.is_empty() {
            let input_schema = self.table_info.schema();
            let mut merged = input_schema.fields().clone();

            for expr in &self.order_keys {
                let cname = expr.column_name();
                let index = match merged.iter().position(|x| x.name() == &cname) {
                    None => {
                        merged.push(expr.to_data_field(&input_schema)?);
                        merged.len() - 1
                    }
                    Some(idx) => idx,
                };
                cluster_keys_index.push(index);
            }

            let output_schema = DataSchemaRefExt::create(merged);

            if output_schema != input_schema {
                pipeline.add_transform(|transform_input_port, transform_output_port| {
                    ExpressionTransform::try_create(
                        transform_input_port,
                        transform_output_port,
                        input_schema.clone(),
                        output_schema.clone(),
                        self.order_keys.clone(),
                        ctx.clone(),
                    )
                })?;

                let exprs: Vec<Expression> = output_schema
                    .fields()
                    .iter()
                    .map(|f| Expression::Column(f.name().to_owned()))
                    .collect();

                let executor = ExpressionExecutor::try_create(
                    ctx.clone(),
                    "remove unused columns",
                    output_schema.clone(),
                    input_schema.clone(),
                    exprs,
                    true,
                )?;
                executor.validate()?;
                expression_executor = Some(executor);
            }

            // sort
            let sort_descs: Vec<SortColumnDescription> = self
                .order_keys
                .iter()
                .map(|expr| SortColumnDescription {
                    column_name: expr.column_name(),
                    asc: true,
                    nulls_first: false,
                })
                .collect();

            pipeline.add_transform(|transform_input_port, transform_output_port| {
                TransformSortPartial::try_create(
                    transform_input_port,
                    transform_output_port,
                    None,
                    sort_descs.clone(),
                )
            })?;
        }

        let mut sink_pipeline_builder = SinkPipeBuilder::create();
        for _ in 0..pipeline.output_len() {
            let input_port = InputPort::create();
            sink_pipeline_builder.add_sink(
                input_port.clone(),
                FuseTableSink::create(
                    input_port,
                    ctx.clone(),
                    block_per_seg,
                    da.clone(),
                    self.meta_location_generator().clone(),
                    cluster_keys_index.clone(),
                    expression_executor.clone(),
                )?,
            );
        }

        pipeline.add_pipe(sink_pipeline_builder.finalize());
        Ok(())
    }

    fn get_option<T: FromStr>(&self, opt_key: &str, default: T) -> T {
        self.table_info
            .options()
            .get(opt_key)
            .and_then(|s| s.parse::<T>().ok())
            .unwrap_or(default)
    }
}
