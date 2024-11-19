use std::collections::HashMap;
use std::sync::Arc;

use async_channel::Sender;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::TopK;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sinks::AsyncSink;
use databend_common_pipeline_sinks::AsyncSinker;
use databend_common_storage::ColumnNodes;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_table_meta::meta::BlockMeta;

use crate::pruning_pipeline::block_prune_result_meta::BlockPruneResult;
use crate::FuseBlockPartInfo;

pub struct SendPartInfoSink {
    sender: Option<Sender<Result<PartInfoPtr>>>,
    push_downs: Option<PushDownInfo>,
    top_k: Option<(TopK, Scalar)>,
    schema: TableSchemaRef,
}

impl SendPartInfoSink {
    pub fn create(
        input: Arc<InputPort>,
        sender: Sender<Result<PartInfoPtr>>,
        push_downs: Option<PushDownInfo>,
        top_k: Option<(TopK, Scalar)>,
        schema: TableSchemaRef,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(AsyncSinker::create(
            input,
            SendPartInfoSink {
                sender: Some(sender),
                push_downs,
                top_k,
                schema,
            },
        )))
    }
}

#[async_trait::async_trait]
impl AsyncSink for SendPartInfoSink {
    const NAME: &'static str = "SendPartInfoSink";

    async fn on_finish(&mut self) -> Result<()> {
        drop(self.sender.take());
        Ok(())
    }

    async fn consume(&mut self, mut data_block: DataBlock) -> Result<bool> {
        if let Some(meta) = data_block.take_meta() {
            if let Some(data) = BlockPruneResult::downcast_from(meta) {
                let arrow_schema = self.schema.as_ref().into();
                let column_nodes = ColumnNodes::new_from_schema(&arrow_schema, Some(&self.schema));
                let block_metas = &data.block_metas;
                let info_ptr = match &self.push_downs {
                    None => self.all_columns_partitions(block_metas),
                    Some(extras) => match &extras.projection {
                        None => self.all_columns_partitions(block_metas),
                        Some(projection) => {
                            self.projection_partitions(block_metas, projection, &column_nodes)
                        }
                    },
                };

                for info in info_ptr {
                    if let Some(sender) = &self.sender {
                        let _ = dbg!(sender.send(Ok(info)).await);
                    }
                }

                return Ok(false);
            }
        }
        Err(ErrorCode::Internal(
            "Cannot downcast data block meta to BlockPruneResult".to_string(),
        ))
    }
}

impl SendPartInfoSink {
    fn all_columns_partitions(
        &self,
        block_metas: &[(BlockMetaIndex, Arc<BlockMeta>)],
    ) -> Vec<PartInfoPtr> {
        block_metas
            .iter()
            .map(|(block_meta_index, block_meta)| {
                let mut columns_meta = HashMap::with_capacity(block_meta.col_metas.len());
                let mut columns_stats = HashMap::with_capacity(block_meta.col_stats.len());

                for column_id in block_meta.col_metas.keys() {
                    // ignore all deleted field
                    if self.schema.is_column_deleted(*column_id) {
                        continue;
                    }

                    // ignore column this block dose not exist
                    if let Some(meta) = block_meta.col_metas.get(column_id) {
                        columns_meta.insert(*column_id, meta.clone());
                    }

                    if let Some(stat) = block_meta.col_stats.get(column_id) {
                        columns_stats.insert(*column_id, stat.clone());
                    }
                }

                let rows_count = block_meta.row_count;
                let location = block_meta.location.0.clone();
                let create_on = block_meta.create_on;

                let sort_min_max = self.top_k.as_ref().map(|(top_k, default)| {
                    block_meta
                        .col_stats
                        .get(&top_k.field.column_id)
                        .map(|stat| (stat.min().clone(), stat.max().clone()))
                        .unwrap_or((default.clone(), default.clone()))
                });

                FuseBlockPartInfo::create(
                    location,
                    rows_count,
                    columns_meta,
                    Some(columns_stats),
                    block_meta.compression(),
                    sort_min_max,
                    Some(block_meta_index.to_owned()),
                    create_on,
                )
            })
            .collect()
    }

    fn projection_partitions(
        &self,
        block_metas: &[(BlockMetaIndex, Arc<BlockMeta>)],
        projection: &Projection,
        column_nodes: &ColumnNodes,
    ) -> Vec<PartInfoPtr> {
        block_metas
            .iter()
            .map(|(block_meta_index, block_meta)| {
                let mut columns_meta = HashMap::with_capacity(projection.len());
                let mut columns_stat = HashMap::with_capacity(projection.len());

                let columns = projection.project_column_nodes(column_nodes).unwrap();
                for column in &columns {
                    for column_id in &column.leaf_column_ids {
                        // ignore column this block dose not exist
                        if let Some(column_meta) = block_meta.col_metas.get(column_id) {
                            columns_meta.insert(*column_id, column_meta.clone());
                        }
                        if let Some(column_stat) = block_meta.col_stats.get(column_id) {
                            columns_stat.insert(*column_id, column_stat.clone());
                        }
                    }
                }

                let rows_count = block_meta.row_count;
                let location = block_meta.location.0.clone();
                let create_on = block_meta.create_on;

                let sort_min_max = self.top_k.clone().map(|(top_k, default)| {
                    let stat = block_meta.col_stats.get(&top_k.field.column_id);
                    stat.map(|stat| (stat.min().clone(), stat.max().clone()))
                        .unwrap_or((default.clone(), default))
                });

                // TODO
                // row_count should be a hint value of  LIMIT,
                // not the count the rows in this partition
                FuseBlockPartInfo::create(
                    location,
                    rows_count,
                    columns_meta,
                    Some(columns_stat),
                    block_meta.compression(),
                    sort_min_max,
                    Some(block_meta_index.to_owned()),
                    create_on,
                )
            })
            .collect()
    }
}
