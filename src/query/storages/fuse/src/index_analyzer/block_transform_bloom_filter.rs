// use std::any::Any;
// use std::fmt::{Debug, Formatter};
// use std::sync::Arc;
// use opendal::Operator;
// use serde::{Deserializer, Serializer};
// use common_exception::ErrorCode;
// use common_expression::{BlockMetaInfo, BlockMetaInfoDowncast, BlockMetaInfoPtr, Expr, TableField, TableSchemaRef};
// use common_expression::DataBlock;
// use common_pipeline_transforms::processors::transforms::{AccumulatingTransform, AsyncTransform, AsyncTransformer, BlockMetaAccumulatingTransform, Transform};
// use storages_common_index::filters::BlockFilter;
// use common_exception::Result;
// use common_pipeline_core::processors::port::{InputPort, OutputPort};
// use common_pipeline_core::processors::processor::ProcessorPtr;
// use storages_common_index::BloomIndex;
//
// use crate::index_analyzer::block_filter_meta::{BlockFilterMeta, BlocksFilterMeta};
// use crate::index_analyzer::bloom_filter_meta::BloomFilterMeta;
// use crate::io::BloomBlockFilterReader;
// use crate::pruning::SegmentLocation;
//
//
// pub struct BlockFilterReadTransform {
//     operator: Operator,
//     index_fields: Vec<TableField>,
// }
//
// impl BlockFilterReadTransform {
//     pub fn create(
//         input: Arc<InputPort>,
//         output: Arc<OutputPort>,
//         schema: &TableSchemaRef,
//         dal: Operator,
//         filter_expr: Option<&Expr<String>>, ) -> ProcessorPtr {
//         if let Some(expr) = filter_expr {
//             let point_query_cols = BloomIndex::find_eq_columns(expr)?;
//         }
//         AsyncTransformer::create(
//             input,
//             output,
//             BlockFilterReadTransform {
//                 operator: (),
//                 index_fields: vec![],
//             },
//         )
//     }
// }
//
// #[async_trait::async_trait]
// impl AsyncTransform for BlockFilterReadTransform {
//     const NAME: &'static str = "BlockFilterReadTransform";
//
//     async fn transform(&mut self, mut data: DataBlock) -> Result<DataBlock> {
//         let meta = data.take_meta().unwrap();
//         if let Some(mut meta) = BloomFilterMeta::downcast_from(meta) {
//             let block_meta = &meta.inner.block_meta;
//
//             meta.block_filter = match &block_meta.bloom_filter_index_location {
//                 None => None,
//                 Some(loc) => {
//                     // filter out columns that no longer exist in the indexed block
//                     let index_columns = self.index_fields.iter().try_fold(
//                         Vec::with_capacity(self.index_fields.len()),
//                         |mut acc, field| {
//                             if block_meta.col_metas.contains_key(&field.column_id()) {
//                                 acc.push(BloomIndex::build_filter_column_name(loc.1, field)?);
//                             }
//                             Ok::<_, ErrorCode>(acc)
//                         },
//                     )?;
//
//                     let operator = self.operator.clone();
//                     let size = block_meta.bloom_filter_index_size;
//
//                     match loc.read_block_filter(operator, &index_columns, size).await {
//                         Ok(filter) => Some(filter),
//                         Err(e) if e.code() == ErrorCode::DEPRECATED_INDEX_FORMAT => None,
//                         Err(e) => { return Err(e); }
//                     }
//                 }
//             };
//
//             return Ok(DataBlock::empty_with_meta(Box::new(meta)));
//         }
//
//         Err(ErrorCode::Internal("BlockFilterReadTransform only recv BloomFilterMeta"))
//     }
// }
//
// pub struct BloomFilterTransform {}
//
// impl BlockMetaAccumulatingTransform<BloomFilterMeta> for BloomFilterTransform {
//     const NAME: &'static str = "BloomFilterTransform";
//
//     fn transform(&mut self, data: BloomFilterMeta) -> Result<Option<DataBlock>> {
//         match data.block_filter {
//             None => Ok(Some(DataBlock::empty_with_meta(Box::new(data.inner)))),
//             Some(filter) => {
//                 // BloomIndex::from_filter_block(
//                 //
//                 // )
//                 // Ok(BloomIndex::from_filter_block(
//                 //     self.func_ctx.clone(),
//                 //     self.data_schema.clone(),
//                 //     filter.filter_schema,
//                 //     filter.filters,
//                 //     version,
//                 // )?
//                 //     .apply(self.filter_expression.clone(), &self.scalar_map)?
//                 //     != FilterEvalResult::MustFalse)
//             }
//         }
//     }
// }
