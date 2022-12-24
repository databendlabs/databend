// use std::any::Any;
// use std::sync::Arc;
// use common_catalog::plan::PartInfoPtr;
// use common_catalog::table_context::TableContext;
// use common_datablocks::{BlockMetaInfoPtr, DataBlock};
// use common_pipeline_core::processors::Processor;
// use common_pipeline_core::processors::processor::Event;
// use crate::io::BlockReader;
// use common_exception::Result;
// use common_pipeline_core::processors::port::OutputPort;
// use common_pipeline_transforms::processors::transforms::Transform;
//
// pub struct FuseParquetSourceNew {}
//
// struct DataSourceMeta {
//     part: PartInfoPtr,
//     data: Option<Vec<(usize, Vec<u8>)>>,
// }
//
// impl DataSourceMeta {
//     pub fn create(data: Vec<(usize, Vec<u8>)>) -> BlockMetaInfoPtr {
//         Arc::new(Box::new(DataSourceMeta { data }))
//     }
// }
//
// struct ReadParquetDataSource {
//     finished: bool,
//     ctx: Arc<dyn TableContext>,
//     block_reader: Arc<BlockReader>,
//
//     output: Arc<OutputPort>,
//     output_data: Option<Vec<(usize, Vec<u8>)>>,
// }
//
// impl Processor for ReadParquetDataSource {
//     fn name(&self) -> String {
//         String::from("ReadParquetDataSource")
//     }
//
//     fn as_any(&mut self) -> &mut dyn Any {
//         self
//     }
//
//     fn event(&mut self) -> Result<Event> {
//         if self.finished {
//             self.output.finish();
//             return Ok(Event::Finished);
//         }
//
//         if self.output.is_finished() {
//             return Ok(Event::Finished);
//         }
//
//         if !self.output.can_push() {
//             return Ok(Event::NeedConsume);
//         }
//
//         if let Some(output_data) = self.output_data.take() {
//             let output = DataBlock::empty_with_meta(DataSourceMeta::create(output_data));
//             self.output.push_data(Ok(output));
//             return Ok(Event::NeedConsume);
//         }
//
//         Ok(Event::Async)
//     }
//
//     async fn async_process(&mut self) -> Result<()> {
//         if let Some(part) = self.ctx.try_get_part() {
//             self.output_data = Some(self.block_reader.read_columns_data(part).await?);
//             return Ok(());
//         }
//
//         self.finished = true;
//         Ok(())
//     }
// }
//
// struct DeserializeDataTransform {
//     block_reader: Arc<BlockReader>,
// }
//
// impl Transform for DeserializeDataTransform {
//     const NAME: &'static str = "";
//
//     fn transform(&mut self, mut data: DataBlock) -> Result<DataBlock> {
//         if let Some(source_meta) = data.take_meta() {
//             // if let Some(source_meta) = source_meta.as_any().downcast_ref::<DataSourceMeta>() {
//             //     self.block_reader.deserialize(
//             //         source_meta.part.clone(),
//             //         source_meta.data.take().unwrap(),
//             //     )
//             // }
//         }
//
//         unimplemented!()
//     }
// }
//
// // pub struct FuseParquetSource {
// //     state: State,
// //     ctx: Arc<dyn TableContext>,
// //     scan_progress: Arc<Progress>,
// //     output: Arc<OutputPort>,
// //     output_reader: Arc<BlockReader>,
// //
// //     prewhere_reader: Arc<BlockReader>,
// //     prewhere_filter: Arc<Option<EvalNode>>,
// //     remain_reader: Arc<Option<BlockReader>>,
// //
// //     support_blocking: bool,
// // }