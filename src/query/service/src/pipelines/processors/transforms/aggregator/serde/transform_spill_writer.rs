// use std::any::Any;
// use std::sync::Arc;
// use common_exception::Result;
// use common_expression::BlockMetaInfoDowncast;
// use common_pipeline_core::processors::port::{InputPort, OutputPort};
// use common_pipeline_core::processors::Processor;
// use common_pipeline_core::processors::processor::Event;
// use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
// use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
//
// pub struct TransformGroupBySpillWriter<Method: HashMethodBounds> {
//     input: Arc<InputPort>,
//     output: Arc<OutputPort>,
// }
//
// #[async_trait::async_trait]
// impl<Method: HashMethodBounds> Processor for TransformGroupBySpillWriter<Method> {
//     fn name(&self) -> String {
//         String::from("TransformSpillWriter")
//     }
//
//     fn as_any(&mut self) -> &mut dyn Any {
//         self
//     }
//
//     fn event(&mut self) -> Result<Event> {
//         if self.output.is_finished() {
//             self.input.finish();
//             return Ok(Event::Finished);
//         }
//
//         if !self.output.can_push() {
//             self.input.set_not_need_data();
//             return Ok(Event::NeedConsume);
//         }
//
//         if self.input.has_data() {
//             let data_block = self.input.pull_data().unwrap()?;
//
//             if let Some(block_meta) = data_block.get_meta().and_then(AggregateMeta::<Method, ()>::downcast_ref_from) {
//                 if matches!(block_meta, AggregateMeta::HashTable())
//             }
//
//             self.output.push_data(Ok(data_block));
//             return Ok(Event::NeedConsume);
//         }
//
//         if self.input.is_finished() {
//             self.output.finish();
//             return Ok(Event::Finished);
//         }
//
//         self.input.set_need_data();
//         Ok(Event::NeedData)
//     }
// }
