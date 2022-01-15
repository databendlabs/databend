// use std::sync::Arc;
// use common_datablocks::DataBlock;
// use common_exception::Result;
// use crate::pipelines::new::processors::port::{InputPort, OutputPort};
// use crate::pipelines::new::processors::Processor;
// use crate::pipelines::new::processors::processor::Event;
//
// // TODO: maybe we also need async transform for `SELECT sleep(1)`?
// pub trait Transform {
//     fn transform(&mut self, data: DataBlock) -> Result<DataBlock>;
// }
//
// pub struct TransformWrap<T: Transform> {
//     transform: T,
//     input: Arc<InputPort>,
//     output: Arc<OutputPort>,
//
//     input_data: Option<DataBlock>,
//     output_data: Option<DataBlock>,
// }
//
// #[async_trait::async_trait]
// impl<T: Transform> Processor for TransformWrap<T> {
//     fn event(&mut self) -> Result<Event> {
//         if self.output.is_finished() {
//             self.input.finish();
//             return Ok(Event::Finished);
//         }
//
//         if !self.output.can_push() {
//             // TODO: set not need for backpressure
//             return Ok(Event::NeedConsume);
//         }
//
//         if let Some(data) = self.output_data.take() {
//             self.output.push_data(Ok(data));
//             return Ok(Event::NeedConsume);
//         }
//
//         Ok(Event::Sync)
//     }
// }
