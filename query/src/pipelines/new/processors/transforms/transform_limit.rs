// // Copyright 2022 Datafuse Labs.
// //
// // Licensed under the Apache License, Version 2.0 (the "License");
// // you may not use this file except in compliance with the License.
// // You may obtain a copy of the License at
// //
// //     http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS,
// // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// // See the License for the specific language governing permissions and
// // limitations under the License.
//
// use std::sync::Arc;
//
// use common_datablocks::DataBlock;
// use common_exception::Result;
//
// use crate::pipelines::new::processors::port::InputPort;
// use crate::pipelines::new::processors::port::OutputPort;
// use crate::pipelines::new::processors::Processor;
// use crate::pipelines::new::processors::processor::{Event, ProcessorPtr};
// use crate::pipelines::new::processors::transforms::transform::Transform;
// use crate::pipelines::new::processors::transforms::transform::Transformer;
//
// pub struct TransformLimit {
//     limit: Option<usize>,
//     offset: usize,
// }
//
// impl TransformLimit {
//     pub fn create(input: Arc<InputPort>, output: Arc<OutputPort>, limit: Option<usize>, offset: usize) -> ProcessorPtr {
//         match (limit, offset) {
//             (None, 0) => unreachable!("It's a bug."),
//             (Some(_), 0) => OnlyLimitTransform::create(input, output, limit, offset),
//             (None, _) => OnlyOffsetTransform::create(input, output, limit, offset),
//             (Some(_), _) => OffsetAndLimitTransform::create(input, output, limit, offset),
//         }
//     }
// }
//
// const ONLY_LIMIT: usize = 0;
// const ONLY_OFFSET: usize = 1;
// const OFFSET_AND_LIMIT: usize = 2;
//
// type OnlyLimitTransform = TransformLimitImpl::<ONLY_LIMIT>;
// type OnlyOffsetTransform = TransformLimitImpl::<ONLY_OFFSET>;
// type OffsetAndLimitTransform = TransformLimitImpl::<OFFSET_AND_LIMIT>;
//
// struct TransformLimitImpl<const MODE: usize> {
//     limit: Option<usize>,
//     remaining: usize,
//
//     input: Arc<InputPort>,
//     output: Arc<OutputPort>,
// }
//
// impl<const MODE: usize> TransformLimitImpl<MODE> where Self: Processor {
//     pub fn create(input: Arc<InputPort>, output: Arc<OutputPort>, limit: Option<usize>, offset: usize) -> ProcessorPtr {
//         ProcessorPtr::create(Box::new(Self { limit, remaining: offset, input, output }))
//     }
// }
//
// #[async_trait::async_trait]
// impl Processor for TransformLimitImpl<ONLY_LIMIT> {
//     fn name(&self) -> &'static str {
//         "TakeTransform"
//     }
//
//     fn event(&mut self) -> Result<Event> {
//         todo!()
//     }
//
//     // fn transform(&mut self, block: DataBlock) -> Result<DataBlock> {
//     //     let rows = block.num_rows();
//     //     if self.remaining == 0 {
//     //         None
//     //     } else if self.remaining >= rows {
//     //         self.remaining -= rows;
//     //         Some(block.clone())
//     //     } else {
//     //         let remaining = self.remaining;
//     //         self.remaining = 0;
//     //         Some(block.slice(0, remaining))
//     //     }
//     // }
// }
//
// #[async_trait::async_trait]
// impl Processor for TransformLimitImpl<ONLY_OFFSET> {
//     fn name(&self) -> &'static str {
//         "SkipTransform"
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
//         match self.remaining {
//             0 => {
//                 self.output.push_data(self.input.pull_data().unwrap());
//                 Ok(Event::NeedConsume)
//             }
//         }
//
//         if self.input.is_finished() {
//             self.output.finish();
//             return Ok(Event::Finished);
//         }
//
//         if !self.input.has_data() {
//             self.input.set_need_data();
//             return Ok(Event::NeedData);
//         }
//
//
//         let rows = block.num_rows();
//         if self.remaining >= rows {
//             self.remaining -= rows;
//             Ok(DataBlock::empty())
//         } else if self.remaining < rows {
//             let remaining = self.remaining;
//             self.remaining = 0;
//             Ok(block.slice(remaining, rows - remaining))
//         }
//     }
//
//     // fn transform(&mut self, block: DataBlock) -> Result<DataBlock> {
//     //     let rows = block.num_rows();
//     //     if self.remaining >= rows {
//     //         self.remaining -= rows;
//     //         Ok(DataBlock::empty())
//     //     } else if self.remaining < rows {
//     //         let remaining = self.remaining;
//     //         self.remaining = 0;
//     //         Ok(block.slice(remaining, rows - remaining))
//     //     }
//     // }
// }
//
// #[async_trait::async_trait]
// impl Processor for TransformLimitImpl<OFFSET_AND_LIMIT> {
//     const NAME: &'static str = "LimitTransform";
//
//     fn event(&mut self) -> Result<Event> {
//         todo!()
//     }
//
//     fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
//         Ok(data)
//     }
// }
