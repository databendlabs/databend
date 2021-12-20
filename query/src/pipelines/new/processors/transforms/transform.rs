// use common_datablocks::DataBlock;
// use common_exception::Result;
//
// use crate::pipelines::new::processors::processor::PrepareState;
// use crate::pipelines::new::processors::Processor;
//
// // TODO: maybe we also need async transform for `SELECT sleep(1)`?
// pub trait Transform {
//     fn transform(&mut self, data: DataBlock) -> Result<DataBlock>;
// }
//
// pub struct TransformWrap<T: Transform> {
//     transform: T,
// }
//
// #[async_trait::async_trait]
// impl<T: Transform> Processor for TransformWrap<T> {
//     fn prepare(&mut self) -> Result<PrepareState> {
//         todo!()
//     }
// }
