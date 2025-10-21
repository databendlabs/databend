// // Copyright 2021 Datafuse Labs
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
// use databend_common_base::base::ProgressValues;
// use databend_common_exception::Result;
// use databend_common_expression::DataBlock;
// use databend_common_pipeline_transforms::MemorySettings;
//
// use crate::pipelines::processors::transforms::new_hash_join::grace::GraceMemoryJoin;
// use crate::pipelines::processors::transforms::new_hash_join::join::JoinStream;
// use crate::pipelines::processors::transforms::new_hash_join::hash_join_factory::HashJoinFactory;
// use crate::pipelines::processors::transforms::Join;
//
// struct MemoryHashJoin {
//     inner: Box<dyn Join>,
//     memory_setting: MemorySettings,
// }
//
// pub struct HybridHashJoin<T: GraceMemoryJoin> {
//     inner: Box<dyn Join>,
//     memory_settings: MemorySettings,
//
//     is_memory: bool,
//     state_factory: Arc<HashJoinFactory>,
// }
//
// impl<T: GraceMemoryJoin> Join for HybridHashJoin<T> {
//     fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
//         self.inner.add_block(data)?;
//
//         // if self.is_memory
//         if let HybridHashJoin::Memory(memory) = self {
//             if memory.memory_setting.check_spill() {
//                 // memory.inner.reset_memory()?;
//             }
//         }
//
//         Ok(())
//     }
//
//     fn final_build(&mut self) -> Result<Option<ProgressValues>> {
//         match self {
//             HybridHashJoin::Memory(memory) => memory.inner.final_build(),
//             HybridHashJoin::GraceHashJoin(grace_hash_join) => grace_hash_join.final_build(),
//         }
//     }
//
//     fn probe_block(&mut self, data: DataBlock) -> Result<Box<dyn JoinStream + '_>> {
//         match self {
//             HybridHashJoin::Memory(memory) => memory.inner.probe_block(data),
//             HybridHashJoin::GraceHashJoin(grace_hash_join) => grace_hash_join.probe_block(data),
//         }
//     }
//
//     fn final_probe(&mut self) -> Result<Option<Box<dyn JoinStream + '_>>> {
//         match self {
//             HybridHashJoin::Memory(memory) => memory.inner.final_probe(),
//             HybridHashJoin::GraceHashJoin(grace_hash_join) => grace_hash_join.final_probe(),
//         }
//     }
// }
