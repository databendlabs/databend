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

use std::any::Any;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::PoisonError;

use databend_common_base::base::ProgressValues;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_transforms::MemorySettings;

use crate::pipelines::processors::transforms::new_hash_join::grace::GraceMemoryJoin;
use crate::pipelines::processors::transforms::new_hash_join::hybrid::hybrid_state::HybridHashJoinState;
use crate::pipelines::processors::transforms::new_hash_join::join::JoinStream;
use crate::pipelines::processors::transforms::GraceHashJoin;
use crate::pipelines::processors::transforms::Join;
use crate::pipelines::processors::transforms::JoinRuntimeFilterPacket;
use crate::pipelines::processors::transforms::RuntimeFiltersDesc;

#[derive(Default)]
enum HybridJoinVariant {
    #[default]
    Swaping,
    Memory(Box<dyn Join>),
    GraceHashJoin(Box<dyn Join>),
}

pub struct HybridHashJoin<T: GraceMemoryJoin> {
    variant: HybridJoinVariant,
    memory_setting: MemorySettings,
    state: Arc<HybridHashJoinState>,
    max_level: usize,
    _marker: std::marker::PhantomData<T>,
}

unsafe impl<T: GraceMemoryJoin + Send> Send for HybridHashJoin<T> {}

unsafe impl<T: GraceMemoryJoin + Sync> Sync for HybridHashJoin<T> {}

impl<T: GraceMemoryJoin> Join for HybridHashJoin<T> {
    fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        match &mut self.variant {
            HybridJoinVariant::Swaping => unreachable!(),
            HybridJoinVariant::Memory(memory) => {
                let finalized = data.is_none();

                memory.add_block(data)?;

                if self.state.level < self.max_level
                    && (self.memory_setting.check_spill()
                        || self.state.has_spilled.load(Ordering::Acquire))
                {
                    self.state.has_spilled.store(true, Ordering::Release);

                    memory.add_block(None)?;
                    self.transform_memory_data(finalized)?;
                }
            }
            HybridJoinVariant::GraceHashJoin(grace) => {
                while let Some((_, data_block)) = self.state.steal_transform_task() {
                    grace.add_block(Some(data_block))?;
                }

                grace.add_block(data)?;
            }
        };

        Ok(())
    }

    fn final_build(&mut self) -> Result<Option<ProgressValues>> {
        if self.state.level < self.max_level && self.state.has_spilled.load(Ordering::Acquire) {
            self.convert_to_grace_join()?
        }

        match &mut self.variant {
            HybridJoinVariant::Swaping => unreachable!(),
            HybridJoinVariant::Memory(v) => v.final_build(),
            HybridJoinVariant::GraceHashJoin(grace) => grace.final_build(),
        }
    }

    fn build_runtime_filter(&self, desc: &RuntimeFiltersDesc) -> Result<JoinRuntimeFilterPacket> {
        match &self.variant {
            HybridJoinVariant::Swaping => unreachable!(),
            HybridJoinVariant::Memory(v) => v.build_runtime_filter(desc),
            HybridJoinVariant::GraceHashJoin(grace) => grace.build_runtime_filter(desc),
        }
    }

    fn probe_block(&mut self, data: DataBlock) -> Result<Box<dyn JoinStream + '_>> {
        match &mut self.variant {
            HybridJoinVariant::Swaping => unreachable!(),
            HybridJoinVariant::Memory(v) => v.probe_block(data),
            HybridJoinVariant::GraceHashJoin(grace) => grace.probe_block(data),
        }
    }

    fn final_probe(&mut self) -> Result<Option<Box<dyn JoinStream + '_>>> {
        match &mut self.variant {
            HybridJoinVariant::Swaping => unreachable!(),
            HybridJoinVariant::Memory(v) => v.final_probe(),
            HybridJoinVariant::GraceHashJoin(grace) => grace.final_probe(),
        }
    }
}

impl<T: GraceMemoryJoin> GraceMemoryJoin for HybridHashJoin<T> {
    fn reset_memory(&mut self, reset_global: bool) {
        match std::mem::take(&mut self.variant) {
            HybridJoinVariant::Swaping => unreachable!(),
            HybridJoinVariant::Memory(memory) => {
                let any_box: Box<dyn Any> = memory;
                let mut downcast_type_join = any_box.downcast::<T>().expect("wrong memory type");

                downcast_type_join.reset_memory(reset_global);
                self.variant = HybridJoinVariant::Memory(downcast_type_join);
            }

            HybridJoinVariant::GraceHashJoin(grace) => {
                let any_box: Box<dyn Any> = grace;

                match any_box.downcast::<GraceHashJoin<HybridHashJoin<T>>>() {
                    Ok(hybrid_inner) => {
                        let mut memory_inner = hybrid_inner.into_inner().into_inner();
                        memory_inner.reset_memory(reset_global);
                        self.variant = HybridJoinVariant::Memory(memory_inner);
                    }
                    Err(any_box) => {
                        let inner_grace = any_box
                            .downcast::<GraceHashJoin<T>>()
                            .expect("downcast grace hash join error");

                        let mut memory_inner = Box::new(inner_grace.into_inner());
                        memory_inner.reset_memory(reset_global);
                        self.variant = HybridJoinVariant::Memory(memory_inner);
                    }
                }
            }
        }
    }

    fn take_memory_chunks(&self) -> Vec<DataBlock> {
        unreachable!()
    }
}

impl<T: GraceMemoryJoin> HybridHashJoin<T> {
    pub fn create(
        inner: T,
        memory_setting: MemorySettings,
        state: Arc<HybridHashJoinState>,
        max_level: usize,
    ) -> Self {
        HybridHashJoin {
            state,
            memory_setting,
            variant: HybridJoinVariant::Memory(Box::new(inner)),
            max_level,
            _marker: Default::default(),
        }
    }

    fn into_inner(mut self) -> Box<T> {
        match std::mem::take(&mut self.variant) {
            HybridJoinVariant::Swaping => unreachable!(),
            HybridJoinVariant::Memory(memory) => {
                let any_box: Box<dyn Any> = memory;
                any_box.downcast::<T>().expect("wrong memory type")
            }
            HybridJoinVariant::GraceHashJoin(grace) => {
                let any_box: Box<dyn Any> = grace;

                match any_box.downcast::<GraceHashJoin<HybridHashJoin<T>>>() {
                    Ok(hybrid_inner) => hybrid_inner.into_inner().into_inner(),
                    Err(any_box) => {
                        let inner_grace = any_box
                            .downcast::<GraceHashJoin<T>>()
                            .expect("downcast grace hash join error");
                        Box::new(inner_grace.into_inner())
                    }
                }
            }
        }
    }

    fn transform_memory_data(&mut self, finalized: bool) -> Result<()> {
        self.variant = match std::mem::take(&mut self.variant) {
            HybridJoinVariant::Swaping => HybridJoinVariant::Swaping,
            HybridJoinVariant::GraceHashJoin(v) => HybridJoinVariant::GraceHashJoin(v),
            HybridJoinVariant::Memory(memory_join) => {
                let any_box: Box<dyn Any> = memory_join;
                let memory_join = any_box.downcast::<T>().expect("wrong memory type");
                let take_memory_chunks = memory_join.take_memory_chunks();

                if !take_memory_chunks.is_empty() {
                    let self_locked = self.state.mutex.lock();
                    let _self_locked = self_locked.unwrap_or_else(PoisonError::into_inner);
                    self.state.spills_queue.as_mut().extend(take_memory_chunks);
                }

                let mut grace_hash_join = self.create_grace_hash_join()?;

                while let Some((_, data_block)) = self.state.steal_transform_task() {
                    if !data_block.is_empty() {
                        grace_hash_join.add_block(Some(data_block))?;
                    }
                }

                if finalized {
                    grace_hash_join.add_block(None)?;
                }

                HybridJoinVariant::GraceHashJoin(Box::new(grace_hash_join))
            }
        };

        Ok(())
    }

    fn convert_to_grace_join(&mut self) -> Result<()> {
        self.variant = match std::mem::take(&mut self.variant) {
            HybridJoinVariant::Swaping => HybridJoinVariant::Swaping,
            HybridJoinVariant::GraceHashJoin(grace) => HybridJoinVariant::GraceHashJoin(grace),
            HybridJoinVariant::Memory(_) => {
                HybridJoinVariant::GraceHashJoin(Box::new(self.create_grace_hash_join()?))
            }
        };
        Ok(())
    }

    fn create_grace_hash_join(&mut self) -> Result<GraceHashJoin<HybridHashJoin<T>>> {
        let child = self.state.factory.create_hash_join(self.state.level + 1)?;
        let any_child: Box<dyn Any> = child;

        let downcast_child = any_child
            .downcast::<HybridHashJoin<T>>()
            .expect("wrong memory type");

        let grace_state = self
            .state
            .factory
            .create_grace_state(self.state.level + 1)?;

        GraceHashJoin::create(
            self.state.ctx.clone(),
            self.state.function_ctx.clone(),
            self.state.hash_method_kind.clone(),
            self.state.desc.clone(),
            grace_state,
            Box::into_inner(downcast_child),
            self.state.level * 4,
        )
    }
}
