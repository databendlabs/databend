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

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_sql::plans::JoinType;

use super::encoded_build::EncodedBuild;
use super::grace::GraceMemoryJoin;
use super::hybrid::HybridHashJoin;
use super::hybrid::HybridHashJoinState;
use super::hybrid::HybridLevelStates;
use super::memory::NestedLoopJoin;
use crate::pipelines::memory_settings::MemorySettingsExt;
use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::BasicHashJoinState;
use crate::pipelines::processors::transforms::InnerHashJoin;
use crate::pipelines::processors::transforms::Join;
use crate::pipelines::processors::transforms::memory::AntiLeftHashJoin;
use crate::pipelines::processors::transforms::memory::AntiRightHashJoin;
use crate::pipelines::processors::transforms::memory::OuterRightHashJoin;
use crate::pipelines::processors::transforms::memory::SemiLeftHashJoin;
use crate::pipelines::processors::transforms::memory::SemiRightHashJoin;
use crate::pipelines::processors::transforms::memory::left_join::OuterLeftHashJoin;
use crate::sessions::QueryContext;
use crate::sessions::TableContextSettings;

pub struct HashJoinFactory {
    ctx: Arc<QueryContext>,
    desc: Arc<HashJoinDesc>,
    hash_method: HashMethodKind,
    function_ctx: FunctionContext,
    encoded_build: Arc<EncodedBuild>,
}

impl HashJoinFactory {
    pub fn create(
        ctx: Arc<QueryContext>,
        function_ctx: FunctionContext,
        method: HashMethodKind,
        desc: Arc<HashJoinDesc>,
    ) -> Arc<HashJoinFactory> {
        let encoded_build = Arc::new(EncodedBuild::create(
            desc.clone(),
            method.clone(),
            function_ctx.clone(),
        ));
        Arc::new(HashJoinFactory {
            ctx,
            desc,
            function_ctx,
            hash_method: method,
            encoded_build,
        })
    }

    /// The form grace hash join spills build rows in, shared by all levels.
    pub fn encoded_build(&self) -> Arc<EncodedBuild> {
        self.encoded_build.clone()
    }

    /// Create the states shared by all processors of one hybrid hash join level.
    pub fn create_level_states(self: &Arc<Self>, level: usize) -> Result<HybridLevelStates> {
        Ok(HybridLevelStates {
            // The levels below the top restore the build rows spilled in encoded form.
            basic: Arc::new(BasicHashJoinState::create(level > 0)),
            hybrid: HybridHashJoinState::create(self.ctx.clone(), level, self.clone())?,
        })
    }

    pub fn create_hash_join(
        self: &Arc<Self>,
        typ: JoinType,
        states: HybridLevelStates,
        shared_build_chunks: bool,
    ) -> Result<Box<dyn Join>> {
        let join = self.create_hybrid_join(typ, states)?;
        // A cache scan reads the build chunks from the shared state, so they must stay there.
        if shared_build_chunks {
            return Ok(Box::new(join));
        }

        Ok(Box::new(join.with_spill_in_final_build()))
    }

    /// Create a basic memory join (used internally by HybridHashJoin)
    pub fn create_memory_join(
        self: &Arc<Self>,
        typ: JoinType,
        basic_state: Arc<BasicHashJoinState>,
    ) -> Result<Box<dyn GraceMemoryJoin>> {
        match typ {
            JoinType::Inner => {
                let settings = self.ctx.get_settings();
                // The nested loop join reads build blocks of the build schema, not encoded ones.
                let nested_loop_desc = match basic_state.encoded_build {
                    true => None,
                    false => self
                        .desc
                        .create_nested_loop_desc(&settings, &self.function_ctx)?,
                };

                let inner = InnerHashJoin::create(
                    &settings,
                    self.function_ctx.clone(),
                    self.hash_method.clone(),
                    self.desc.clone(),
                    basic_state.clone(),
                    nested_loop_desc
                        .as_ref()
                        .map(|desc| desc.nested_loop_join_threshold)
                        .unwrap_or_default(),
                )?;

                match nested_loop_desc {
                    Some(desc) => Ok(Box::new(NestedLoopJoin::new(inner, basic_state, desc))),
                    None => Ok(Box::new(inner)),
                }
            }
            JoinType::Left => Ok(Box::new(OuterLeftHashJoin::create(
                &self.ctx,
                self.function_ctx.clone(),
                self.hash_method.clone(),
                self.desc.clone(),
                basic_state,
            )?)),
            JoinType::LeftAnti => Ok(Box::new(AntiLeftHashJoin::create(
                &self.ctx,
                self.function_ctx.clone(),
                self.hash_method.clone(),
                self.desc.clone(),
                basic_state,
            )?)),
            JoinType::LeftSemi => Ok(Box::new(SemiLeftHashJoin::create(
                &self.ctx,
                self.function_ctx.clone(),
                self.hash_method.clone(),
                self.desc.clone(),
                basic_state,
            )?)),
            JoinType::Right => Ok(Box::new(OuterRightHashJoin::create(
                &self.ctx,
                self.function_ctx.clone(),
                self.hash_method.clone(),
                self.desc.clone(),
                basic_state,
            )?)),
            JoinType::RightSemi => Ok(Box::new(SemiRightHashJoin::create(
                &self.ctx,
                self.function_ctx.clone(),
                self.hash_method.clone(),
                self.desc.clone(),
                basic_state,
            )?)),
            JoinType::RightAnti => Ok(Box::new(AntiRightHashJoin::create(
                &self.ctx,
                self.function_ctx.clone(),
                self.hash_method.clone(),
                self.desc.clone(),
                basic_state,
            )?)),
            _ => unreachable!(),
        }
    }

    /// Create a HybridHashJoin on the given level states
    pub fn create_hybrid_join(
        self: &Arc<Self>,
        typ: JoinType,
        states: HybridLevelStates,
    ) -> Result<HybridHashJoin> {
        let memory_join = self.create_memory_join(typ, states.basic.clone())?;
        let memory_settings = MemorySettings::from_join_settings(&self.ctx)?;

        Ok(HybridHashJoin::create(
            self.ctx.clone(),
            self.function_ctx.clone(),
            self.hash_method.clone(),
            self.desc.clone(),
            memory_settings,
            states.hybrid,
            states.basic,
            memory_join,
            typ,
        ))
    }
}
