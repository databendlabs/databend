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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::Weak;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_sql::plans::JoinType;

use crate::pipelines::memory_settings::MemorySettingsExt;
use crate::pipelines::processors::transforms::memory::outer_left_join::OuterLeftHashJoin;
use crate::pipelines::processors::transforms::new_hash_join::common::CStyleCell;
use crate::pipelines::processors::transforms::new_hash_join::grace::GraceHashJoinState;
use crate::pipelines::processors::transforms::new_hash_join::hybrid::HybridHashJoin;
use crate::pipelines::processors::transforms::new_hash_join::hybrid::HybridHashJoinState;
use crate::pipelines::processors::transforms::BasicHashJoinState;
use crate::pipelines::processors::transforms::GraceHashJoin;
use crate::pipelines::processors::transforms::InnerHashJoin;
use crate::pipelines::processors::transforms::Join;
use crate::pipelines::processors::HashJoinDesc;
use crate::sessions::QueryContext;

pub struct HashJoinFactory {
    mutex: Mutex<()>,
    join_type: JoinType,
    ctx: Arc<QueryContext>,
    desc: Arc<HashJoinDesc>,
    hash_method: HashMethodKind,
    function_ctx: FunctionContext,
    grace_state: CStyleCell<HashMap<usize, Weak<GraceHashJoinState>>>,
    basic_state: CStyleCell<HashMap<usize, Weak<BasicHashJoinState>>>,
    hybrid_state: CStyleCell<HashMap<usize, Weak<HybridHashJoinState>>>,
}

impl HashJoinFactory {
    pub fn create(
        join_type: JoinType,
        ctx: Arc<QueryContext>,
        function_ctx: FunctionContext,
        method: HashMethodKind,
        desc: Arc<HashJoinDesc>,
    ) -> Arc<HashJoinFactory> {
        Arc::new(HashJoinFactory {
            ctx,
            desc,
            join_type,
            function_ctx,
            hash_method: method,
            mutex: Mutex::new(()),
            grace_state: CStyleCell::new(HashMap::new()),
            basic_state: CStyleCell::new(HashMap::new()),
            hybrid_state: CStyleCell::new(HashMap::new()),
        })
    }

    pub fn create_hybrid_state(self: &Arc<Self>, id: usize) -> Result<Arc<HybridHashJoinState>> {
        let locked = self.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);

        match self.hybrid_state.as_mut().entry(id) {
            Entry::Occupied(v) => match v.get().upgrade() {
                Some(v) => Ok(v),
                None => Err(ErrorCode::Internal(format!(
                    "Error state: The level {} hybrid hash state has been destroyed.",
                    id
                ))),
            },
            Entry::Vacant(v) => {
                let hybrid_state = HybridHashJoinState::create(
                    self.ctx.clone(),
                    self.function_ctx.clone(),
                    self.hash_method.clone(),
                    self.desc.clone(),
                    id,
                    self.clone(),
                );

                v.insert(Arc::downgrade(&hybrid_state));
                Ok(hybrid_state)
            }
        }
    }

    pub fn create_grace_state(self: &Arc<Self>, id: usize) -> Result<Arc<GraceHashJoinState>> {
        let locked = self.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);

        let ctx = self.ctx.clone();
        match self.grace_state.as_mut().entry(id) {
            Entry::Occupied(v) => match v.get().upgrade() {
                Some(v) => Ok(v),
                None => Err(ErrorCode::Internal(format!(
                    "Error state: The level {} grace hash state has been destroyed.",
                    id
                ))),
            },
            Entry::Vacant(v) => {
                let grace_state = GraceHashJoinState::create(ctx, id, self.clone());
                v.insert(Arc::downgrade(&grace_state));
                Ok(grace_state)
            }
        }
    }

    pub fn create_basic_state(self: &Arc<Self>, id: usize) -> Result<Arc<BasicHashJoinState>> {
        let locked = self.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);

        match self.basic_state.as_mut().entry(id) {
            Entry::Occupied(v) => match v.get().upgrade() {
                Some(v) => Ok(v),
                None => Err(ErrorCode::Internal(format!(
                    "Error state: The level {} basic hash state has been destroyed.",
                    id
                ))),
            },
            Entry::Vacant(v) => {
                let basic_hash_state = Arc::new(BasicHashJoinState::create(id, self.clone()));
                v.insert(Arc::downgrade(&basic_hash_state));
                Ok(basic_hash_state)
            }
        }
    }

    pub fn remove_basic_state(&self, id: usize) {
        let locked = self.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);

        self.basic_state.as_mut().remove(&id);
    }

    pub fn remove_grace_state(&self, id: usize) {
        let locked = self.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);
        self.grace_state.as_mut().remove(&id);
    }

    pub fn remove_hybrid_state(&self, id: usize) {
        let locked = self.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);
        self.hybrid_state.as_mut().remove(&id);
    }

    pub fn create_hash_join(self: &Arc<Self>, id: usize) -> Result<Box<dyn Join>> {
        let settings = self.ctx.get_settings();

        if settings.get_force_join_data_spill()? {
            return self.create_grace_join(id);
        }

        let max_level = settings.get_max_grace_hash_join_level()?;
        let memory_settings = MemorySettings::from_join_settings(&self.ctx)?;

        match self.join_type {
            JoinType::Inner => {
                let inner_hash_join = InnerHashJoin::create(
                    &self.ctx,
                    self.function_ctx.clone(),
                    self.hash_method.clone(),
                    self.desc.clone(),
                    self.create_basic_state(id)?,
                )?;

                Ok(Box::new(HybridHashJoin::create(
                    inner_hash_join,
                    memory_settings.clone(),
                    self.create_hybrid_state(id)?,
                    max_level,
                )))
            }
            JoinType::Left => {
                let outer_left_hash_join = OuterLeftHashJoin::create(
                    &self.ctx,
                    self.function_ctx.clone(),
                    self.hash_method.clone(),
                    self.desc.clone(),
                    self.create_basic_state(id)?,
                )?;

                Ok(Box::new(HybridHashJoin::create(
                    outer_left_hash_join,
                    memory_settings.clone(),
                    self.create_hybrid_state(id)?,
                    max_level,
                )))
            }
            _ => unreachable!(),
        }
    }

    pub fn create_grace_join(self: &Arc<Self>, id: usize) -> Result<Box<dyn Join>> {
        match self.join_type {
            JoinType::Inner => {
                let inner_hash_join = InnerHashJoin::create(
                    &self.ctx,
                    self.function_ctx.clone(),
                    self.hash_method.clone(),
                    self.desc.clone(),
                    self.create_basic_state(id)?,
                )?;

                Ok(Box::new(GraceHashJoin::create(
                    self.ctx.clone(),
                    self.function_ctx.clone(),
                    self.hash_method.clone(),
                    self.desc.clone(),
                    self.create_grace_state(id)?,
                    inner_hash_join,
                    0,
                )?))
            }
            JoinType::Left => {
                let left_hash_join = OuterLeftHashJoin::create(
                    &self.ctx,
                    self.function_ctx.clone(),
                    self.hash_method.clone(),
                    self.desc.clone(),
                    self.create_basic_state(id)?,
                )?;

                Ok(Box::new(GraceHashJoin::create(
                    self.ctx.clone(),
                    self.function_ctx.clone(),
                    self.hash_method.clone(),
                    self.desc.clone(),
                    self.create_grace_state(id)?,
                    left_hash_join,
                    0,
                )?))
            }
            _ => unreachable!(),
        }
    }
}
