// Copyright 2021 Datafuse Labs.
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

use common_meta_sled_store::openraft;
use common_meta_sled_store::sled;
use common_meta_sled_store::AsKeySpace;
use common_meta_sled_store::SledTree;
use common_meta_types::MetaError;
use common_meta_types::MetaResult;
use common_meta_types::MetaStorageResult;
use common_meta_types::NodeId;
use common_tracing::tracing;
use openraft::storage::HardState;

use crate::config::RaftConfig;
use crate::sled_key_spaces::RaftStateKV;
use crate::state::RaftStateKey;
use crate::state::RaftStateValue;

/// Raft state stores everything else other than log and state machine, which includes:
/// id: NodeId,
/// hard_state:
///      current_term,
///      voted_for,
///
#[derive(Debug)]
pub struct RaftState {
    pub id: NodeId,

    /// If the instance is opened(true) from an existent state(e.g. load from disk) or created(false).
    is_open: bool,

    /// A sled tree with key space support.
    pub inner: SledTree,
}

const TREE_RAFT_STATE: &str = "raft_state";

impl RaftState {
    pub fn is_open(&self) -> bool {
        self.is_open
    }
}

impl RaftState {
    /// Open/create a raft state in a sled db.
    /// 1. If `open` is `Some`,  it tries to open an existent RaftState if there is one.
    /// 2. If `create` is `Some`, it tries to initialize a new RaftState if there is not one.
    /// If none of them is `Some`, it is a programming error and will panic.
    #[tracing::instrument(level = "info", skip(db,config,open,create), fields(config_id=%config.config_id))]
    pub async fn open_create(
        db: &sled::Db,
        config: &RaftConfig,
        open: Option<()>,
        create: Option<()>,
    ) -> MetaResult<RaftState> {
        tracing::info!(?config);
        tracing::info!("open: {:?}, create: {:?}", open, create);

        let tree_name = config.tree_name(TREE_RAFT_STATE);
        let inner = SledTree::open(db, &tree_name, config.is_sync())?;

        let state = inner.key_space::<RaftStateKV>();
        let curr_id = state.get(&RaftStateKey::Id)?.map(NodeId::from);

        tracing::debug!("get curr_id: {:?}", curr_id);

        let (id, is_open) = if let Some(curr_id) = curr_id {
            match (open, create) {
                (Some(_), _) => (curr_id, true),
                (None, Some(_)) => {
                    return Err(MetaError::MetaStoreAlreadyExists(curr_id));
                }
                (None, None) => panic!("no open no create"),
            }
        } else {
            match (open, create) {
                (Some(_), Some(_)) => (config.id, false),
                (Some(_), None) => {
                    return Err(MetaError::MetaStoreNotFound);
                }
                (None, Some(_)) => (config.id, false),
                (None, None) => panic!("no open no create"),
            }
        };

        let rs = RaftState { id, is_open, inner };

        if !rs.is_open() {
            rs.init().await?;
        }

        Ok(rs)
    }

    /// Initialize a raft state. The only thing to do is to persist the node id
    /// so that next time opening it the caller knows it is initialized.
    #[tracing::instrument(level = "info", skip(self))]
    async fn init(&self) -> MetaResult<()> {
        let state = self.state();
        state
            .insert(&RaftStateKey::Id, &RaftStateValue::NodeId(self.id))
            .await?;
        Ok(())
    }

    pub async fn write_hard_state(&self, hs: &HardState) -> MetaStorageResult<()> {
        let state = self.state();
        state
            .insert(
                &RaftStateKey::HardState,
                &RaftStateValue::HardState(hs.clone()),
            )
            .await?;
        Ok(())
    }

    pub fn read_hard_state(&self) -> MetaStorageResult<Option<HardState>> {
        let state = self.state();
        let hs = state.get(&RaftStateKey::HardState)?;
        let hs = hs.map(HardState::from);
        Ok(hs)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn write_state_machine_id(&self, id: &(u64, u64)) -> MetaStorageResult<()> {
        let state = self.state();
        state
            .insert(
                &RaftStateKey::StateMachineId,
                &RaftStateValue::StateMachineId(*id),
            )
            .await?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub fn read_state_machine_id(&self) -> MetaStorageResult<(u64, u64)> {
        let state = self.state();
        let smid = state.get(&RaftStateKey::StateMachineId)?;
        let smid: (u64, u64) = smid.map_or((0, 0), |v| v.into());
        Ok(smid)
    }

    /// Returns a borrowed sled tree key space to store meta of raft log
    pub fn state(&self) -> AsKeySpace<RaftStateKV> {
        self.inner.key_space()
    }
}
