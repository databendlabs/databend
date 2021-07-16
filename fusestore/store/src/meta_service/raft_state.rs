// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_raft::storage::HardState;
use common_exception::ErrorCode;
use common_exception::ToErrorCode;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sled::IVec;

use crate::meta_service::NodeId;

/// Raft state stores everything else other than log and state machine, which includes:
/// id: NodeId,
/// hard_state:
///      current_term,
///      voted_for,
///
#[derive(Debug, Clone)]
pub struct RaftState {
    pub id: NodeId,
    tree: sled::Tree,
}

const K_RAFT_STATE: &str = "raft_state";
const K_ID: &str = "id";
const K_HARD_STATE: &str = "hard_state";

/// Serialize/deserialize(ser/de) for RaftState.
trait RaftStateSerde {
    fn ser(&self) -> Result<IVec, ErrorCode>;
    fn de<T: AsRef<[u8]>>(v: T) -> Result<Self, ErrorCode>
    where Self: Sized;
}

impl<SD: Serialize + DeserializeOwned + Sized> RaftStateSerde for SD {
    fn ser(&self) -> Result<IVec, ErrorCode> {
        let x = serde_json::to_vec(self)?;
        Ok(x.into())
    }

    fn de<T: AsRef<[u8]>>(v: T) -> Result<Self, ErrorCode> {
        let s = serde_json::from_slice(v.as_ref())?;
        Ok(s)
    }
}

impl RaftState {
    /// Create a new sled db backed RaftState.
    /// Initialize with id and empty hard state.
    pub async fn create(db: &sled::Db, node_id: &NodeId) -> common_exception::Result<RaftState> {
        let t = db
            .open_tree(K_RAFT_STATE)
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || "open tree raft_state")?;

        let prev_id = t
            .get(K_ID)
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || "get id")?;

        if prev_id.is_some() {
            return Err(ErrorCode::MetaStoreAlreadyExists(format!(
                "exist: id={:?}",
                NodeId::de(&prev_id.unwrap())
            )));
        }

        let rs = RaftState {
            id: *node_id,
            tree: t,
        };

        let id_ivec = node_id.ser()?;
        rs.tree
            .insert(K_ID, id_ivec)
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || "write id")?;

        rs.tree
            .flush_async()
            .await
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || "flush raft state creation")?;

        Ok(rs)
    }

    /// Open an existent raft state in a sled db.
    /// If the node id is not found, it is treated as an error opening nonexistent raft state.
    pub fn open(db: &sled::Db) -> common_exception::Result<RaftState> {
        let t = db
            .open_tree("raft_state")
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || "open tree raft_state")?;

        let id = t
            .get(K_ID)
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || "read id")?;

        let id = match id {
            Some(id) => id,
            None => {
                return Err(ErrorCode::MetaStoreDamaged("id not found"));
            }
        };

        let rs = RaftState {
            id: NodeId::de(id)?,
            tree: t,
        };
        Ok(rs)
    }

    pub async fn write_hard_state(&self, hs: &HardState) -> common_exception::Result<()> {
        self.tree
            .insert(K_HARD_STATE, hs.ser()?)
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || "write hard_state")?;

        self.tree
            .flush_async()
            .await
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || "flush hard_state")?;
        Ok(())
    }

    pub async fn read_hard_state(&self) -> common_exception::Result<Option<HardState>> {
        let hs = self
            .tree
            .get(K_HARD_STATE)
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || "read hard_state")?;

        let hs = match hs {
            Some(hs) => hs,
            None => {
                return Ok(None);
            }
        };

        let hs = HardState::de(hs)?;
        Ok(Some(hs))
    }
}
