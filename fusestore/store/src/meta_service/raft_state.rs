// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_raft::storage::HardState;
use common_exception::ErrorCode;
use common_exception::ToErrorCode;
use common_tracing::tracing;

use crate::configs;
use crate::meta_service::sled_serde::SledOrderedSerde;
use crate::meta_service::NodeId;
use crate::meta_service::SledSerde;

/// Raft state stores everything else other than log and state machine, which includes:
/// id: NodeId,
/// hard_state:
///      current_term,
///      voted_for,
///
#[derive(Debug, Clone)]
pub struct RaftState {
    pub id: NodeId,

    /// If the instance is opened(true) from an existent state(e.g. load from disk) or created(false).
    is_open: bool,

    /// A unique prefix for opening multiple RaftState in a same sled::Db
    // tree_prefix: String,
    tree: sled::Tree,
}

const K_RAFT_STATE: &str = "raft_state";
const K_ID: &str = "id";
const K_HARD_STATE: &str = "hard_state";

impl SledSerde for HardState {}

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
    #[tracing::instrument(level = "info", skip(db))]
    pub async fn open_create(
        db: &sled::Db,
        config: &configs::Config,
        open: Option<()>,
        create: Option<()>,
    ) -> common_exception::Result<RaftState> {
        let tree_name = config.tree_name(K_RAFT_STATE);
        let t = db
            .open_tree(&tree_name)
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || {
                format!("open tree raft_state: name={}", "")
            })?;

        tracing::debug!("opened tree: {}", tree_name);

        let curr_id = t
            .get(K_ID)
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || "get id")?;

        tracing::debug!("get curr_id: {:?}", curr_id);

        let curr_id = match curr_id {
            Some(id) => Some(NodeId::de(id)?),
            None => None,
        };

        let (id, is_open) = if let Some(curr_id) = curr_id {
            match (open, create) {
                (Some(_), _) => (curr_id, true),
                (None, Some(_)) => {
                    return Err(ErrorCode::MetaStoreAlreadyExists(format!(
                        "raft state present id={}, can not create",
                        curr_id
                    )));
                }
                (None, None) => panic!("no open no create"),
            }
        } else {
            match (open, create) {
                (Some(_), Some(_)) => (config.id, false),
                (Some(_), None) => {
                    return Err(ErrorCode::MetaStoreNotFound(
                        "raft state absent, can not open",
                    ));
                }
                (None, Some(_)) => (config.id, false),
                (None, None) => panic!("no open no create"),
            }
        };

        let rs = RaftState {
            id,
            is_open,
            tree: t,
        };

        if !rs.is_open() {
            rs.init().await?;
        }

        Ok(rs)
    }

    /// Initialize a raft state. The only thing to do is to persist the node id
    /// so that next time opening it the caller knows it is initialized.
    #[tracing::instrument(level = "info", skip(self))]
    async fn init(&self) -> common_exception::Result<()> {
        let tree = &self.tree;

        let id_ivec = self.id.ser()?;

        tree.insert(K_ID, id_ivec)
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || "write id")?;

        tracing::info!("inserted id: {}", K_ID);

        tree.flush_async()
            .await
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || "flush raft state creation")?;

        tracing::info!("flushed RaftState");

        Ok(())
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
