// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_raft::storage::HardState;
use async_raft::RaftStorage;
use common_runtime::tokio;
use common_tracing::tracing;

use crate::meta_service::MetaStore;
use crate::tests::service::new_test_context;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_store_restart() -> anyhow::Result<()> {
    // - Create a MetaStore
    // - Update MetaStore
    // - Close and reopen it
    // - Test state is restored

    // TODO check log
    // TODO check state machine

    let id = 3;
    let tc = new_test_context();

    tracing::info!("--- new MetaStore");
    {
        let ms = MetaStore::new(id, &tc.config).await?;
        assert_eq!(id, ms.id);
        assert_eq!(None, ms.read_hard_state().await?);

        tracing::info!("--- update MetaStore");

        ms.save_hard_state(&HardState {
            current_term: 10,
            voted_for: Some(5),
        })
        .await?;
    }

    tracing::info!("--- reopen MetaStore");
    {
        let ms = MetaStore::open(&tc.config).await?;
        assert_eq!(id, ms.id);
        assert_eq!(
            Some(HardState {
                current_term: 10,
                voted_for: Some(5),
            }),
            ms.read_hard_state().await?
        );
    }

    Ok(())
}
