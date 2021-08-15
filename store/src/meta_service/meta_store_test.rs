// Copyright 2020 Datafuse Labs.
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

use async_raft::storage::HardState;
use async_raft::RaftStorage;
use common_runtime::tokio;
use common_tracing::tracing;

use crate::meta_service::MetaStore;
use crate::tests::service::init_store_unittest;
use crate::tests::service::new_test_context;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_store_restart() -> anyhow::Result<()> {
    // - Create a MetaStore
    // - Update MetaStore
    // - Close and reopen it
    // - Test state is restored

    // TODO check log
    // TODO check state machine

    init_store_unittest();

    let id = 3;
    let mut tc = new_test_context();
    tc.config.id = id;

    tracing::info!("--- new MetaStore");
    {
        let ms = MetaStore::open_create(&tc.config, None, Some(())).await?;
        assert_eq!(id, ms.id);
        assert!(!ms.is_open());
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
        let ms = MetaStore::open_create(&tc.config, Some(()), None).await?;
        assert_eq!(id, ms.id);
        assert!(ms.is_open());
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
