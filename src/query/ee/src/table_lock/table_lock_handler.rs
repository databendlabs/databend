// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::time::Duration;

use common_base::base::tokio::time::timeout;
use common_base::base::GlobalInstance;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::protobuf::watch_request::FilterType;
use common_meta_types::protobuf::WatchRequest;
use common_pipeline_core::TableLock;
use common_storages_fuse::TableContext;
use common_users::UserApiProvider;
use futures_util::StreamExt;
use table_lock::TableLockHandler;
use table_lock::TableLockHeartbeat;
use table_lock::TableLockManager;

pub struct RealTableLockHandler {}

#[async_trait::async_trait]
impl TableLockHandler for RealTableLockHandler {
    #[async_backtrace::framed]
    async fn try_lock(
        &self,
        ctx: Arc<dyn TableContext>,
        lock: &mut dyn TableLock,
    ) -> Result<TableLockHeartbeat> {
        let expire_secs = ctx.get_settings().get_table_lock_expire_secs()?;
        let catalog = ctx.get_catalog(&ctx.get_current_catalog()).await?;

        // get a new table lock revision.
        let res = catalog
            .create_table_lock_rev(lock.create_table_lock_req(expire_secs))
            .await?;
        let revision = res.revision;
        lock.set_revision(revision);

        let duration = Duration::from_secs(expire_secs);
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let list_table_lock_req = lock.list_table_lock_req();
        let delete_table_lock_req = lock.delete_table_lock_req();
        loop {
            // List all revisions and check if the current is the minimum.
            let reply = catalog
                .list_table_lock_revs(list_table_lock_req.clone())
                .await?;
            let position = reply.iter().position(|x| *x == revision).ok_or(
                // If the current is not found in list,  it means that the current has expired.
                ErrorCode::TableLockExpired("the acquired table lock has expired".to_string()),
            )?;

            if position == 0 {
                // The lock is acquired by current session.
                break;
            }

            // Get the previous revision, watch the delete event.
            let req = WatchRequest {
                key: lock.watch_key(reply[position - 1]),
                key_end: None,
                filter_type: FilterType::Delete.into(),
            };
            let mut watch_stream = meta_api.watch(req).await?;
            // Add a timeout period for watch.
            match timeout(duration, async move {
                while let Some(Ok(resp)) = watch_stream.next().await {
                    if let Some(event) = resp.event {
                        if event.current.is_none() {
                            break;
                        }
                    }
                }
            })
            .await
            {
                Ok(_) => Ok(()),
                Err(_) => {
                    catalog
                        .delete_table_lock_rev(delete_table_lock_req.clone())
                        .await?;
                    Err(ErrorCode::TableAlreadyLocked(
                        "table is locked by other session, please retry later".to_string(),
                    ))
                }
            }?;
        }

        let mut heartbeat = TableLockHeartbeat::default();
        heartbeat.start(ctx, lock).await?;
        Ok(heartbeat)
    }
}

impl RealTableLockHandler {
    pub fn init() -> Result<()> {
        let handler = RealTableLockHandler {};
        let manager = TableLockManager::create(Box::new(handler));
        GlobalInstance::set(Arc::new(manager));
        Ok(())
    }
}
