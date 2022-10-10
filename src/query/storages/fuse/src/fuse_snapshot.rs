//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use async_channel::Receiver;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_fuse_meta::meta::TableSnapshot;
use futures_util::TryStreamExt;

use crate::io::MetaReaders;
use crate::io::SnapshotHistoryReader;
use crate::io::TableMetaLocationGenerator;

#[tracing::instrument(level = "debug", skip_all)]
pub async fn read_snapshots(
    ctx: Arc<dyn TableContext>,
    location: String,
    format_version: u64,
    location_gen: TableMetaLocationGenerator,
) -> Result<Receiver<Arc<TableSnapshot>>> {
    let (tx, rx) = async_channel::bounded(100);
    let reader = MetaReaders::table_snapshot_reader(ctx);
    let mut snapshot_history = reader.snapshot_history(location, format_version, location_gen);

    common_base::base::tokio::spawn(async move {
        while let Ok(Some(s)) = snapshot_history.try_next().await {
            if let Err(_cause) = tx.send(s).await {
                break;
            }
        }
    });

    Ok(rx)
}
