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

use common_catalog::table_context::TableContext;
use common_meta_app::principal::StageInfo;
use common_storage::StageFileInfo;
use common_storages_fuse::io::Files;
use common_storages_stage::StageTable;
use tracing::error;

use crate::sessions::QueryContext;

#[async_backtrace::framed]
pub async fn try_purge_files(
    ctx: Arc<QueryContext>,
    stage_info: &StageInfo,
    stage_files: &[StageFileInfo],
) {
    let table_ctx: Arc<dyn TableContext> = ctx.clone();
    let op = StageTable::get_op(stage_info);
    match op {
        Ok(op) => {
            let file_op = Files::create(table_ctx, op);
            let files = stage_files
                .iter()
                .map(|v| v.path.clone())
                .collect::<Vec<_>>();
            if let Err(e) = file_op.remove_file_in_batch(&files).await {
                error!("Failed to delete file: {:?}, error: {}", files, e);
            }
        }
        Err(e) => {
            error!("Failed to get stage table op, error: {}", e);
        }
    }
}
