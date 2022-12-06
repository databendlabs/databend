// Copyright 2022 Datafuse Labs.
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

use chrono::TimeZone;
use chrono::Utc;
use common_catalog::plan::StageFileInfo;
use common_catalog::plan::StageFileStatus;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_meta_types::UserStageInfo;
use futures::TryStreamExt;
use opendal::Object;
use opendal::Operator;
use tracing::warn;

use crate::StageTable;

pub async fn stat_file(
    table_ctx: Arc<dyn TableContext>,
    path: &str,
    stage: &UserStageInfo,
) -> Result<StageFileInfo> {
    let op = StageTable::get_op(&table_ctx, stage)?;
    let meta = op.object(path).metadata().await?;

    Ok(StageFileInfo {
        path: path.to_string(),
        size: meta.content_length(),
        md5: meta.content_md5().map(str::to_string),
        last_modified: meta
            .last_modified()
            .map_or(Utc::now(), |t| Utc.timestamp(t.unix_timestamp(), 0)),
        etag: meta.etag().map(str::to_string),
        status: StageFileStatus::NeedCopy,
        creator: None,
    })
}

/// List files from DAL in recursive way.
///
/// - If input path is a dir, we will list it recursively.
/// - Or, we will append the file itself, and try to list `path/`.
/// - If not exist, we will try to list `path/` too.
///
/// TODO(@xuanwo): return a stream instead.
pub async fn list_file(
    table_ctx: Arc<dyn TableContext>,
    path: &str,
    stage: &UserStageInfo,
) -> Result<Vec<StageFileInfo>> {
    let op = StageTable::get_op(&table_ctx, stage)?;
    let mut files = Vec::new();

    // - If the path itself is a dir, return directly.
    // - Otherwise, return a path suffix by `/`
    // - If other errors happen, we will ignore them by returning None.
    let dir_path = match op.object(path).metadata().await {
        Ok(meta) if meta.mode().is_dir() => Some(path.to_string()),
        Ok(meta) if !meta.mode().is_dir() => {
            files.push((path.to_string(), meta));

            None
        }
        Err(e) if e.kind() == opendal::ErrorKind::ObjectNotFound => None,
        Err(e) => return Err(e.into()),
        _ => None,
    };

    // Check the if this dir valid and list it recursively.
    if let Some(dir) = dir_path {
        match op.object(&dir).metadata().await {
            Ok(_) => {
                let mut ds = op.batch().walk_top_down(&dir)?;
                while let Some(de) = ds.try_next().await? {
                    if de.mode().await?.is_file() {
                        let path = de.path().to_string();
                        let meta = de.metadata().await?;
                        files.push((path, meta));
                    }
                }
            }
            Err(e) => warn!("ignore listing {path}/, because: {:?}", e),
        };
    }

    let results = files
        .into_iter()
        .map(|(name, meta)| StageFileInfo {
            path: name,
            size: meta.content_length(),
            md5: meta.content_md5().map(str::to_string),
            last_modified: meta
                .last_modified()
                .map_or(Utc::now(), |t| Utc.timestamp(t.unix_timestamp(), 0)),
            etag: meta.etag().map(str::to_string),
            status: StageFileStatus::NeedCopy,
            creator: None,
        })
        .collect::<Vec<StageFileInfo>>();

    Ok(results)
}

pub async fn get_first_file(op: &Operator, path: &str) -> Result<Option<Object>> {
    let root_object = op.object(path);
    let meta = match root_object.metadata().await {
        Ok(meta) => meta,
        Err(e) if e.kind() == opendal::ErrorKind::ObjectNotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    if !meta.mode().is_dir() {
        Ok(Some(root_object))
    } else {
        let mut lister = root_object.list().await?;
        Ok(lister.try_next().await?)
    }
}
