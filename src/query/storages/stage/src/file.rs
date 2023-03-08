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

use chrono::TimeZone;
use chrono::Utc;
use common_catalog::plan::StageFileInfo;
use common_catalog::plan::StageFileStatus;
use common_exception::Result;
use futures::TryStreamExt;
use opendal::Entry;
use opendal::Metadata;
use opendal::Metakey;
use opendal::Operator;
use tracing::warn;

/// List files from DAL in recursive way.
///
/// - If input path is a dir, we will list it recursively.
/// - Or, we will append the file itself, and try to list `path/`.
/// - If not exist, we will try to list `path/` too.
///
/// TODO(@xuanwo): return a stream instead.
pub async fn list_file(op: &Operator, path: &str) -> Result<Vec<StageFileInfo>> {
    let mut files = Vec::new();

    // - If the path itself is a dir, return directly.
    // - Otherwise, return a path suffix by `/`
    // - If other errors happen, we will ignore them by returning None.
    let dir_path = match op.stat(path).await {
        Ok(meta) if meta.mode().is_dir() => Some(path.to_string()),
        Ok(meta) if !meta.mode().is_dir() => {
            files.push(format_stage_file_info(path.to_string(), &meta));

            None
        }
        Err(e) if e.kind() == opendal::ErrorKind::NotFound => None,
        Err(e) => return Err(e.into()),
        _ => None,
    };

    // Check the if this dir valid and list it recursively.
    if let Some(dir) = dir_path {
        match op.stat(&dir).await {
            Ok(_) => {
                let mut ds = op.scan(&dir).await?;
                while let Some(de) = ds.try_next().await? {
                    if let Some(fi) = stat_file(op.clone(), de).await? {
                        files.push(fi)
                    }
                }
            }
            Err(e) => warn!("ignore listing {path}/, because: {:?}", e),
        };
    }

    Ok(files)
}

/// # Behavior
///
///
/// - `Ok(Some(v))` if given object is a file and no error happened.
/// - `Ok(None)` if given object is not a file.
/// - `Err(err)` if there is an error happened.
pub async fn stat_file(op: Operator, de: Entry) -> Result<Option<StageFileInfo>> {
    let meta = op
        .metadata(&de, {
            Metakey::Mode
                | Metakey::ContentLength
                | Metakey::ContentMd5
                | Metakey::LastModified
                | Metakey::Etag
        })
        .await?;

    if !meta.mode().is_file() {
        return Ok(None);
    }

    Ok(Some(format_stage_file_info(de.path().to_string(), &meta)))
}

pub fn format_stage_file_info(path: String, meta: &Metadata) -> StageFileInfo {
    StageFileInfo {
        path,
        size: meta.content_length(),
        md5: meta.content_md5().map(str::to_string),
        last_modified: meta
            .last_modified()
            .map_or(Utc::now(), |t| Utc.timestamp(t.unix_timestamp(), 0)),
        etag: meta.etag().map(str::to_string),
        status: StageFileStatus::NeedCopy,
        creator: None,
    }
}
