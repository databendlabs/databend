// Copyright 2023 Datafuse Labs.
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

use std::path::Path;

use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::principal::StageInfo;
use common_meta_app::principal::StageType;
use common_meta_app::principal::UserIdentity;
use futures::TryStreamExt;
use opendal::Entry;
use opendal::EntryMode;
use opendal::Metadata;
use opendal::Metakey;
use opendal::Operator;
use regex::Regex;

use crate::init_operator;
use crate::DataOperator;

pub struct FileWithMeta {
    pub path: String,
    pub metadata: Metadata,
}

impl FileWithMeta {
    fn new(path: &str, meta: Metadata) -> Self {
        Self {
            path: path.to_string(),
            metadata: meta,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum StageFileStatus {
    NeedCopy,
    AlreadyCopied,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct StageFileInfo {
    pub path: String,
    pub size: u64,
    pub md5: Option<String>,
    pub last_modified: DateTime<Utc>,
    pub etag: Option<String>,
    pub status: StageFileStatus,
    pub creator: Option<UserIdentity>,
}

impl StageFileInfo {
    pub fn new(path: String, meta: &Metadata) -> StageFileInfo {
        StageFileInfo {
            path,
            size: meta.content_length(),
            md5: meta.content_md5().map(str::to_string),
            last_modified: meta
                .last_modified()
                .map(|v| Utc.timestamp_nanos(v.unix_timestamp_nanos() as i64))
                .unwrap_or_default(),
            etag: meta.etag().map(str::to_string),
            status: StageFileStatus::NeedCopy,
            creator: None,
        }
    }
}

impl From<FileWithMeta> for StageFileInfo {
    fn from(value: FileWithMeta) -> Self {
        StageFileInfo::new(value.path, &value.metadata)
    }
}

pub fn init_stage_operator(stage_info: &StageInfo) -> Result<Operator> {
    if stage_info.stage_type == StageType::External {
        Ok(init_operator(&stage_info.stage_params.storage)?)
    } else {
        let stage_prefix = stage_info.stage_prefix();
        let param = DataOperator::instance()
            .params()
            .map_root(|path| format!("{path}/{stage_prefix}"));

        Ok(init_operator(&param)?)
    }
}
/// select * from @s1/<path> (FILES => <files> PATTERN => <pattern>)
/// copy from @s1/<path> FILES = <files> PATTERN => <pattern>
#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct StageFilesInfo {
    pub path: String,
    pub files: Option<Vec<String>>,
    pub pattern: Option<String>,
}

impl StageFilesInfo {
    fn get_pattern(&self) -> Result<Option<Regex>> {
        match &self.pattern {
            Some(pattern) => match Regex::new(pattern) {
                Ok(r) => Ok(Some(r)),
                Err(e) => Err(ErrorCode::SyntaxException(format!(
                    "Pattern format invalid, got:{}, error:{:?}",
                    pattern, e
                ))),
            },
            None => Ok(None),
        }
    }

    pub async fn list(&self, operator: &Operator, first_only: bool) -> Result<Vec<FileWithMeta>> {
        if let Some(files) = &self.files {
            let mut res = Vec::new();
            for file in files {
                let full_path = Path::new(&self.path)
                    .join(file)
                    .to_string_lossy()
                    .to_string();
                let meta = operator.stat(&full_path).await?;
                if meta.mode().is_file() {
                    res.push(FileWithMeta::new(&full_path, meta))
                } else {
                    return Err(ErrorCode::BadArguments(format!(
                        "{full_path} is not a file"
                    )));
                }
                if first_only {
                    break;
                }
            }
            Ok(res)
        } else {
            let pattern = self.get_pattern()?;
            StageFilesInfo::list_files_with_pattern(operator, &self.path, pattern, first_only).await
        }
    }

    pub async fn first_file(&self, operator: &Operator) -> Result<FileWithMeta> {
        let mut files = self.list(operator, true).await?;
        match files.pop() {
            None => Err(ErrorCode::BadArguments("no file found")),
            Some(f) => Ok(f),
        }
    }

    pub fn blocking_first_file(&self, operator: &Operator) -> Result<FileWithMeta> {
        let mut files = self.blocking_list(operator, true)?;
        match files.pop() {
            None => Err(ErrorCode::BadArguments("no file found")),
            Some(f) => Ok(f),
        }
    }

    pub fn blocking_list(
        &self,
        operator: &Operator,
        first_only: bool,
    ) -> Result<Vec<FileWithMeta>> {
        if let Some(files) = &self.files {
            let mut res = Vec::new();
            for file in files {
                let full_path = Path::new(&self.path)
                    .join(file)
                    .to_string_lossy()
                    .to_string();
                let meta = operator.blocking().stat(&full_path)?;
                if meta.mode().is_file() {
                    res.push(FileWithMeta::new(&full_path, meta))
                } else {
                    return Err(ErrorCode::BadArguments(format!(
                        "{full_path} is not a file"
                    )));
                }
                if first_only {
                    break;
                }
            }
            Ok(res)
        } else {
            let pattern = self.get_pattern()?;
            blocking_list_files_with_pattern(operator, &self.path, pattern, first_only)
        }
    }

    pub async fn list_files_with_pattern(
        operator: &Operator,
        path: &str,
        pattern: Option<Regex>,
        first_only: bool,
    ) -> Result<Vec<FileWithMeta>> {
        let root_meta = operator.stat(path).await;
        match root_meta {
            Ok(meta) => match meta.mode() {
                EntryMode::FILE => return Ok(vec![FileWithMeta::new(path, meta)]),
                EntryMode::DIR => {}
                EntryMode::Unknown => {
                    return Err(ErrorCode::BadArguments("object mode is unknown"));
                }
            },
            Err(e) => {
                if e.kind() == opendal::ErrorKind::NotFound {
                    return Ok(vec![]);
                } else {
                    return Err(e.into());
                }
            }
        };

        // path is a dir
        let mut files = Vec::new();
        let mut list = operator.scan(path).await?;
        while let Some(obj) = list.try_next().await? {
            // todo(youngsofun): not always need Metakey::Complete
            let meta = operator.metadata(&obj, Metakey::Complete).await?;
            if check_file(obj.path(), meta.mode(), &pattern) {
                files.push(FileWithMeta::new(obj.path(), meta));
                if first_only {
                    return Ok(files);
                }
            }
        }
        Ok(files)
    }
}

fn check_file(path: &str, mode: EntryMode, pattern: &Option<Regex>) -> bool {
    if mode.is_file() {
        match pattern {
            Some(p) => p.is_match(path),
            None => true,
        }
    } else {
        false
    }
}

fn blocking_list_files_with_pattern(
    operator: &Operator,
    path: &str,
    pattern: Option<Regex>,
    first_only: bool,
) -> Result<Vec<FileWithMeta>> {
    let operator = operator.blocking();

    let root_meta = operator.stat(path);
    match root_meta {
        Ok(meta) => match meta.mode() {
            EntryMode::FILE => return Ok(vec![FileWithMeta::new(path, meta)]),
            EntryMode::DIR => {}
            EntryMode::Unknown => return Err(ErrorCode::BadArguments("object mode is unknown")),
        },
        Err(e) => {
            if e.kind() == opendal::ErrorKind::NotFound {
                return Ok(vec![]);
            } else {
                return Err(e.into());
            }
        }
    };

    // path is a dir
    let mut files = Vec::new();
    let list = operator.list(path)?;
    for obj in list {
        let obj = obj?;
        let meta = operator.metadata(&obj, Metakey::Complete)?;
        if check_file(obj.path(), meta.mode(), &pattern) {
            files.push(FileWithMeta::new(obj.path(), meta));
            if first_only {
                return Ok(files);
            }
        }
    }
    Ok(files)
}

/// # Behavior
///
///
/// - `Ok(Some(v))` if given object is a file and no error happened.
/// - `Ok(None)` if given object is not a file.
/// - `Err(err)` if there is an error happened.
#[allow(unused)]
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

    Ok(Some(StageFileInfo::new(de.path().to_string(), &meta)))
}
