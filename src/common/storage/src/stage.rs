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

use std::path::Path;

use chrono::DateTime;
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
            last_modified: meta.last_modified().unwrap_or_default(),
            etag: meta.etag().map(str::to_string),
            status: StageFileStatus::NeedCopy,
            creator: None,
        }
    }

    /// NOTE: update this query when add new meta
    pub fn meta_query() -> flagset::FlagSet<Metakey> {
        Metakey::ContentLength | Metakey::ContentMd5 | Metakey::LastModified | Metakey::Etag
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

    #[async_backtrace::framed]
    pub async fn list(
        &self,
        operator: &Operator,
        first_only: bool,
        max_files: Option<usize>,
    ) -> Result<Vec<StageFileInfo>> {
        let max_files = max_files.unwrap_or(usize::MAX);
        if let Some(files) = &self.files {
            let mut res = Vec::new();
            let mut limit: usize = 0;
            for file in files {
                let full_path = Path::new(&self.path)
                    .join(file)
                    .to_string_lossy()
                    .to_string();
                let meta = operator.stat(&full_path).await?;
                if meta.mode().is_file() {
                    res.push(StageFileInfo::new(full_path, &meta))
                } else {
                    return Err(ErrorCode::BadArguments(format!(
                        "{full_path} is not a file"
                    )));
                }
                if first_only {
                    return Ok(res);
                }
                limit += 1;
                if limit == max_files {
                    return Ok(res);
                }
            }
            Ok(res)
        } else {
            let pattern = self.get_pattern()?;
            StageFilesInfo::list_files_with_pattern(
                operator, &self.path, pattern, first_only, max_files,
            )
            .await
        }
    }

    #[async_backtrace::framed]
    pub async fn first_file(&self, operator: &Operator) -> Result<StageFileInfo> {
        let mut files = self.list(operator, true, None).await?;
        files.pop().ok_or(ErrorCode::BadArguments("no file found"))
    }

    pub fn blocking_first_file(&self, operator: &Operator) -> Result<StageFileInfo> {
        let mut files = self.blocking_list(operator, true, None)?;
        files.pop().ok_or(ErrorCode::BadArguments("no file found"))
    }

    pub fn blocking_list(
        &self,
        operator: &Operator,
        first_only: bool,
        max_files: Option<usize>,
    ) -> Result<Vec<StageFileInfo>> {
        let max_files = max_files.unwrap_or(usize::MAX);
        let mut limit = 0;
        if let Some(files) = &self.files {
            let mut res = Vec::new();
            for file in files {
                let full_path = Path::new(&self.path)
                    .join(file)
                    .to_string_lossy()
                    .to_string();
                let meta = operator.blocking().stat(&full_path)?;
                if meta.mode().is_file() {
                    res.push(StageFileInfo::new(full_path, &meta))
                } else {
                    return Err(ErrorCode::BadArguments(format!(
                        "{full_path} is not a file"
                    )));
                }
                if first_only {
                    break;
                }
                limit += 1;
                if limit == max_files {
                    return Ok(res);
                }
            }
            Ok(res)
        } else {
            let pattern = self.get_pattern()?;
            blocking_list_files_with_pattern(operator, &self.path, pattern, first_only, max_files)
        }
    }

    #[async_backtrace::framed]
    pub async fn list_files_with_pattern(
        operator: &Operator,
        path: &str,
        pattern: Option<Regex>,
        first_only: bool,
        max_files: usize,
    ) -> Result<Vec<StageFileInfo>> {
        let root_meta = operator.stat(path).await;
        match root_meta {
            Ok(meta) => match meta.mode() {
                EntryMode::FILE => return Ok(vec![StageFileInfo::new(path.to_string(), &meta)]),
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
        let mut limit: usize = 0;
        while let Some(obj) = list.try_next().await? {
            let meta = operator.metadata(&obj, StageFileInfo::meta_query()).await?;
            if check_file(obj.path(), meta.mode(), &pattern) {
                files.push(StageFileInfo::new(obj.path().to_string(), &meta));
                if first_only {
                    return Ok(files);
                }
                limit += 1;
                if limit == max_files {
                    return Ok(files);
                }
            }
        }
        Ok(files)
    }
}

fn check_file(path: &str, mode: EntryMode, pattern: &Option<Regex>) -> bool {
    if mode.is_file() {
        pattern.as_ref().map_or(true, |p| p.is_match(path))
    } else {
        false
    }
}

fn blocking_list_files_with_pattern(
    operator: &Operator,
    path: &str,
    pattern: Option<Regex>,
    first_only: bool,
    max_files: usize,
) -> Result<Vec<StageFileInfo>> {
    let operator = operator.blocking();

    let root_meta = operator.stat(path);
    match root_meta {
        Ok(meta) => match meta.mode() {
            EntryMode::FILE => return Ok(vec![StageFileInfo::new(path.to_string(), &meta)]),
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
    let list = operator.scan(path)?;
    let mut limit = 0;
    for obj in list {
        let obj = obj?;
        let meta = operator.metadata(&obj, StageFileInfo::meta_query())?;
        if check_file(obj.path(), meta.mode(), &pattern) {
            files.push(StageFileInfo::new(obj.path().to_string(), &meta));
            if first_only {
                return Ok(files);
            }
            limit += 1;
            if limit == max_files {
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
#[async_backtrace::framed]
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
