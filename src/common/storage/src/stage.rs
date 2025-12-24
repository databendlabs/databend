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
use std::pin::Pin;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use databend_common_base::runtime::execute_futures_in_parallel;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::principal::StageType;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::storage::StorageParams;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream;
use opendal::EntryMode;
use opendal::Metadata;
use opendal::Operator;
use regex::Regex;

use crate::DataOperator;
use crate::init_operator;

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
    pub last_modified: Option<DateTime<Utc>>,
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
            last_modified: meta.last_modified().map(|m| {
                let ns = m.into_inner().as_nanosecond();
                DateTime::from_timestamp_nanos(ns as i64)
            }),
            etag: meta.etag().map(str::to_string),
            status: StageFileStatus::NeedCopy,
            creator: None,
        }
    }

    pub fn dedup_key(&self) -> String {
        // should not use last_modified because the accuracy is in seconds for S3.
        if let Some(md5) = &self.md5 {
            md5.clone()
        } else {
            let last_modified = self
                .last_modified
                .as_ref()
                .map(|x| x.to_string())
                .unwrap_or_default();

            self.etag
                .clone()
                .unwrap_or(format!("{}/{last_modified}/{}", self.path, self.size))
        }
    }
}

pub fn init_stage_operator(stage_info: &StageInfo) -> Result<Operator> {
    if stage_info.stage_type == StageType::External {
        // External S3 stages disallow the ambient credential chain by default.
        // `role_arn` opts into using the credential chain as the source credential.
        let storage = match stage_info.stage_params.storage.clone() {
            StorageParams::S3(mut cfg) => {
                let allow_credential_chain =
                    stage_info.allow_credential_chain || !cfg.role_arn.is_empty();
                cfg.allow_credential_chain = Some(allow_credential_chain);
                StorageParams::S3(cfg)
            }
            v => v,
        };

        Ok(init_operator(&storage)?)
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
#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq, Debug, Default)]
pub struct StageFilesInfo {
    pub path: String,
    pub files: Option<Vec<String>>,
    pub pattern: Option<String>,
}

pub type StageFileInfoStream = Pin<Box<dyn Stream<Item = Result<StageFileInfo>> + Send>>;

impl StageFilesInfo {
    fn get_pattern(&self) -> Result<Option<Regex>> {
        match &self.pattern {
            Some(pattern) => match Regex::new(&format!("^{pattern}$")) {
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
    async fn list_files(
        &self,
        operator: &Operator,
        thread_num: usize,
        max_files: Option<usize>,
        mut files: &[String],
    ) -> Result<Vec<StageFileInfo>> {
        if let Some(m) = max_files {
            files = &files[..m.min(files.len())]
        }
        let file_infos = self.stat_concurrent(operator, thread_num, files).await?;
        let mut res = Vec::with_capacity(file_infos.len());

        for file_info in file_infos {
            match file_info {
                Ok((path, meta)) if meta.is_dir() => {
                    return Err(ErrorCode::BadArguments(format!("{path} is not a file")));
                }
                Ok((path, meta)) => res.push(StageFileInfo::new(path, &meta)),
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(res)
    }

    #[async_backtrace::framed]
    pub async fn list(
        &self,
        operator: &Operator,
        thread_num: usize,
        max_files: Option<usize>,
    ) -> Result<Vec<StageFileInfo>> {
        if self.path == STDIN_FD {
            return Ok(vec![stdin_stage_info()]);
        }

        if let Some(files) = &self.files {
            self.list_files(operator, thread_num, max_files, files)
                .await
        } else {
            let pattern = self.get_pattern()?;
            StageFilesInfo::list_files_with_pattern(
                operator,
                &self.path,
                pattern,
                // TODO(youngsofun): after select-from-stage and list-stage use list_stream, we will force caller to provide max_files here.
                max_files.unwrap_or(usize::MAX),
            )
            .await
        }
    }

    #[async_backtrace::framed]
    pub async fn list_stream(
        &self,
        operator: &Operator,
        thread_num: usize,
        max_files: Option<usize>,
    ) -> Result<StageFileInfoStream> {
        if self.path == STDIN_FD {
            return Ok(Box::pin(stream::iter(vec![Ok(stdin_stage_info())])));
        }

        if let Some(files) = &self.files {
            let files = self
                .list_files(operator, thread_num, max_files, files)
                .await?;
            let files = files.into_iter().map(Ok);
            Ok(Box::pin(stream::iter(files)))
        } else {
            let pattern = self.get_pattern()?;
            StageFilesInfo::list_files_stream_with_pattern(operator, &self.path, pattern, max_files)
                .await
        }
    }

    #[async_backtrace::framed]
    pub async fn first_file(&self, operator: &Operator) -> Result<Option<StageFileInfo>> {
        // We only fetch first file.
        let mut files = self.list(operator, 1, Some(1)).await?;
        Ok(files.pop())
    }

    #[async_backtrace::framed]
    pub async fn list_files_with_pattern(
        operator: &Operator,
        path: &str,
        pattern: Option<Regex>,
        max_files: usize,
    ) -> Result<Vec<StageFileInfo>> {
        Self::list_files_stream_with_pattern(operator, path, pattern, Some(max_files))
            .await?
            .try_collect::<Vec<_>>()
            .await
    }

    #[async_backtrace::framed]
    pub async fn list_files_stream_with_pattern(
        operator: &Operator,
        path: &str,
        pattern: Option<Regex>,
        max_files: Option<usize>,
    ) -> Result<StageFileInfoStream> {
        if path == STDIN_FD {
            return Ok(Box::pin(stream::once(async { Ok(stdin_stage_info()) })));
        }
        let prefix_len = if path == "/" { 0 } else { path.len() };
        let prefix_meta = operator.stat(path).await;
        let file_exact: Option<Result<StageFileInfo>> = match prefix_meta {
            Ok(meta) if meta.is_file() => {
                let f = StageFileInfo::new(path.to_string(), &meta);
                if max_files == Some(1) {
                    return Ok(Box::pin(stream::once(async { Ok(f) })));
                }
                Some(Ok(f))
            }
            Err(e) if e.kind() != opendal::ErrorKind::NotFound => {
                return Err(e.into());
            }
            _ => None,
        };
        let file_exact_stream = stream::iter(file_exact.clone().into_iter());

        let lister = operator.lister_with(path).recursive(true).await?;

        let pattern = Arc::new(pattern);
        let operator = operator.clone();
        let files_with_prefix = lister.filter_map(move |result| {
            let pattern = pattern.clone();
            let operator = operator.clone();
            async move {
                match result {
                    Ok(entry) => {
                        let (path, mut meta) = entry.into_parts();
                        if check_file(&path[prefix_len..], meta.mode(), &pattern) {
                            if meta.etag().is_none() {
                                meta = match operator.stat(&path).await {
                                    Ok(meta) => meta,
                                    Err(err) => return Some(Err(ErrorCode::from(err))),
                                }
                            }

                            Some(Ok(StageFileInfo::new(path, &meta)))
                        } else {
                            None
                        }
                    }
                    Err(e) => Some(Err(ErrorCode::from(e))),
                }
            }
        });
        if let Some(max_files) = max_files {
            if file_exact.is_some() {
                Ok(Box::pin(
                    file_exact_stream.chain(files_with_prefix.take(max_files - 1)),
                ))
            } else {
                Ok(Box::pin(files_with_prefix.take(max_files)))
            }
        } else {
            Ok(Box::pin(file_exact_stream.chain(files_with_prefix)))
        }
    }

    /// Stat files concurrently.
    #[async_backtrace::framed]
    pub async fn stat_concurrent(
        &self,
        operator: &Operator,
        thread_num: usize,
        files: &[String],
    ) -> Result<Vec<Result<(String, Metadata)>>> {
        if files.len() == 1 {
            let Some(file) = files.first() else {
                return Ok(vec![]);
            };
            let full_path = Path::new(&self.path)
                .join(file)
                .to_string_lossy()
                .trim_start_matches('/')
                .to_string();
            let meta = operator.stat(&full_path).await;
            return Ok(vec![meta.map(|m| (full_path, m)).map_err(Into::into)]);
        }

        // This clone is required to make sure we are not referring to `file: &String` in the closure
        let tasks = files.iter().cloned().map(|file| {
            let full_path = Path::new(&self.path)
                .join(file)
                .to_string_lossy()
                .trim_start_matches('/')
                .to_string();
            let operator = operator.clone();
            async move {
                let meta = operator.stat(&full_path).await?;
                Ok((full_path, meta))
            }
        });

        execute_futures_in_parallel(
            tasks,
            thread_num * 5,
            thread_num * 10,
            "batch-stat-file-worker".to_owned(),
        )
        .await
    }
}

fn check_file(path: &str, mode: EntryMode, pattern: &Option<Regex>) -> bool {
    if !path.is_empty() && mode.is_file() {
        pattern.as_ref().is_none_or(|p| p.is_match(path))
    } else {
        false
    }
}

pub const STDIN_FD: &str = "/dev/fd/0";

fn stdin_stage_info() -> StageFileInfo {
    StageFileInfo {
        path: STDIN_FD.to_string(),
        size: u64::MAX,
        md5: None,
        last_modified: Some(Utc::now()),
        etag: None,
        status: StageFileStatus::NeedCopy,
        creator: None,
    }
}
