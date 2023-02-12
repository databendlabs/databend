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

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::principal::StageType;
use common_meta_app::principal::UserStageInfo;
use futures::TryStreamExt;
use opendal::ObjectMetadata;
use opendal::ObjectMode;
use opendal::Operator;
use regex::Regex;

use crate::init_operator;
use crate::DataOperator;

pub struct FileWithMeta {
    pub path: String,
    pub meta: ObjectMetadata,
}

impl FileWithMeta {
    fn new(path: &str, meta: ObjectMetadata) -> Self {
        Self {
            path: path.to_string(),
            meta,
        }
    }
}

pub fn init_stage_operator(stage_info: &UserStageInfo) -> Result<Operator> {
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
                let meta = operator.object(&full_path).metadata().await?;
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
            list_files_with_pattern(operator, &self.path, pattern, first_only).await
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
                let meta = operator.object(&full_path).blocking_metadata()?;
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
}

async fn list_files_with_pattern(
    operator: &Operator,
    path: &str,
    pattern: Option<Regex>,
    first_only: bool,
) -> Result<Vec<FileWithMeta>> {
    let root_obj = operator.object(path);
    let root_meta = root_obj.metadata().await;
    match root_meta {
        Ok(meta) => match meta.mode() {
            ObjectMode::FILE => return Ok(vec![FileWithMeta::new(path, meta)]),
            ObjectMode::DIR => {}
            ObjectMode::Unknown => return Err(ErrorCode::BadArguments("object mode is unknown")),
        },
        Err(e) => {
            if e.kind() == opendal::ErrorKind::ObjectNotFound {
                return Ok(vec![]);
            } else {
                return Err(e.into());
            }
        }
    };

    // path is a dir
    let mut files = Vec::new();
    let mut list = operator.object(path).scan().await?;
    while let Some(obj) = list.try_next().await? {
        let meta = obj.metadata().await?;
        if check_file(obj.path(), &meta, &pattern) {
            files.push(FileWithMeta::new(obj.path(), meta));
            if first_only {
                return Ok(files);
            }
        }
    }
    Ok(files)
}

fn check_file(path: &str, meta: &ObjectMetadata, pattern: &Option<Regex>) -> bool {
    if meta.mode().is_file() {
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
    let root_obj = operator.object(path);
    let root_meta = root_obj.blocking_metadata();
    match root_meta {
        Ok(meta) => match meta.mode() {
            ObjectMode::FILE => return Ok(vec![FileWithMeta::new(path, meta)]),
            ObjectMode::DIR => {}
            ObjectMode::Unknown => return Err(ErrorCode::BadArguments("object mode is unknown")),
        },
        Err(e) => {
            if e.kind() == opendal::ErrorKind::ObjectNotFound {
                return Ok(vec![]);
            } else {
                return Err(e.into());
            }
        }
    };

    // path is a dir
    let mut files = Vec::new();
    let list = root_obj.blocking_list()?;
    for obj in list {
        let obj = obj?;
        let meta = obj.blocking_metadata()?;
        if check_file(obj.path(), &meta, &pattern) {
            files.push(FileWithMeta::new(obj.path(), meta));
            if first_only {
                return Ok(files);
            }
        }
    }
    Ok(files)
}
