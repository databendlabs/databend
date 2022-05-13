// Copyright 2021 Datafuse Labs.
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

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;

use common_configs::S3StorageConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::get_abs_path;
use common_io::prelude::parse_escape_string;
use common_meta_types::FileFormatOptions;
use common_meta_types::StageFileFormatType;
use common_meta_types::StageStorage;
use common_meta_types::StageType;
use common_meta_types::UserStageInfo;
use sqlparser::ast::ObjectName;

use crate::sessions::QueryContext;

/// Returns user's stage info and relative path towards the stage's root.
pub async fn location_to_stage_path(
    location: &str,
    ctx: &Arc<QueryContext>,
) -> Result<(UserStageInfo, String)> {
    let mgr = ctx.get_user_manager();
    let s: Vec<&str> = location.split('@').collect();
    // @my_ext_stage/abc
    let names: Vec<&str> = s[1].splitn(2, '/').collect();
    let stage = mgr.get_stage(&ctx.get_tenant(), names[0]).await?;

    let path = if names.len() > 1 { names[1] } else { "" };

    let relative_path = match stage.stage_type {
        // It's internal, so we should prefix with stage name.
        StageType::Internal => {
            let prefix = format!("stage/{}", stage.stage_name);
            get_abs_path(prefix.as_str(), path)
        }
        StageType::External => path.to_string(),
    };
    Ok((stage, relative_path))
}

pub fn parse_stage_storage(
    location: &str,
    credential_options: &BTreeMap<String, String>,
    encryption_options: &BTreeMap<String, String>,
) -> Result<(StageStorage, String)> {
    // TODO(xuanwo): we should support use non-aws s3 as stage like oss.
    // TODO(xuanwo): we should make the path logic more clear, ref: https://github.com/datafuselabs/databend/issues/5295

    // Parse uri.
    // 's3://<bucket>[/<path>]'
    let uri = location.parse::<http::Uri>().map_err(|_e| {
        ErrorCode::SyntaxException(
            "File location uri must be specified, for example: 's3://<bucket>[/<path>]'",
        )
    })?;

    let bucket = uri
        .host()
        .ok_or_else(|| {
            ErrorCode::SyntaxException(
                "File location uri must be specified, for example: 's3://<bucket>[/<path>]'",
            )
        })?
        .to_string();
    // Path maybe a dir or a file.
    let path = uri.path().to_string();
    // Path endswith `/` means it's a directory, otherwise it's a file.
    // If the path is a directory, we will use this path as root.
    // If the path is a file, we will use `/` as root (which is the default value)
    let (root, path) = if path.ends_with('/') {
        (path.as_str(), "")
    } else {
        ("", path.as_str())
    };

    // File storage plan.
    match uri.scheme_str() {
        None => Err(ErrorCode::SyntaxException(
            "File location scheme must be specified",
        )),
        Some(v) => match v {
            // AWS s3 plan.
            "s3" => {
                let cfg = S3StorageConfig {
                    bucket,
                    root: root.to_string(),
                    access_key_id: credential_options
                        .get("aws_key_id")
                        .cloned()
                        .unwrap_or_default(),
                    secret_access_key: credential_options
                        .get("aws_secret_key")
                        .cloned()
                        .unwrap_or_default(),
                    master_key: encryption_options
                        .get("master_key")
                        .cloned()
                        .unwrap_or_default(),
                    ..Default::default()
                };

                Ok((StageStorage::S3(cfg), path.to_string()))
            }

            // Others.
            _ => Err(ErrorCode::SyntaxException(
                "File location uri unsupported, must be one of [s3, @stage]",
            )),
        },
    }
}

pub fn parse_copy_file_format_options(
    file_format_options: &BTreeMap<String, String>,
) -> Result<FileFormatOptions> {
    // File format type.
    let format = file_format_options
        .get("type")
        .ok_or_else(|| ErrorCode::SyntaxException("File format type must be specified"))?;
    let file_format = StageFileFormatType::from_str(format)
        .map_err(|e| ErrorCode::SyntaxException(format!("File format type error:{:?}", e)))?;

    // Skip header.
    let skip_header = file_format_options
        .get("skip_header")
        .unwrap_or(&"0".to_string())
        .parse::<u64>()?;

    // Field delimiter.
    let field_delimiter = parse_escape_string(
        file_format_options
            .get("field_delimiter")
            .unwrap_or(&"".to_string())
            .as_bytes(),
    );

    // Record delimiter.
    let record_delimiter = parse_escape_string(
        file_format_options
            .get("record_delimiter")
            .unwrap_or(&"".to_string())
            .as_bytes(),
    );

    Ok(FileFormatOptions {
        format: file_format,
        skip_header,
        field_delimiter,
        record_delimiter,
        compression: Default::default(),
    })
}

pub fn resolve_table(
    ctx: &QueryContext,
    object_name: &ObjectName,
    statement_name: &str,
) -> Result<(String, String, String)> {
    let idents = &object_name.0;
    match idents.len() {
        0 => Err(ErrorCode::SyntaxException(format!(
            "table name must be specified in statement `{}`",
            statement_name
        ))),
        1 => Ok((
            ctx.get_current_catalog(),
            ctx.get_current_database(),
            idents[0].value.clone(),
        )),
        2 => Ok((
            ctx.get_current_catalog(),
            idents[0].value.clone(),
            idents[1].value.clone(),
        )),
        3 => Ok((
            idents[0].value.clone(),
            idents[1].value.clone(),
            idents[2].value.clone(),
        )),
        _ => Err(ErrorCode::SyntaxException(format!(
            "table name should be [`catalog`].[`db`].`table` in statement {}",
            statement_name
        ))),
    }
}

pub fn resolve_database(
    ctx: &QueryContext,
    name: &ObjectName,
    statement_name: &str,
) -> Result<(String, String)> {
    let idents = &name.0;
    match idents.len() {
        0 => Err(ErrorCode::SyntaxException(format!(
            "database name must be specified in statement `{}`",
            statement_name
        ))),
        1 => Ok((ctx.get_current_catalog(), idents[0].value.clone())),
        2 => Ok((idents[0].value.clone(), idents[1].value.clone())),
        _ => Err(ErrorCode::SyntaxException(format!(
            "database name should be [`catalog`].`db` in statement {}",
            statement_name
        ))),
    }
}
