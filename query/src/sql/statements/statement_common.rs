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

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::get_abs_path;
use common_meta_types::FileFormatOptions;
use common_meta_types::StageFileFormatType;
use common_meta_types::StageS3Storage;
use common_meta_types::StageStorage;
use common_meta_types::StageType;
use common_meta_types::UserStageInfo;

use crate::sessions::QueryContext;

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
    let related_path: String;
    match stage.stage_type {
        // It's internal, so we already have an op which has the root path
        // need to inject a tenant path
        StageType::Internal => {
            let prefix = format!("stage/{}", stage.stage_name);
            related_path = get_abs_path(prefix.as_str(), path);
        }
        // It's  external, so we need to join the root path
        StageType::External => match stage.stage_params.storage {
            StageStorage::S3(ref s3) => {
                related_path = get_abs_path(s3.path.as_str(), path);
            }
        },
    }
    Ok((stage, related_path))
}

// path_as_root set to true when we create external stage
// path_as_root set to false when we copy from external stage
pub fn parse_stage_storage(
    location: &str,
    credential_options: &HashMap<String, String>,
    encryption_options: &HashMap<String, String>,
) -> Result<(StageStorage, String)> {
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

    // File storage plan.
    match uri.scheme_str() {
        None => Err(ErrorCode::SyntaxException(
            "File location scheme must be specified",
        )),
        Some(v) => match v {
            // AWS s3 plan.
            "s3" => {
                let credentials_aws_key_id = credential_options
                    .get("aws_key_id")
                    .unwrap_or(&"".to_string())
                    .clone();
                let credentials_aws_secret_key = credential_options
                    .get("aws_secret_key")
                    .unwrap_or(&"".to_string())
                    .clone();
                let encryption_master_key = encryption_options
                    .get("master_key")
                    .unwrap_or(&"".to_string())
                    .clone();

                let storage_stage = StageStorage::S3(StageS3Storage {
                    bucket,
                    path: path.clone(),
                    credentials_aws_key_id,
                    credentials_aws_secret_key,
                    encryption_master_key,
                });

                Ok((storage_stage, path))
            }

            // Others.
            _ => Err(ErrorCode::SyntaxException(
                "File location uri unsupported, must be one of [s3, @stage]",
            )),
        },
    }
}

pub fn parse_copy_file_format_options(
    file_format_options: &HashMap<String, String>,
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
    let field_delimiter = file_format_options
        .get("field_delimiter")
        .unwrap_or(&"".to_string())
        .clone();

    // Record delimiter.
    let record_delimiter = file_format_options
        .get("record_delimiter")
        .unwrap_or(&"".to_string())
        .clone();

    Ok(FileFormatOptions {
        format: file_format,
        skip_header,
        field_delimiter,
        record_delimiter,
        compression: Default::default(),
    })
}
