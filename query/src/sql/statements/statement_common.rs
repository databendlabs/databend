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

use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::parse_escape_string;
use common_io::prelude::StorageParams;
use common_io::prelude::StorageS3Config;
use common_meta_types::FileFormatOptions;
use common_meta_types::StageFileFormatType;
use common_meta_types::StageParams;
use common_meta_types::StageType;
use common_meta_types::UserStageInfo;
use common_tracing::tracing::debug;
use sqlparser::ast::ObjectName;

use crate::sessions::QueryContext;

/// Named stage(start with `@`):
///
/// ```sql
/// copy into mytable from @my_ext_stage
///     file_format = (type = csv);
/// ```
///
/// Returns user's stage info and relative path towards the stage's root.
///
/// If input location is empty we will convert it to `/` means the root of stage
///
/// - @mystage => (mystage, "/")
///
/// If input location is endswith `/`, it's a folder.
///
/// - @mystage/ => (mystage, "/")
///
/// Otherwise, it's a file
///
/// - @mystage/abc => (mystage, "abc")
///
/// For internal stage, we will also add prefix `/stage/<stage>/`
///
/// - @internal/abc => (internal, "/stage/internal/abc")
///
/// TODO(xuanwo): Move those logic into parser.
pub async fn parse_stage_location(
    ctx: &Arc<QueryContext>,
    location: &str,
) -> Result<(UserStageInfo, String)> {
    let mgr = ctx.get_user_manager();
    let s: Vec<&str> = location.split('@').collect();
    // @my_ext_stage/abc/
    let names: Vec<&str> = s[1].splitn(2, '/').filter(|v| !v.is_empty()).collect();
    let stage = mgr.get_stage(&ctx.get_tenant(), names[0]).await?;

    let path = names.get(1).unwrap_or(&"").trim_start_matches('/');

    let prefix = stage.get_prefix();
    let relative_path = format!("{prefix}{path}");

    debug!("parsed stage: {stage:?}, path: {relative_path}");
    Ok((stage, relative_path))
}

/// parse_stage_location_v2 work similar to parse_stage_location.
///
/// Difference is input location has already been parsed by parser.
///
/// TODO(xuanwo): Move this logic into parser
pub async fn parse_stage_location_v2(
    ctx: &Arc<QueryContext>,
    name: &str,
    path: &str,
) -> Result<(UserStageInfo, String)> {
    debug_assert!(path.starts_with('/'), "path should starts with '/'");

    let mgr = ctx.get_user_manager();
    let stage = mgr.get_stage(&ctx.get_tenant(), name).await?;

    let prefix = stage.get_prefix();
    debug_assert!(prefix.ends_with('/'), "prefix should ends with '/'");

    // prefix must be endswith `/`, so we should trim path here.
    let relative_path = format!("{prefix}{}", path.trim_start_matches('/'));

    debug!("parsed stage: {stage:?}, path: {relative_path}");
    Ok((stage, relative_path))
}

/// External stage(location starts without `@`):
///
/// ```sql
/// copy into table from 's3://mybucket/data/files'
///     credentials=(aws_key_id='my_key_id' aws_secret_key='my_secret_key')
///     encryption=(master_key = 'my_master_key')
///     file_format = (type = csv field_delimiter = '|' skip_header = 1)"
/// ```
///
/// TODO(xuanwo): Move this logic into parser
pub fn parse_uri_location(
    location: &str,
    credential_options: &BTreeMap<String, String>,
    encryption_options: &BTreeMap<String, String>,
) -> Result<(UserStageInfo, String)> {
    // TODO(xuanwo): we should support use non-aws s3 as stage like oss.
    // TODO(xuanwo): we should make the path logic more clear, ref: https://github.com/datafuselabs/databend/issues/5295

    // Parse uri.
    // 's3://<bucket>[/<path>/]'
    let uri = location.parse::<http::Uri>().map_err(|_e| {
        ErrorCode::SyntaxException(
            "File location uri must be specified, for example: 's3://<bucket>[/<path>/]'",
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
    let mut path = uri.path().to_string();
    // Rewrite path to `/` if it's empty to fit the directory check rule.
    if path.is_empty() {
        path = "/".to_string();
    }

    parse_uri_location_v2(
        uri.scheme_str().ok_or(ErrorCode::SyntaxException(
            "File location scheme must be specified",
        ))?,
        &bucket,
        &path,
        credential_options,
        encryption_options,
    )
}

/// External stage(location starts without `@`):
///
/// ```sql
/// copy into table from 's3://mybucket/data/files'
///     credentials=(aws_key_id='my_key_id' aws_secret_key='my_secret_key')
///     encryption=(master_key = 'my_master_key')
///     file_format = (type = csv field_delimiter = '|' skip_header = 1)"
/// ```
///
/// This function works similar with parse_uri_location.
/// Different is input location has been parsed.
///
/// TODO(xuanwo): Move this logic into parser
pub fn parse_uri_location_v2(
    protocol: &str,
    name: &str,
    path: &str,
    credentials: &BTreeMap<String, String>,
    encryption: &BTreeMap<String, String>,
) -> Result<(UserStageInfo, String)> {
    // Path endswith `/` means it's a directory, otherwise it's a file.
    // If the path is a directory, we will use this path as root.
    // If the path is a file, we will use `/` as root (which is the default value)
    let (root, path) = if path.ends_with('/') {
        (path, "/")
    } else {
        ("/", path)
    };

    // File storage plan.
    let stage_storage = match protocol {
        // AWS s3 plan.
        "s3" => {
            let cfg = StorageS3Config {
                bucket: name.to_string(),
                root: root.to_string(),
                access_key_id: credentials.get("aws_key_id").cloned().unwrap_or_default(),
                secret_access_key: credentials
                    .get("aws_secret_key")
                    .cloned()
                    .unwrap_or_default(),
                master_key: encryption.get("master_key").cloned().unwrap_or_default(),
                disable_credential_loader: true,
                ..Default::default()
            };

            Ok(StorageParams::S3(cfg))
        }

        // Others.
        _ => Err(ErrorCode::SyntaxException(
            "File location uri unsupported, must be one of [s3]",
        )),
    }?;

    let stage = UserStageInfo {
        stage_name: format!("{protocol}://{name}{path}"),
        stage_type: StageType::External,
        stage_params: StageParams {
            storage: stage_storage,
        },
        ..Default::default()
    };

    Ok((stage, path.to_string()))
}

/// TODO(xuanwo): Move those logic into parser
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

    // Compression delimiter.
    let compression = parse_escape_string(
        file_format_options
            .get("compression")
            .unwrap_or(&"none".to_string())
            .as_bytes(),
    )
    .parse()
    .map_err(ErrorCode::UnknownCompressionType)?;

    Ok(FileFormatOptions {
        format: file_format,
        skip_header,
        field_delimiter,
        record_delimiter,
        compression,
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
