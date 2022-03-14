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

use common_datavalues::DataSchemaRefExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::get_abs_path;
use common_meta_types::FileFormatOptions;
use common_meta_types::OnErrorMode;
use common_meta_types::StageFileFormatType;
use common_meta_types::StageParams;
use common_meta_types::StageS3Storage;
use common_meta_types::StageStorage;
use common_meta_types::StageType;
use common_meta_types::UserStageInfo;
use common_planners::CopyPlan;
use common_planners::PlanNode;
use common_planners::ReadDataSourcePlan;
use common_planners::S3ExternalTableInfo;
use common_planners::SourceInfo;
use common_planners::ValidationMode;
use sqlparser::ast::Ident;
use sqlparser::ast::ObjectName;

use crate::sessions::QueryContext;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq)]
pub struct DfCopy {
    pub name: ObjectName,
    pub columns: Vec<Ident>,
    pub location: String,
    pub credential_options: HashMap<String, String>,
    pub encryption_options: HashMap<String, String>,
    pub file_format_options: HashMap<String, String>,
    pub files: Vec<String>,
    pub pattern: String,
    pub on_error: String,
    pub size_limit: String,
    pub validation_mode: String,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfCopy {
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        let mut db_name = ctx.get_current_database();
        let mut tbl_name = self.name.0[0].value.clone();

        if self.name.0.len() > 1 {
            db_name = tbl_name;
            tbl_name = self.name.0[1].value.clone();
        }

        let table = ctx.get_table(&db_name, &tbl_name).await?;
        let mut schema = table.schema();
        let tbl_id = table.get_id();

        if !self.columns.is_empty() {
            let fields = self
                .columns
                .iter()
                .map(|ident| schema.field_with_name(&ident.value).map(|v| v.clone()))
                .collect::<Result<Vec<_>>>()?;

            schema = DataSchemaRefExt::create(fields);
        }

        // Stage info.
        let mut stage_info = if self.location.starts_with('@') {
            self.analyze_named(&ctx).await?
        } else {
            self.analyze_location().await?
        };

        if !self.file_format_options.is_empty() {
            stage_info.file_format_options =
                parse_copy_file_format_options(&self.file_format_options)?;
        }

        // Copy options.
        {
            // on_error.
            if !self.on_error.is_empty() {
                stage_info.copy_options.on_error =
                    OnErrorMode::from_str(&self.on_error).map_err(ErrorCode::SyntaxException)?;
            }

            // size_limit.
            if !self.size_limit.is_empty() {
                let size_limit = self.size_limit.parse::<usize>().map_err(|_e| {
                    ErrorCode::SyntaxException(format!(
                        "size_limit must be number, got: {}",
                        self.size_limit
                    ))
                })?;
                stage_info.copy_options.size_limit = size_limit;
            }
        }

        // Validation mode.
        let validation_mode = ValidationMode::from_str(self.validation_mode.as_str())
            .map_err(ErrorCode::SyntaxException)?;

        // Read source plan.
        let from = ReadDataSourcePlan {
            source_info: SourceInfo::S3ExternalSource(S3ExternalTableInfo {
                schema: schema.clone(),
                file_name: None,
                stage_info,
            }),
            scan_fields: None,
            parts: vec![],
            statistics: Default::default(),
            description: "".to_string(),
            tbl_args: None,
            push_downs: None,
        };

        // Pattern.
        let pattern = self.pattern.clone();

        // Copy plan.
        let plan_node = CopyPlan {
            db_name,
            tbl_name,
            tbl_id,
            schema,
            from,
            validation_mode,
            files: self.files.clone(),
            pattern,
        };

        Ok(AnalyzedResult::SimpleQuery(Box::new(PlanNode::Copy(
            plan_node,
        ))))
    }
}

impl DfCopy {
    // Named stage(start with `@`):
    // copy into mytable from @my_ext_stage
    // file_format = (type = csv);
    async fn analyze_named(&self, ctx: &Arc<QueryContext>) -> Result<UserStageInfo> {
        let mgr = ctx.get_user_manager();
        let s: Vec<&str> = self.location.split('@').collect();
        // @my_ext_stage/abc
        let names: Vec<&str> = s[1].splitn(2, '/').collect();
        let mut stage = mgr.get_stage(&ctx.get_tenant(), names[0]).await?;

        let path = if names.len() > 1 { names[1] } else { "/" };
        // Set Path
        match &mut stage.stage_params.storage {
            StageStorage::S3(v) => match stage.stage_type {
                // It's internal, so we already have an op which has the root path
                StageType::Internal => {
                    v.path = path.to_string();
                }
                // It's  external, so we need to join the root path
                StageType::External => {
                    v.path = get_abs_path(v.path.as_str(), path);
                }
            },
        }

        Ok(stage)
    }

    // External stage(location starts without `@`):
    // copy into table from 's3://mybucket/data/files'
    // credentials=(aws_key_id='my_key_id' aws_secret_key='my_secret_key')
    // encryption=(master_key = 'my_master_key')
    // file_format = (type = csv field_delimiter = '|' skip_header = 1)"
    async fn analyze_location(&self) -> Result<UserStageInfo> {
        let stage_storage = parse_stage_storage(
            &self.location,
            &self.credential_options,
            &self.encryption_options,
        )?;
        // Stage params.
        let stage_params = StageParams {
            storage: stage_storage,
        };
        // Stage info.
        Ok(UserStageInfo {
            stage_name: self.location.clone(),
            stage_type: StageType::External,
            stage_params,
            ..Default::default()
        })
    }
}

pub fn parse_stage_storage(
    location: &str,
    credential_options: &HashMap<String, String>,
    encryption_options: &HashMap<String, String>,
) -> Result<StageStorage> {
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

                Ok(StageStorage::S3(StageS3Storage {
                    bucket,
                    path,
                    credentials_aws_key_id,
                    credentials_aws_secret_key,
                    encryption_master_key,
                }))
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
        .parse::<i32>()?;

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
