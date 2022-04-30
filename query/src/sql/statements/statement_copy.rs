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

use common_datavalues::DataSchemaRefExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::OnErrorMode;
use common_meta_types::StageParams;
use common_meta_types::StageType;
use common_meta_types::UserStageInfo;
use common_planners::CopyPlan;
use common_planners::PlanNode;
use common_planners::ReadDataSourcePlan;
use common_planners::S3StageTableInfo;
use common_planners::SourceInfo;
use common_planners::ValidationMode;
use sqlparser::ast::Ident;
use sqlparser::ast::ObjectName;

use super::location_to_stage_path;
use super::parse_copy_file_format_options;
use super::parse_stage_storage;
use crate::sessions::QueryContext;
use crate::sql::statements::resolve_table;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq)]
pub struct DfCopy {
    pub name: ObjectName,
    pub columns: Vec<Ident>,
    pub location: String,
    pub credential_options: BTreeMap<String, String>,
    pub encryption_options: BTreeMap<String, String>,
    pub file_format_options: BTreeMap<String, String>,
    pub files: Vec<String>,
    pub pattern: String,
    pub on_error: String,
    pub size_limit: String,
    pub validation_mode: String,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfCopy {
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        let (catalog_name, db_name, tbl_name) = resolve_table(&ctx, &self.name, "COPY")?;
        let table = ctx.get_table(&catalog_name, &db_name, &tbl_name).await?;
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
        let (mut stage_info, path) = if self.location.starts_with('@') {
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
            catalog: catalog_name.clone(),
            source_info: SourceInfo::S3StageSource(S3StageTableInfo {
                schema: schema.clone(),
                stage_info,
                path,
                files: vec![],
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
            catalog_name,
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
    async fn analyze_named(&self, ctx: &Arc<QueryContext>) -> Result<(UserStageInfo, String)> {
        location_to_stage_path(self.location.as_str(), ctx).await
    }

    // External stage(location starts without `@`):
    // copy into table from 's3://mybucket/data/files'
    // credentials=(aws_key_id='my_key_id' aws_secret_key='my_secret_key')
    // encryption=(master_key = 'my_master_key')
    // file_format = (type = csv field_delimiter = '|' skip_header = 1)"
    async fn analyze_location(&self) -> Result<(UserStageInfo, String)> {
        let (stage_storage, path) = parse_stage_storage(
            &self.location,
            &self.credential_options,
            &self.encryption_options,
        )?;
        // Stage params.
        let stage_params = StageParams {
            storage: stage_storage,
        };
        // Stage info.
        let stage = UserStageInfo {
            stage_name: self.location.clone(),
            stage_type: StageType::External,
            stage_params,
            ..Default::default()
        };
        Ok((stage, path))
    }
}
