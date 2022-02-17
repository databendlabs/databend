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
use std::sync::Arc;

use common_datavalues2::DataSchemaRefExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::CopyPlan;
use common_planners::DiskStoragePlan;
use common_planners::FileFormatType;
use common_planners::FileStoragePlan;
use common_planners::PlanNode;
use common_planners::S3StoragePlan;
use common_planners::UserStagePlan;
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
}

impl DfCopy {
    fn to_file_format(typ: &str) -> Result<FileFormatType> {
        match typ.to_uppercase().as_str() {
            "CSV" => Ok(FileFormatType::Csv),
            "JSON" => Ok(FileFormatType::Json),
            "AVRO" => Ok(FileFormatType::Avro),
            "ORC" => Ok(FileFormatType::Orc),
            "PARQUET" => Ok(FileFormatType::Parquet),
            "XML" => Ok(FileFormatType::Xml),
            _ => Err(ErrorCode::SyntaxException(
                "Unknown file format type, must one of  { CSV | JSON | AVRO | ORC | PARQUET | XML }",
            )),
        }
    }
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

        // TODO(bohu): Add from @stage_name check.

        // File format type.
        let format = self
            .file_format_options
            .get("TYPE")
            .as_ref()
            .ok_or_else(|| ErrorCode::SyntaxException("File format type must be specified"))?;
        let file_format_type = Self::to_file_format(format)?;

        // Parse uri.
        let uri = uriparse::URIReference::try_from(self.location.as_str())
            .map_err(|e| ErrorCode::SyntaxException("File location uri must be specified"))?;

        // File storage plan.
        let storage = match uri.scheme() {
            None => Err(ErrorCode::SyntaxException(
                "File location scheme must be specified",
            )),
            Some(v) => match v.as_str() {
                // AWS s3 plan.
                "s3" => {
                    let credentials_aws_key_id = self
                        .credential_options
                        .get("aws_key_id")
                        .unwrap_or(&"".to_string())
                        .clone();
                    let credentials_aws_secret_key = self
                        .credential_options
                        .get("aws_key_id")
                        .unwrap_or(&"".to_string())
                        .clone();
                    let encryption_master_key = self
                        .encryption_options
                        .get("master_key")
                        .unwrap_or(&"".to_string())
                        .clone();

                    Ok(FileStoragePlan::S3(S3StoragePlan {
                        credentials_aws_key_id,
                        credentials_aws_secret_key,
                        encryption_master_key,
                    }))
                }

                // Others, Disk plan.
                _ => Ok(FileStoragePlan::Disk(DiskStoragePlan {})),
            },
        }?;

        // Stage plan.
        let stage_plan = UserStagePlan {
            location: self.location.clone(),
            storage,
            file_format_type,
        };

        // Copy plan.
        let plan_node = CopyPlan {
            db_name,
            tbl_name,
            tbl_id,
            schema,
            stage_plan,
            stage_name: None,
        };

        Ok(AnalyzedResult::SimpleQuery(Box::new(PlanNode::Copy(
            plan_node,
        ))))
    }
}
