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

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

use arrow_csv::reader::Format;
use arrow_json::reader::infer_json_schema;
use arrow_schema::Schema as ArrowSchema;
use databend_common_ast::ast::FileLocation;
use databend_common_ast::ast::UriLocation;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableSchema;
use databend_common_meta_app::principal::CsvFileFormatParams;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::StageType;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;
use databend_common_sql::binder::resolve_file_location;
use databend_common_storage::init_stage_operator;
use databend_common_storage::read_parquet_schema_async_rs;
use databend_common_storage::StageFilesInfo;
use databend_common_users::Object;
use opendal::Operator;
use opendal::Scheme;

use crate::table_functions::infer_schema::infer_schema_table::INFER_SCHEMA;
use crate::table_functions::infer_schema::table_args::InferSchemaArgsParsed;

pub(crate) struct InferSchemaSource {
    is_finished: bool,
    ctx: Arc<dyn TableContext>,
    args_parsed: InferSchemaArgsParsed,
}

impl InferSchemaSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        args_parsed: InferSchemaArgsParsed,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.clone(), output, InferSchemaSource {
            is_finished: false,
            ctx,
            args_parsed,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for InferSchemaSource {
    const NAME: &'static str = INFER_SCHEMA;

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.is_finished {
            return Ok(None);
        }
        self.is_finished = true;

        let file_location = if let Some(location) =
            self.args_parsed.location.clone().strip_prefix('@')
        {
            FileLocation::Stage(location.to_string())
        } else if let Some(connection_name) = &self.args_parsed.connection_name {
            let conn = self.ctx.get_connection(connection_name).await?;
            let uri =
                UriLocation::from_uri(self.args_parsed.location.clone(), conn.storage_params)?;
            let proto = conn.storage_type.parse::<Scheme>()?;
            if proto != uri.protocol.parse::<Scheme>()? {
                return Err(ErrorCode::BadArguments(format!(
                    "protocol from connection_name={connection_name} ({proto}) not match with uri protocol ({0}).",
                    uri.protocol
                )));
            }
            FileLocation::Uri(uri)
        } else {
            let uri =
                UriLocation::from_uri(self.args_parsed.location.clone(), BTreeMap::default())?;
            FileLocation::Uri(uri)
        };
        let (stage_info, path) = resolve_file_location(self.ctx.as_ref(), &file_location).await?;
        let enable_experimental_rbac_check = self
            .ctx
            .get_settings()
            .get_enable_experimental_rbac_check()?;
        if enable_experimental_rbac_check {
            let visibility_checker = self
                .ctx
                .get_visibility_checker(false, Object::Stage)
                .await?;
            if !(stage_info.is_temporary
                || visibility_checker.check_stage_read_visibility(&stage_info.stage_name)
                || stage_info.stage_type == StageType::User
                    && stage_info.stage_name == self.ctx.get_current_user()?.name)
            {
                return Err(ErrorCode::PermissionDenied(format!(
                    "Permission denied: privilege READ is required on stage {} for user {}",
                    stage_info.stage_name.clone(),
                    &self.ctx.get_current_user()?.identity().display(),
                )));
            }
        }
        let files_info = StageFilesInfo {
            path: path.clone(),
            ..self.args_parsed.files_info.clone()
        };
        let operator = init_stage_operator(&stage_info)?;

        let first_file = files_info.first_file(&operator).await?;
        let file_format_params = match &self.args_parsed.file_format {
            Some(f) => self.ctx.get_file_format(f).await?,
            None => stage_info.file_format_params.clone(),
        };
        let schema = match (first_file.as_ref(), file_format_params) {
            (None, _) => return Ok(None),
            (Some(first_file), FileFormatParams::Parquet(_)) => {
                let arrow_schema = read_parquet_schema_async_rs(
                    &operator,
                    &first_file.path,
                    Some(first_file.size),
                )
                .await?;
                TableSchema::try_from(&arrow_schema)?
            }
            (Some(first_file), FileFormatParams::Csv(params)) => {
                let arrow_schema = read_csv_metadata_async(
                    &first_file.path,
                    &operator,
                    Some(first_file.size),
                    self.args_parsed.max_records,
                    &params,
                )
                .await?;
                TableSchema::try_from(&arrow_schema)?
            }
            (Some(first_file), FileFormatParams::NdJson(_)) => {
                let arrow_schema = read_json_metadata_async(
                    &first_file.path,
                    &operator,
                    Some(first_file.size),
                    self.args_parsed.max_records,
                )
                .await?;
                TableSchema::try_from(&arrow_schema)?
            }
            _ => {
                return Err(ErrorCode::BadArguments(
                    "infer_schema is currently limited to format Parquet",
                ));
            }
        };

        let mut names: Vec<String> = vec![];
        let mut types: Vec<String> = vec![];
        let mut nulls: Vec<bool> = vec![];

        for field in schema.fields().iter() {
            names.push(field.name().to_string());

            let non_null_type = field.data_type().remove_recursive_nullable();
            types.push(non_null_type.sql_name());
            nulls.push(field.is_nullable());
        }

        let order_ids = (0..schema.fields().len() as u64).collect::<Vec<_>>();

        let block = DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            StringType::from_data(types),
            BooleanType::from_data(nulls),
            UInt64Type::from_data(order_ids),
        ]);
        Ok(Some(block))
    }
}

pub async fn read_csv_metadata_async(
    path: &str,
    operator: &Operator,
    file_size: Option<u64>,
    max_records: Option<usize>,
    params: &CsvFileFormatParams,
) -> Result<ArrowSchema> {
    let file_size = match file_size {
        None => operator.stat(path).await?.content_length(),
        Some(n) => n,
    };
    let escape = if params.escape.is_empty() {
        None
    } else {
        Some(params.escape.as_bytes()[0])
    };

    // TODO: It would be better if it could be read in the form of Read trait
    let buf = operator.read_with(path).range(..file_size).await?.to_vec();
    let mut format = Format::default()
        .with_delimiter(params.field_delimiter.as_bytes()[0])
        .with_quote(params.quote.as_bytes()[0])
        .with_header(params.headers != 0);

    if let Some(escape) = escape {
        format = format.with_escape(escape);
    }
    let (schema, _) = format.infer_schema(Cursor::new(&buf), max_records)?;

    Ok(schema)
}

pub async fn read_json_metadata_async(
    path: &str,
    operator: &Operator,
    file_size: Option<u64>,
    max_records: Option<usize>,
) -> Result<ArrowSchema> {
    let file_size = match file_size {
        None => operator.stat(path).await?.content_length(),
        Some(n) => n,
    };
    // TODO: It would be better if it could be read in the form of Read trait
    let buf = operator.read_with(path).range(..file_size).await?.to_vec();
    let (schema, _) = infer_json_schema(Cursor::new(&buf), max_records)?;

    Ok(schema)
}
