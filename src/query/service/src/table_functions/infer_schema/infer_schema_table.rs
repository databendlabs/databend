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

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

use databend_common_ast::ast::FileLocation;
use databend_common_ast::ast::UriLocation;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::StageTableInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_compress::CompressAlgorithm;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::BlockThresholds;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::principal::StageType;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sources::PrefetchAsyncSourcer;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_sql::binder::resolve_file_location;
use databend_common_storage::init_stage_operator;
use databend_common_storage::StageFilesInfo;
use databend_common_storages_stage::BytesReader;
use databend_common_storages_stage::Decompressor;
use databend_common_storages_stage::InferSchemaPartInfo;
use databend_common_storages_stage::LoadContext;
use databend_common_users::Object;
use databend_storages_common_stage::SingleFilePartition;
use opendal::Scheme;

use super::parquet::ParquetInferSchemaSource;
use crate::sessions::TableContext;
use crate::table_functions::infer_schema::separator::InferSchemaSeparator;
use crate::table_functions::infer_schema::table_args::InferSchemaArgsParsed;
use crate::table_functions::TableFunction;

pub(crate) const INFER_SCHEMA: &str = "infer_schema";

pub struct InferSchemaTable {
    table_info: TableInfo,
    args_parsed: InferSchemaArgsParsed,
    table_args: TableArgs,
}

impl InferSchemaTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let args_parsed = InferSchemaArgsParsed::parse(&table_args)?;
        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: table_func_name.to_string(),
            meta: TableMeta {
                schema: Self::schema(),
                engine: INFER_SCHEMA.to_owned(),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(Self {
            table_info,
            args_parsed,
            table_args,
        }))
    }

    pub fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("column_name", TableDataType::String),
            TableField::new("type", TableDataType::String),
            TableField::new("nullable", TableDataType::Boolean),
            TableField::new("filenames", TableDataType::String),
            TableField::new("order_id", TableDataType::Number(NumberDataType::UInt64)),
        ])
    }

    fn build_read_stage_source(
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        stage_info: &StageInfo,
    ) -> Result<()> {
        let operator = init_stage_operator(stage_info)?;
        let batch_size = ctx.get_settings().get_input_read_buffer_size()? as usize;
        pipeline.add_source(
            |output| {
                let reader = BytesReader::try_create(ctx.clone(), operator.clone(), batch_size, 1)?;
                PrefetchAsyncSourcer::create(ctx.clone(), output, reader)
            },
            1,
        )?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Table for InferSchemaTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        let file_location = if let Some(location) =
            self.args_parsed.location.clone().strip_prefix('@')
        {
            FileLocation::Stage(location.to_string())
        } else if let Some(connection_name) = &self.args_parsed.connection_name {
            let conn = ctx.get_connection(connection_name).await?;
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
        let (stage_info, path) = resolve_file_location(ctx.as_ref(), &file_location).await?;
        let enable_experimental_rbac_check =
            ctx.get_settings().get_enable_experimental_rbac_check()?;
        if enable_experimental_rbac_check {
            let visibility_checker = ctx.get_visibility_checker(false, Object::Stage).await?;
            if !(stage_info.is_temporary
                || visibility_checker.check_stage_read_visibility(&stage_info.stage_name)
                || stage_info.stage_type == StageType::User
                    && stage_info.stage_name == ctx.get_current_user()?.name)
            {
                return Err(ErrorCode::PermissionDenied(format!(
                    "Permission denied: privilege READ is required on stage {} for user {}",
                    stage_info.stage_name.clone(),
                    &ctx.get_current_user()?.identity().display(),
                )));
            }
        }
        let files_info = StageFilesInfo {
            path: path.clone(),
            ..self.args_parsed.files_info.clone()
        };

        let file_format_params = match &self.args_parsed.file_format {
            Some(f) => ctx.get_file_format(f).await?,
            None => stage_info.file_format_params.clone(),
        };
        let operator = init_stage_operator(&stage_info)?;
        let stage_file_infos = files_info
            .list(&operator, 1, self.args_parsed.max_file_count)
            .await?;
        Ok((
            PartStatistics::default(),
            Partitions::create(PartitionsShuffleKind::Seq, vec![
                InferSchemaPartInfo::create(
                    files_info,
                    file_format_params,
                    stage_info,
                    stage_file_infos,
                ),
            ]),
        ))
    }

    fn table_args(&self) -> Option<TableArgs> {
        Some(self.table_args.clone())
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        let Some(part) = ctx.get_partition() else {
            return Ok(());
        };
        let info = InferSchemaPartInfo::from_part(&part)?;

        match info.file_format_params {
            FileFormatParams::Csv(_) | FileFormatParams::NdJson(_) => {
                let partitions = info
                    .stage_file_infos
                    .iter()
                    .map(|v| {
                        let part = SingleFilePartition {
                            path: v.path.clone(),
                            size: v.size as usize,
                        };
                        let part_info: Box<dyn PartInfo> = Box::new(part);
                        Arc::new(part_info)
                    })
                    .collect::<Vec<_>>();
                ctx.set_partitions(Partitions::create(PartitionsShuffleKind::Seq, partitions))?;
                Self::build_read_stage_source(ctx.clone(), pipeline, &info.stage_info)?;

                let stage_table_info = StageTableInfo {
                    stage_root: "".to_string(),
                    stage_info: info.stage_info.clone(),
                    schema: Arc::new(Default::default()),
                    default_exprs: None,
                    files_info: info.files_info.clone(),
                    files_to_copy: None,
                    duplicated_files_detected: vec![],
                    is_select: false,
                    copy_into_table_options: Default::default(),
                    is_variant: false,
                };

                let load_ctx = Arc::new(LoadContext::try_create_for_copy(
                    ctx.clone(),
                    &stage_table_info,
                    None,
                    BlockThresholds::default(),
                    vec![],
                )?);

                let mut algo = None;

                for file_info in info.stage_file_infos.iter() {
                    let Some(new_algo) = CompressAlgorithm::from_path(&file_info.path) else {
                        continue;
                    };

                    if let Some(algo) = algo {
                        if algo != new_algo {
                            return Err(ErrorCode::UnknownCompressionType(
                                "`infer_schema` only supports single compression type",
                            ));
                        }
                    }
                    algo = Some(new_algo);
                }
                if algo.is_some() {
                    pipeline.try_add_accumulating_transformer(|| {
                        Decompressor::try_create(load_ctx.clone(), algo)
                    })?;
                }
                pipeline.add_accumulating_transformer(|| {
                    InferSchemaSeparator::create(
                        info.file_format_params.clone(),
                        self.args_parsed.max_records,
                        info.stage_file_infos.len(),
                    )
                });
            }
            FileFormatParams::Parquet(_) => {
                pipeline.add_source(
                    |output| {
                        ParquetInferSchemaSource::create(
                            ctx.clone(),
                            output,
                            info.stage_info.clone(),
                            info.stage_file_infos.clone(),
                        )
                    },
                    1,
                )?;
            }
            _ => {
                return Err(ErrorCode::BadArguments(
                    "infer_schema is currently limited to format Parquet, CSV and NDJSON",
                ));
            }
        }

        Ok(())
    }
}

impl TableFunction for InferSchemaTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}
