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
use std::sync::Arc;

use databend_common_ast::Span;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_catalog::table_function::TableFunction;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::cast_scalar;
use databend_common_expression::infer_schema_type;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::principal::StageType;
use databend_common_meta_app::principal::UDTFServer;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::storage::StorageParams;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::sources::AsyncSourcer;
use databend_common_sql::StageLocationParam;
use databend_common_sql::binder::resolve_stage_location;

use crate::pipelines::builders::UdtfFunctionDesc;
use crate::pipelines::builders::UdtfServerSource;

pub struct UDTFTable {
    desc: UdtfFunctionDesc,
    table_info: TableInfo,
}

impl UDTFTable {
    pub fn create(
        ctx: &dyn TableContext,
        database_name: &str,
        table_func_name: &str,
        table_args: &TableArgs,
        mut udtf: UDTFServer,
    ) -> Result<Arc<dyn TableFunction>> {
        let schema = Self::schema(&udtf)?;

        let table_info = TableInfo {
            ident: databend_common_meta_app::schema::TableIdent::new(0, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: table_func_name.to_string(),
            meta: databend_common_meta_app::schema::TableMeta {
                schema: schema.clone(),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut stage_locations = Vec::new();
        for (i, (argument, dest_type)) in table_args
            .positioned
            .iter()
            .zip(udtf.arg_types.iter())
            .enumerate()
        {
            if dest_type.remove_nullable() == DataType::StageLocation {
                let Some(location) = argument.as_string() else {
                    return Err(ErrorCode::SemanticError(format!(
                        "invalid parameter {argument} for udf function, expected constant string",
                    )));
                };
                let (stage_info, relative_path) =
                    databend_common_base::runtime::block_on(resolve_stage_location(ctx, location))?;

                if !matches!(stage_info.stage_type, StageType::External) {
                    return Err(ErrorCode::SemanticError(format!(
                        "stage {} type is {}, UDF only support External Stage",
                        stage_info.stage_name, stage_info.stage_type,
                    )));
                }
                if let StorageParams::S3(config) = &stage_info.stage_params.storage {
                    if !config.security_token.is_empty() || !config.role_arn.is_empty() {
                        return Err(ErrorCode::SemanticError(format!(
                            "StageLocation: @{} must use a separate credential",
                            location
                        )));
                    }
                }

                stage_locations.push(StageLocationParam {
                    param_name: udtf.arg_names[i].clone(),
                    relative_path,
                    stage_info,
                });
            }
        }
        if !stage_locations.is_empty() {
            let stage_location_value = serde_json::to_string(&stage_locations)?;
            udtf.headers
                .insert("databend-stage-mapping".to_string(), stage_location_value);
        }

        if table_args.positioned.len() != udtf.arg_types.len() {
            return Err(ErrorCode::SyntaxException(format!(
                "UDTF '{}' argument types length {} does not match input arguments length {}",
                table_func_name,
                udtf.arg_types.len(),
                table_args.positioned.len()
            )));
        }
        let args = table_args
            .positioned
            .iter()
            .cloned()
            .zip(udtf.arg_types)
            .map(|(scalar, ty)| {
                cast_scalar(Span::None, scalar, &ty, &BUILTIN_FUNCTIONS).map(|scalar| (scalar, ty))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Arc::new(Self {
            desc: UdtfFunctionDesc {
                name: table_func_name.to_string(),
                func_name: udtf.handler,
                return_ty: DataType::Tuple(
                    udtf.return_types.into_iter().map(|(_, ty)| ty).collect(),
                ),
                args,
                headers: udtf.headers,
                server: udtf.address,
            },
            table_info,
        }))
    }

    fn schema(udtf: &UDTFServer) -> Result<Arc<TableSchema>> {
        let fields = udtf
            .return_types
            .iter()
            .map(|(name, ty)| infer_schema_type(ty).map(|ty| TableField::new(name.as_str(), ty)))
            .collect::<Result<Vec<_>>>()?;

        Ok(TableSchemaRefExt::create(fields))
    }
}

impl TableFunction for UDTFTable {
    fn function_name(&self) -> &str {
        self.desc.name.as_str()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

#[async_trait::async_trait]
impl Table for UDTFTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        Ok((PartStatistics::default(), Partitions::default()))
    }

    fn table_args(&self) -> Option<TableArgs> {
        let scalars = self
            .desc
            .args
            .iter()
            .map(|(scalar, _)| scalar)
            .cloned()
            .collect();

        Some(TableArgs::new_positioned(scalars))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        let semaphore = UdtfServerSource::init_semaphore(ctx.clone())?;
        let endpoints = UdtfServerSource::init_endpoints(ctx.clone(), &self.desc)?;
        pipeline.add_source(
            |output| {
                let inner = UdtfServerSource::new(
                    ctx.clone(),
                    self.desc.clone(),
                    semaphore.clone(),
                    endpoints.clone(),
                )?;
                AsyncSourcer::create(ctx.get_scan_progress(), output, inner)
            },
            1,
        )?;

        Ok(())
    }
}
