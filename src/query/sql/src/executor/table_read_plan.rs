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
use std::sync::Arc;

use common_ast::parser::parse_expr;
use common_ast::parser::tokenize_sql;
use common_ast::Dialect;
use common_base::base::ProgressValues;
use common_catalog::plan::DataSourceInfo;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::InternalColumn;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::FieldIndex;
use common_expression::RemoteExpr;
use common_expression::Scalar;
use common_expression::TableField;
use common_license::license::Feature::DataMask;
use common_license::license_manager::get_license_manager;
use common_settings::Settings;
use common_users::UserApiProvider;
use data_mask_feature::get_datamask_handler;
use log::info;
use parking_lot::RwLock;

use crate::binder::ColumnBindingBuilder;
use crate::plans::BoundColumnRef;
use crate::resolve_type_name_by_str;
use crate::BindContext;
use crate::Metadata;
use crate::NameResolutionContext;
use crate::ScalarExpr;
use crate::TypeChecker;
use crate::Visibility;

#[async_trait::async_trait]
pub trait ToReadDataSourcePlan {
    /// Real read_plan to access partitions/push_downs
    #[async_backtrace::framed]
    async fn read_plan(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        dry_run: bool,
    ) -> Result<DataSourcePlan> {
        self.read_plan_with_catalog(ctx, "default".to_owned(), push_downs, None, dry_run)
            .await
    }

    async fn read_plan_with_catalog(
        &self,
        ctx: Arc<dyn TableContext>,
        catalog: String,
        push_downs: Option<PushDownInfo>,
        internal_columns: Option<BTreeMap<FieldIndex, InternalColumn>>,
        dry_run: bool,
    ) -> Result<DataSourcePlan>;
}

#[async_trait::async_trait]
impl ToReadDataSourcePlan for dyn Table {
    #[async_backtrace::framed]
    async fn read_plan_with_catalog(
        &self,
        ctx: Arc<dyn TableContext>,
        catalog: String,
        push_downs: Option<PushDownInfo>,
        internal_columns: Option<BTreeMap<FieldIndex, InternalColumn>>,
        dry_run: bool,
    ) -> Result<DataSourcePlan> {
        let catalog_info = ctx.get_catalog(&catalog).await?.info();

        let (statistics, parts) = if let Some(PushDownInfo {
            filter:
                Some(RemoteExpr::Constant {
                    scalar: Scalar::Boolean(false),
                    ..
                }),
            ..
        }) = &push_downs
        {
            Ok((PartStatistics::default(), Partitions::default()))
        } else {
            ctx.set_status_info("build physical plan - read partitions");
            self.read_partitions(ctx.clone(), push_downs.clone(), dry_run)
                .await
        }?;

        ctx.incr_total_scan_value(ProgressValues {
            rows: statistics.read_rows,
            bytes: statistics.read_bytes,
        });

        // We need the partition sha256 to specify the result cache.
        if ctx.get_settings().get_enable_query_result_cache()? {
            let sha = parts.compute_sha256()?;
            ctx.add_partitions_sha(sha);
        }

        let source_info = self.get_data_source_info();

        let schema = &source_info.schema();
        let description = statistics.get_description(&source_info.desc());
        let mut output_schema = match (self.support_column_projection(), &push_downs) {
            (true, Some(push_downs)) => match &push_downs.prewhere {
                Some(prewhere) => Arc::new(prewhere.output_columns.project_schema(schema)),
                _ => {
                    if let Some(output_columns) = &push_downs.output_columns {
                        Arc::new(output_columns.project_schema(schema))
                    } else if let Some(projection) = &push_downs.projection {
                        Arc::new(projection.project_schema(schema))
                    } else {
                        schema.clone()
                    }
                }
            },
            _ => schema.clone(),
        };

        if let Some(ref push_downs) = push_downs {
            if let Some(ref virtual_columns) = push_downs.virtual_columns {
                let mut schema = output_schema.as_ref().clone();
                let fields = virtual_columns
                    .iter()
                    .map(|c| TableField::new(&c.name, *c.data_type.clone()))
                    .collect::<Vec<_>>();
                schema.add_columns(&fields)?;
                output_schema = Arc::new(schema);
            }
        }

        if let Some(ref internal_columns) = internal_columns {
            let mut schema = output_schema.as_ref().clone();
            for internal_column in internal_columns.values() {
                schema.add_internal_field(
                    internal_column.column_name(),
                    internal_column.table_data_type(),
                    internal_column.column_id(),
                );
            }
            output_schema = Arc::new(schema);
        }

        // check if need to apply data mask policy
        let data_mask_policy = if let DataSourceInfo::TableSource(table_info) = &source_info {
            let table_meta = &table_info.meta;
            let tenant = ctx.get_tenant();

            if let Some(column_mask_policy) = &table_meta.column_mask_policy {
                let license_manager = get_license_manager();
                let ret = license_manager.manager.check_enterprise_enabled(
                    &ctx.get_settings(),
                    tenant.clone(),
                    DataMask,
                );
                if ret.is_err() {
                    None
                } else {
                    let mut mask_policy_map = BTreeMap::new();
                    let meta_api = UserApiProvider::instance().get_meta_store_client();
                    let handler = get_datamask_handler();
                    for (i, field) in output_schema.fields().iter().enumerate() {
                        if let Some(mask_policy) = column_mask_policy.get(field.name()) {
                            if let Ok(policy) = handler
                                .get_data_mask(
                                    meta_api.clone(),
                                    tenant.clone(),
                                    mask_policy.clone(),
                                )
                                .await
                            {
                                let args = &policy.args;
                                let mut aliases = Vec::with_capacity(args.len());
                                for (i, (arg_name, arg_type)) in args.iter().enumerate() {
                                    let table_data_type =
                                        resolve_type_name_by_str(arg_type.as_str())?;
                                    let data_type = (&table_data_type).into();
                                    let bound_column = BoundColumnRef {
                                        span: None,
                                        column: ColumnBindingBuilder::new(
                                            arg_name.to_string(),
                                            i,
                                            Box::new(data_type),
                                            Visibility::Visible,
                                        )
                                        .build(),
                                    };
                                    let scalar_expr = ScalarExpr::BoundColumnRef(bound_column);
                                    aliases.push((arg_name.clone(), scalar_expr));
                                }

                                let body = &policy.body;
                                let tokens = tokenize_sql(body)?;
                                let ast_expr = parse_expr(&tokens, Dialect::PostgreSQL)?;
                                let mut bind_context = BindContext::new();
                                let settings = Settings::create("".to_string());
                                let name_resolution_ctx =
                                    NameResolutionContext::try_from(settings.as_ref())?;
                                let metadata = Arc::new(RwLock::new(Metadata::default()));
                                let mut type_checker = TypeChecker::new(
                                    &mut bind_context,
                                    ctx.clone(),
                                    &name_resolution_ctx,
                                    metadata,
                                    &aliases,
                                    false,
                                    false,
                                );

                                let scalar = type_checker.resolve(&ast_expr).await?;
                                let expr = scalar.0.as_expr()?.project_column_ref(|col| col.index);
                                mask_policy_map.insert(i, expr.as_remote_expr());
                            } else {
                                info!("cannot find mask policy {}/{}", tenant, mask_policy);
                            }
                        }
                    }
                    Some(mask_policy_map)
                }
            } else {
                None
            }
        } else {
            None
        };
        // TODO pass in catalog name

        Ok(DataSourcePlan {
            catalog_info,
            source_info,
            output_schema,
            parts,
            statistics,
            description,
            tbl_args: self.table_args(),
            push_downs,
            query_internal_columns: internal_columns.is_some(),
            data_mask_policy,
        })
    }
}
