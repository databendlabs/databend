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
use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;

use chrono::Utc;
use databend_common_ast::ast::CreateCatalogStmt;
use databend_common_ast::ast::DropCatalogStmt;
use databend_common_ast::ast::ShowCatalogsStmt;
use databend_common_ast::ast::ShowCreateCatalogStmt;
use databend_common_ast::ast::ShowLimit;
use databend_common_ast::ast::UriLocation;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use databend_common_meta_app::schema::CatalogMeta;
use databend_common_meta_app::schema::CatalogOption;
use databend_common_meta_app::schema::CatalogType;
use databend_common_meta_app::schema::HiveCatalogOption;
use databend_common_meta_app::schema::IcebergCatalogOption;
use databend_common_meta_app::schema::IcebergHmsCatalogOption;
use databend_common_meta_app::schema::IcebergRestCatalogOption;
use databend_common_meta_app::schema::ShareCatalogOption;
use databend_common_meta_app::storage::StorageParams;

use crate::binder::parse_storage_params_from_uri;
use crate::normalize_identifier;
use crate::plans::CreateCatalogPlan;
use crate::plans::DropCatalogPlan;
use crate::plans::Plan;
use crate::plans::RewriteKind;
use crate::plans::ShowCreateCatalogPlan;
use crate::BindContext;
use crate::Binder;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_catalogs(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &ShowCatalogsStmt,
    ) -> Result<Plan> {
        let ShowCatalogsStmt { limit } = stmt;
        let mut query = String::new();
        write!(query, "SELECT name AS Catalogs FROM system.catalogs").unwrap();
        match limit {
            Some(ShowLimit::Like { pattern }) => {
                write!(query, " WHERE name LIKE '{pattern}'").unwrap();
            }
            Some(ShowLimit::Where { selection }) => {
                write!(query, " WHERE {selection}").unwrap();
            }
            None => (),
        }
        write!(query, " ORDER BY name").unwrap();

        self.bind_rewrite_to_query(bind_context, query.as_str(), RewriteKind::ShowCatalogs)
            .await
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_create_catalogs(
        &self,
        stmt: &ShowCreateCatalogStmt,
    ) -> Result<Plan> {
        let ShowCreateCatalogStmt { catalog } = stmt;
        let catalog = normalize_identifier(catalog, &self.name_resolution_ctx).name;
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("Catalog", DataType::String),
            DataField::new("Type", DataType::String),
            DataField::new("Option", DataType::String),
        ]);
        Ok(Plan::ShowCreateCatalog(Box::new(ShowCreateCatalogPlan {
            catalog,
            schema,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_catalog(
        &self,
        stmt: &CreateCatalogStmt,
    ) -> Result<Plan> {
        let CreateCatalogStmt {
            if_not_exists,
            catalog_name: catalog,
            catalog_type,
            catalog_options: options,
        } = stmt;

        let tenant = self.ctx.get_tenant();

        let meta = self
            .try_create_meta_from_options(&self.ctx, catalog_type.clone().into(), options)
            .await?;

        Ok(Plan::CreateCatalog(Box::new(CreateCatalogPlan {
            if_not_exists: *if_not_exists,
            tenant,
            catalog: catalog.to_string(),
            meta,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_catalog(
        &self,
        stmt: &DropCatalogStmt,
    ) -> Result<Plan> {
        let DropCatalogStmt { if_exists, catalog } = stmt;
        let tenant = self.ctx.get_tenant();
        let catalog = normalize_identifier(catalog, &self.name_resolution_ctx).name;
        Ok(Plan::DropCatalog(Box::new(DropCatalogPlan {
            if_exists: *if_exists,
            tenant,
            catalog,
        })))
    }

    async fn try_create_meta_from_options(
        &self,
        ctx: &Arc<dyn TableContext>,
        catalog_type: CatalogType,
        options: &BTreeMap<String, String>,
    ) -> Result<CatalogMeta> {
        // get catalog options from options
        let catalog_option = match catalog_type {
            // creating default catalog type is not supported
            CatalogType::Default => {
                return Err(ErrorCode::CatalogNotSupported(
                    "Creating default catalog is not allowed!",
                ));
            }
            CatalogType::Hive => {
                let mut options = options.clone();

                // Remove address and url to avoid unexpected field error in uri location.
                let address = options.remove("metastore_address").ok_or_else(|| {
                    ErrorCode::InvalidArgument("expected field: METASTORE_ADDRESS")
                })?;

                let sp = parse_hive_catalog_url(ctx, options).await?;

                CatalogOption::Hive(HiveCatalogOption {
                    address,
                    storage_params: sp.map(Box::new),
                })
            }
            CatalogType::Iceberg => {
                let opt = parse_iceberg_rest_catalog(options.clone())?;
                CatalogOption::Iceberg(opt)
            }
            CatalogType::Share => {
                let opt = parse_share_catalog(options.clone())?;
                CatalogOption::Share(opt)
            }
        };

        Ok(CatalogMeta {
            catalog_option,
            created_on: Utc::now(),
        })
    }
}

async fn parse_hive_catalog_url(
    ctx: &Arc<dyn TableContext>,
    options: BTreeMap<String, String>,
) -> Result<Option<StorageParams>> {
    // Make sure options has been lower cases.
    let mut options = options
        .into_iter()
        .map(|(k, v)| (k.to_lowercase(), v))
        .collect::<BTreeMap<_, _>>();

    // has to be removed, or UriLocation will complain about unknown field
    let uri = if let Some(v) = options.remove("url") {
        v
    } else {
        return Ok(None);
    };

    let mut location = UriLocation::from_uri(uri, "".to_string(), options)?;
    let sp = parse_storage_params_from_uri(
        &mut location,
        Some(ctx.as_ref()),
        "when create Hive Catalog",
    )
    .await?;

    Ok(Some(sp))
}

fn parse_iceberg_rest_catalog(
    mut options: BTreeMap<String, String>,
) -> Result<IcebergCatalogOption> {
    let typ = options
        .remove("type")
        .ok_or_else(|| ErrorCode::InvalidArgument("type for iceberg catalog is not specified"))?
        .to_lowercase();

    let address = options
        .remove("address")
        .ok_or_else(|| ErrorCode::InvalidArgument("address for iceberg catalog is not specified"))?
        .to_string();

    let warehouse = options
        .remove("warehouse")
        .ok_or_else(|| {
            ErrorCode::InvalidArgument("warehouse for iceberg catalog is not specified")
        })?
        .to_string();

    let option = match typ.as_str() {
        "rest" => IcebergCatalogOption::Rest(IcebergRestCatalogOption {
            uri: address,
            warehouse,
            props: HashMap::from_iter(options),
        }),
        "hive" => IcebergCatalogOption::Hms(IcebergHmsCatalogOption {
            address,
            warehouse,
            props: HashMap::from_iter(options),
        }),
        v => {
            return Err(ErrorCode::InvalidArgument(format!(
                "iceberg catalog with type {v} is not supported"
            )));
        }
    };

    Ok(option)
}

fn parse_share_catalog(mut options: BTreeMap<String, String>) -> Result<ShareCatalogOption> {
    let input = options
        .remove("name")
        .ok_or_else(|| ErrorCode::InvalidArgument("share name for share catalog is not specified"))?
        .to_lowercase();
    let tenant_share: Vec<String> = input.split('.').map(|s| s.to_string()).collect();
    if tenant_share.len() != 2 {
        return Err(ErrorCode::InvalidArgument(
            "invalid share name for share catalog",
        ));
    }

    let share_endpoint = options
        .remove("endpoint")
        .ok_or_else(|| {
            ErrorCode::InvalidArgument("share endpoint name for share catalog is not specified")
        })?
        .to_lowercase();

    Ok(ShareCatalogOption {
        provider: tenant_share[0].clone(),
        share_name: tenant_share[1].clone(),
        share_endpoint,
    })
}
