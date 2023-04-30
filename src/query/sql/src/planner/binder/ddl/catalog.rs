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
use std::fmt::Write;

use chrono::Utc;
use common_ast::ast::CreateCatalogStmt;
use common_ast::ast::DropCatalogStmt;
use common_ast::ast::ShowCatalogsStmt;
use common_ast::ast::ShowCreateCatalogStmt;
use common_ast::ast::ShowLimit;
use common_ast::ast::UriLocation;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::DataField;
use common_expression::DataSchemaRefExt;
use common_meta_app::schema::CatalogMeta;
use common_meta_app::schema::CatalogOption;
use common_meta_app::schema::CatalogType;
use common_meta_app::schema::IcebergCatalogOption;
use url::Url;

use crate::binder::parse_uri_location;
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
            DataField::new("Create Catalog", DataType::String),
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

        let meta = self.try_create_meta_from_options(*catalog_type, options)?;

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

    fn try_create_meta_from_options(
        &self,
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
                if !cfg!(feature = "hive") {
                    return Err(ErrorCode::CatalogNotSupported(
                        "Hive catalog support is not enabled in your databend-query distribution."
                            .to_string(),
                    ));
                }
                let address = options
                    .get("address")
                    .ok_or_else(|| ErrorCode::InvalidArgument("expected field: ADDRESS"))?;

                CatalogOption::Hive(address.to_string())
            }
            CatalogType::Iceberg => {
                let mut catalog_options = options.clone();

                // getting other options to create this catalog
                let flatten = matches!(
                    catalog_options
                        .get("flatten")
                        .map(|v| v.to_lowercase())
                        .unwrap_or_default()
                        .as_str(),
                    "true" | "on"
                );

                // the uri should in the same schema as in stages
                let uri = catalog_options
                    .remove("url") // has to be removed, or UriLocation will complain about unknown field.
                    .ok_or_else(|| ErrorCode::InvalidArgument("expected field: URL"))?;

                // create a uri location
                let mut location = if let Some(path) = uri.strip_prefix("fs://") {
                    UriLocation::new(
                        "fs".to_string(),
                        "".to_string(),
                        path.to_string(),
                        "".to_string(),
                        catalog_options,
                    )
                } else {
                    let parsed = Url::parse(&uri).map_err(|err| {
                        ErrorCode::InvalidArgument(format!("expected valid URL: {:?}", err))
                    })?;
                    let name = parsed
                        .host_str()
                        .map(|hostname| {
                            if let Some(port) = parsed.port() {
                                format!("{}:{}", hostname, port)
                            } else {
                                hostname.to_string()
                            }
                        })
                        .ok_or_else(|| {
                            ErrorCode::InvalidArgument("expected valid URI: no hostname section")
                        })?;

                    let path = if parsed.path().is_empty() {
                        "/".to_string()
                    } else {
                        parsed.path().to_string()
                    };

                    UriLocation::new(
                        parsed.scheme().to_string(),
                        name,
                        path,
                        "".to_string(),
                        catalog_options,
                    )
                };

                let (sp, _) = parse_uri_location(&mut location)?;

                let opt = IcebergCatalogOption {
                    storage_params: Box::new(sp),
                    flatten,
                };
                CatalogOption::Iceberg(opt)
            }
        };

        Ok(CatalogMeta {
            catalog_option,
            created_on: Utc::now(),
        })
    }
}
