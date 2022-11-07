// Copyright 2022 Datafuse Labs.
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
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::ToDataType;
use common_datavalues::Vu8;
use common_exception::Result;
use common_meta_app::schema::CatalogMeta;
use common_meta_app::schema::CatalogType;

use crate::normalize_identifier;
use crate::plans::CreateCatalogPlan;
use crate::plans::DropCatalogPlan;
use crate::plans::Plan;
use crate::plans::RewriteKind;
use crate::plans::ShowCreateCatalogPlan;
use crate::BindContext;
use crate::Binder;

impl<'a> Binder {
    pub(in crate::planner::binder) async fn bind_show_catalogs(
        &mut self,
        bind_context: &BindContext,
        stmt: &ShowCatalogsStmt<'a>,
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

    pub(in crate::planner::binder) async fn bind_show_create_catalogs(
        &self,
        stmt: &ShowCreateCatalogStmt<'_>,
    ) -> Result<Plan> {
        let ShowCreateCatalogStmt { catalog } = stmt;
        let catalog = normalize_identifier(catalog, &self.name_resolution_ctx).name;
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("Catalog", Vu8::to_data_type()),
            DataField::new("Create Catalog", Vu8::to_data_type()),
        ]);
        Ok(Plan::ShowCreateCatalog(Box::new(ShowCreateCatalogPlan {
            catalog,
            schema,
        })))
    }

    pub(in crate::planner::binder) async fn bind_create_catalog(
        &self,
        stmt: &CreateCatalogStmt,
    ) -> Result<Plan> {
        let CreateCatalogStmt {
            if_not_exists,
            catalog_name: catalog,
            catalog_type,
            options,
        } = stmt;

        let tenant = self.ctx.get_tenant();

        let meta = self.catalog_meta(*catalog_type, options);

        Ok(Plan::CreateCatalog(Box::new(CreateCatalogPlan {
            if_not_exists: *if_not_exists,
            tenant,
            catalog: catalog.to_string(),
            meta,
        })))
    }

    pub(in crate::planner::binder) async fn bind_drop_catalog(
        &self,
        stmt: &DropCatalogStmt<'_>,
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

    fn catalog_meta(
        &self,
        catalog_type: CatalogType,
        options: &BTreeMap<String, String>,
    ) -> CatalogMeta {
        let options = options.clone();
        CatalogMeta {
            catalog_type,
            options,
            created_on: Utc::now(),
        }
    }
}
