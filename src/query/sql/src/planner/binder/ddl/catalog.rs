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

use common_ast::ast::ShowCatalogsStmt;
use common_ast::ast::ShowCreateCatalogStmt;
use common_ast::ast::ShowLimit;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_exception::Result;

use crate::sql::plans::Plan;
use crate::sql::plans::RewriteKind;
use crate::sql::BindContext;
use crate::sql::Binder;

impl<'a> Binder {
    pub(in crate::sql::planner::binder) async fn bind_show_catalogs(
        &mut self,
        bind_context: &BindContext,
        stmt: &ShowCatalogsStmt<'a>,
    ) -> Result<Plan> {
        let ShowCatalogsStmt { limit } = stmt;
        let mut query = String::new();
        write!(query, "SELECT name AS Catalog FROM system.catalogs").unwrap();
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

    pub(in crate::sql::planner::binder) async fn bind_show_create_catalogs(
        &self,
        stmt: &ShowCreateCatalogStmt,
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
}
