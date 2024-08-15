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

use databend_common_ast::ast::CreateDictionaryStmt;
use databend_common_ast::ast::DropDictionaryStmt;
use databend_common_ast::ast::ShowCreateDictionaryStmt;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use databend_common_meta_app::schema::DictionaryMeta;

use crate::plans::CreateDictionaryPlan;
use crate::plans::DropDictionaryPlan;
use crate::plans::Plan;
use crate::plans::ShowCreateDictionaryPlan;
use crate::Binder;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_dictionary(
        &mut self,
        stmt: &CreateDictionaryStmt,
    ) -> Result<Plan> {
        let CreateDictionaryStmt {
            create_option,
            catalog,
            database,
            dictionary_name,
            columns,
            primary_keys,
            source_name,
            source_options,
            comment,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let (catalog, database, dictionary_name) =
            self.normalize_object_identifier_triple(catalog, database, dictionary_name);

        let database_id;
        {
            let catalog = self.ctx.get_catalog(&catalog).await?;
            let db = catalog.get_database(&tenant, &database).await?;
            database_id = db.get_db_info().ident.db_id;
        }

        let source = self.normalize_object_identifier(source_name);
        let options: BTreeMap<String, String> = source_options
            .into_iter()
            .map(|(k, v)| (k.to_lowercase(), v.to_string().to_lowercase()))
            .collect();

        let mut field_comments = BTreeMap::new();
        for (index, column) in columns.iter().enumerate() {
            field_comments.insert(index as u32, column.comment.clone().unwrap_or_default());
        }

        let mut primary_column_ids = Vec::new();
        for (index, column) in columns.into_iter().enumerate() {
            if primary_keys.iter().any(|pk| pk.name == column.name.name) {
                primary_column_ids.push(index as u32);
            }
        }

        let comment = comment.clone().unwrap_or("".to_string());
        let (schema, _) = self.analyze_create_table_schema_by_columns(columns).await?;

        let meta = DictionaryMeta {
            source,
            options,
            schema,
            field_comments,
            primary_column_ids,
            comment,
            ..Default::default()
        };

        Ok(Plan::CreateDictionary(Box::new(CreateDictionaryPlan {
            create_option: create_option.clone().into(),
            tenant,
            catalog,
            database_id,
            dictionary: dictionary_name,
            meta,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_dictionary(
        &mut self,
        stmt: &DropDictionaryStmt,
    ) -> Result<Plan> {
        let DropDictionaryStmt {
            if_exists: _,
            catalog,
            database,
            dictionary_name,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let (catalog, database, dictionary_name) =
            self.normalize_object_identifier_triple(catalog, database, dictionary_name);

        let database_id;
        {
            let catalog = self.ctx.get_catalog(&catalog).await?;
            let db = catalog.get_database(&tenant, &database).await?;
            database_id = db.get_db_info().ident.db_id;
        }
        Ok(Plan::DropDictionary(Box::new(DropDictionaryPlan {
            tenant,
            catalog,
            database_id,
            dictionary: dictionary_name,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_create_dictionary(
        &mut self,
        stmt: &ShowCreateDictionaryStmt,
    ) -> Result<Plan> {
        let ShowCreateDictionaryStmt {
            catalog,
            database,
            dictionary_name,
        } = stmt;

        let (catalog, database, dictionary_name) =
            self.normalize_object_identifier_triple(catalog, database, dictionary_name);

        let schema = DataSchemaRefExt::create(vec![
            DataField::new("Dictionary", DataType::String),
            DataField::new("Create Dictionary", DataType::String),
        ]);

        let database_id;
        {
            let tenant = self.ctx.get_tenant();
            let catalog = self.ctx.get_catalog(&catalog).await?;
            let db = catalog.get_database(&tenant, &database).await?;
            database_id = db.get_db_info().ident.db_id;
        }

        Ok(Plan::ShowCreateDictionary(Box::new(
            ShowCreateDictionaryPlan {
                catalog,
                database_id,
                dictionary: dictionary_name,
                schema,
            },
        )))
    }
}
