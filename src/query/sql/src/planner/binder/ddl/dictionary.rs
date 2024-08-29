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
use std::collections::HashSet;

use databend_common_ast::ast::CreateDictionaryStmt;
use databend_common_ast::ast::DropDictionaryStmt;
use databend_common_ast::ast::ShowCreateDictionaryStmt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::TableDataType;
use databend_common_meta_app::schema::DictionaryMeta;
use itertools::Itertools;

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
            database_id = db.get_db_info().database_id.db_id;
        }

        let source = self.normalize_object_identifier(source_name);

        if source.to_lowercase() != *"mysql" && source.to_lowercase() != *"redis" {
            return Err(ErrorCode::UnsupportedDictionarySource(format!(
                "The specified source '{}' is not currently supported",
                source.to_lowercase(),
            )))
        }

        let options: BTreeMap<String, String> = source_options
            .iter()
            .map(|(k, v)| (k.to_lowercase(), v.to_string().to_lowercase()))
            .collect();
        if source.to_lowercase() == *"mysql" {
            let required_options = ["host", "port", "username", "password", "db"];
            for option in required_options {
                if !options.contains_key(option) {
                    return Err(ErrorCode::MissingDictionaryOption(
                        "The mysql configuration is missing one or more required options. ".to_owned()
                            + "Please ensure you have provided values for 'host', 'port', 'username', 'password', and 'db'.",
                    ));
                }
            }
            if required_options.len() != options.len() {
                return Err(ErrorCode::UnsupportedDictionaryOption(format!(
                    "Some provided options are not recognized."
                )));
            }
            let port = options.get("port")?;
            if port.parse<u64>().is_err() {
                return Err(ErrorCode::UnsupportedDictionaryOption(format!(
                    "The value of `port` must be UInt"
                )))
            }
        } else if source.to_lowercase() == *"redis" {
            let required_options = ["host", "port"];
            for option in required_options {
                if !options.contains_key(option) {
                    return Err(ErrorCode::MissingDictionaryOption(
                        "The redis configuration is missing one or more required options. ".to_owned()
                            + "Please ensure you have provided values for 'host' and 'port'.",
                    ));
                }
            }
            if let Some(db_index) = options.get("db_index") {
                let db_index = db_index.parse::<u64>().unwrap();
                if db_index < 0 || db_index > 15 {
                    return Err(ErrorCode::UnsupportedDictionaryOption(format!(
                        "The value of `db_index` must be between [0,15]"
                    )))
                }
            } else {
                options.insert("db_index".to_string(), 0);
            }
            if None == options.get("password") {
                options.insert("password".to_string(), String::new())
            }
            let allowed_options = HashSet::from(["host", "port", "password", "db_index"]);
            let keys = HashSet::new();
            for key in options.keys().cloned().collect_vec() {
                keys.insert(key.as_str())
            }
            if !keys.is_subset(&allowed_options) {
                return Err(ErrorCode::UnsupportedDictionaryOption(format!(
                    "The redis configurations must be in [`host`, `port`, `password`, `db_index`] ",
                )))
            }
        } else {
            todo!()
        }

        let mut field_comments = BTreeMap::new();
        let mut primary_column_ids = Vec::new();

        let (schema, _) = self.analyze_create_table_schema_by_columns(columns).await?;

        let mut fields_names: Vec<String> = Vec::new();
        for table_field in schema.fields() {
            if table_field.computed_expr.is_some() {
                return Err(ErrorCode::WrongDictionaryFieldExpr(
                    "The table field configuration is invalid. ".to_owned()
                        + "Computed expressions for the table fields should not be set",
                ));
            }
            fields_names.push(table_field.name.clone());
        }
        // Check for redis.
        if source.to_lowercase() == *"redis" {
            if fields_names.len() != 2 {
                return Err(ErrorCode::WrongDictionaryFieldExpr(
                    "The number of Redis fields must be two",
                ));
            }
            for table_field in schema.fields() {
                if *table_field.data_type() != TableDataType::String {
                    return Err(ErrorCode::WrongDictionaryFieldExpr(
                        "The type of Redis field must be string",
                    ));
                }
            }
        // Check for mysql
        } else if source.to_lowercase() == *"mysql" {
            for table_field in schema.fields() {
                let field_type = table_field.data_type;
                if !(field_type == TableDataType::Boolean || field_type == TableDataType::String ||
                    field_type == TableDataType::Number(()) || field_type == TableDataType::Timestamp
                    || field_type == TableDataType::Date) {
                    return Err(ErrorCode::WrongDictionaryFieldExpr(format!(
                        "Mysql field types must be in [`boolean`, `string`, `number`, `timestamp`, `date`]",
                    )))
                }
            }
        } else {
            todo!()
        }
        for column in columns {
            if column.comment.is_some() {
                let column_id = schema.column_id_of(column.name.name.as_str())?;
                field_comments.insert(column_id, column.comment.clone().unwrap_or_default());
            }
        }
        if primary_keys.len() != 1 {
            return Err(ErrorCode::WrongPKNumber("Only support one primary key"))
        }
        let primary_key = primary_keys.get(0)?;
        let pk_id = schema.column_id_of(primary_key.name.as_str())?;
        primary_column_ids.push(pk_id);

        let comment = comment.clone().unwrap_or("".to_string());
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
            create_option: create_option.clone(),
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
            if_exists,
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
            database_id = db.get_db_info().database_id.db_id;
        }
        Ok(Plan::DropDictionary(Box::new(DropDictionaryPlan {
            if_exists: *if_exists,
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
            database_id = db.get_db_info().database_id.db_id;
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
