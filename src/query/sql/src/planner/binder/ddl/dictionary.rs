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

use std::borrow::BorrowMut;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::LazyLock;

use databend_common_ast::ast::CreateDictionaryStmt;
use databend_common_ast::ast::DropDictionaryStmt;
use databend_common_ast::ast::ShowCreateDictionaryStmt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_meta_app::schema::DictionaryMeta;
use itertools::Itertools;

use crate::plans::CreateDictionaryPlan;
use crate::plans::DropDictionaryPlan;
use crate::plans::Plan;
use crate::plans::ShowCreateDictionaryPlan;
use crate::Binder;

pub static REQUIRED_MYSQL_OPTION_KEYS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut r = HashSet::new();
    r.insert("host");
    r.insert("port");
    r.insert("username");
    r.insert("db");
    r.insert("table");
    r
});

fn validate_mysql_opt_key(options: &BTreeMap<String, String>) -> Result<()> {
    let mut flag = true;
    for option in REQUIRED_MYSQL_OPTION_KEYS.clone() {
        if !options.contains_key(option) {
            flag = false
        }
    }
    if REQUIRED_MYSQL_OPTION_KEYS.len() != options.len() {
        flag = false
    }
    if let Some(port) = options.get("port") {
        if port.parse::<u64>().is_err() {
            flag = false
        }
    }
    if flag {
        Ok(())
    } else {
        Err(ErrorCode::BadArguments(
            "Please ensure you have provided correct values for [`host`, `port`, `username`, `password`, `db`, `table`]",
        ))
    }
}

pub static REQUIRED_REDIS_OPTION_KEYS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut r = HashSet::new();
    r.insert("host");
    r.insert("port");
    r
});

fn validate_redis_opt_key(options: &mut BTreeMap<String, String>) -> Result<()> {
    let mut flag = true;
    for option in REQUIRED_REDIS_OPTION_KEYS.clone() {
        if !options.contains_key(option) {
            flag = false
        }
    }

    if let Some(db_index) = options.get("db_index") {
        let db_index = db_index.parse::<u64>().unwrap();
        if db_index > 15 {
            flag = false
        }
    } else {
        options.insert("db_index".to_string(), 0.to_string());
    }

    if !options.contains_key("password") {
        options.insert("password".to_string(), String::new());
    }

    if let Some(port) = options.get("port") {
        if port.parse::<u64>().is_err() {
            flag = false
        }
    }

    let allowed_options = HashSet::from([
        "host".to_string(),
        "port".to_string(),
        "password".to_string(),
        "db_index".to_string(),
    ]);
    let mut keys = HashSet::new();
    for key in options.keys().cloned().collect_vec() {
        keys.insert(key);
    }
    if !keys.is_subset(&allowed_options) {
        flag = false
    }
    if flag {
        Ok(())
    } else {
        Err(ErrorCode::BadArguments(
            "Please ensure you have provided correct values which contains [`host`,`port`] and no other value than [`host`,`port`,`password`, `db_index`]",
        ))
    }
}

fn validate_mysql_fields(schema: &TableSchema) -> Result<()> {
    for field in schema.fields() {
        if !matches!(
            field.data_type().remove_nullable(),
            TableDataType::Boolean
                | TableDataType::String
                | TableDataType::Number(_)
                | TableDataType::Date
                | TableDataType::Timestamp
        ) {
            return Err(ErrorCode::BadArguments(
                "Mysql field types must be in [`boolean`, `string`, `number`, `timestamp`, `date`] and must be `NOT NULL`",
            ));
        }
    }
    Ok(())
}

fn validate_redis_fields(schema: &TableSchema) -> Result<()> {
    let fields_names: Vec<String> = schema.fields().iter().map(|f| f.name.clone()).collect();
    if fields_names.len() != 2 {
        return Err(ErrorCode::BadArguments(
            "The number of Redis fields must be two",
        ));
    }
    for field in schema.fields() {
        if field.data_type().remove_nullable() != TableDataType::String {
            return Err(ErrorCode::BadArguments(
                "The type of Redis field must be string",
            ));
        }
    }
    Ok(())
}

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
            return Err(ErrorCode::BadArguments(format!(
                "The specified source '{}' is not currently supported",
                source.to_lowercase(),
            )));
        }

        // Check for options.
        let mut options: BTreeMap<String, String> = source_options
            .iter()
            .map(|(k, v)| (k.to_lowercase(), v.to_string().to_lowercase()))
            .collect();
        match source.to_lowercase().as_str() {
            "redis" => validate_redis_opt_key(options.borrow_mut())?,
            "mysql" => validate_mysql_opt_key(options.borrow_mut())?,
            _ => todo!(),
        };

        // Check for data source fields.
        let (schema, _) = self.analyze_create_table_schema_by_columns(columns).await?;
        match source.to_lowercase().as_str() {
            "redis" => validate_redis_fields(&schema)?,
            "mysql" => validate_mysql_fields(&schema)?,
            _ => todo!(),
        }

        // Collect field_comments.
        let mut field_comments = BTreeMap::new();
        for column in columns {
            if column.comment.is_some() {
                let column_id = schema.column_id_of(column.name.name.as_str())?;
                field_comments.insert(column_id, column.comment.clone().unwrap_or_default());
            }
        }

        // Collect and check primary column.
        let mut primary_column_ids = Vec::new();
        if primary_keys.len() != 1 {
            return Err(ErrorCode::BadArguments("Only support one primary key"));
        }
        let primary_key = match primary_keys.first() {
            Some(pk) => pk.clone(),
            None => return Err(ErrorCode::BadArguments("Miss primary key")),
        };
        let pk_id = schema.column_id_of(primary_key.name.as_str())?;
        primary_column_ids.push(pk_id);

        // Comment.
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
