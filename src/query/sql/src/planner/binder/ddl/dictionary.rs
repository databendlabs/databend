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
use std::sync::LazyLock;

use databend_common_ast::ast::CreateDictionaryStmt;
use databend_common_ast::ast::DropDictionaryStmt;
use databend_common_ast::ast::RenameDictionaryStmt;
use databend_common_ast::ast::ShowCreateDictionaryStmt;
use databend_common_ast::ast::ShowDictionariesStmt;
use databend_common_ast::ast::ShowLimit;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_meta_app::schema::DictionaryMeta;
use itertools::Itertools;
use log::debug;

use crate::plans::CreateDictionaryPlan;
use crate::plans::DropDictionaryPlan;
use crate::plans::Plan;
use crate::plans::RenameDictionaryPlan;
use crate::plans::RewriteKind;
use crate::plans::ShowCreateDictionaryPlan;
use crate::BindContext;
use crate::Binder;
use crate::SelectBuilder;

pub const DICT_OPT_KEY_SQL_HOST: &str = "host";
pub const DICT_OPT_KEY_SQL_PORT: &str = "port";
pub const DICT_OPT_KEY_SQL_USERNAME: &str = "username";
pub const DICT_OPT_KEY_SQL_PASSWORD: &str = "password";
pub const DICT_OPT_KEY_SQL_DB: &str = "db";
pub const DICT_OPT_KEY_SQL_TABLE: &str = "table";

pub const DICT_OPT_KEY_REDIS_HOST: &str = "host";
pub const DICT_OPT_KEY_REDIS_PORT: &str = "port";
pub const DICT_OPT_KEY_REDIS_USERNAME: &str = "username";
pub const DICT_OPT_KEY_REDIS_PASSWORD: &str = "password";
pub const DICT_OPT_KEY_REDIS_DB_INDEX: &str = "db_index";

static DICT_REQUIRED_SQL_OPTION_KEYS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut r = HashSet::new();
    r.insert(DICT_OPT_KEY_SQL_HOST);
    r.insert(DICT_OPT_KEY_SQL_PORT);
    r.insert(DICT_OPT_KEY_SQL_USERNAME);
    r.insert(DICT_OPT_KEY_SQL_PASSWORD);
    r.insert(DICT_OPT_KEY_SQL_DB);
    r.insert(DICT_OPT_KEY_SQL_TABLE);
    r
});

static DICT_REQUIRED_REDIS_OPTION_KEYS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut r = HashSet::new();
    r.insert(DICT_OPT_KEY_REDIS_HOST);
    r.insert(DICT_OPT_KEY_REDIS_PORT);
    r
});

static DICT_OPTIONAL_REDIS_OPTION_KEYS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut r = HashSet::new();
    r.insert(DICT_OPT_KEY_REDIS_USERNAME);
    r.insert(DICT_OPT_KEY_REDIS_PASSWORD);
    r.insert(DICT_OPT_KEY_REDIS_DB_INDEX);
    r
});

fn is_dict_required_sql_opt_key<S: AsRef<str>>(opt_key: S) -> bool {
    DICT_REQUIRED_SQL_OPTION_KEYS.contains(opt_key.as_ref())
}

fn is_dict_required_redis_opt_key<S: AsRef<str>>(opt_key: S) -> bool {
    DICT_REQUIRED_REDIS_OPTION_KEYS.contains(opt_key.as_ref())
}

fn is_dict_optional_redis_opt_key<S: AsRef<str>>(opt_key: S) -> bool {
    DICT_OPTIONAL_REDIS_OPTION_KEYS.contains(opt_key.as_ref())
}

fn insert_dictionary_sql_option_with_validation(
    options: &mut BTreeMap<String, String>,
    key: String,
    value: String,
) -> Result<()> {
    if is_dict_required_sql_opt_key(&key) {
        if key == DICT_OPT_KEY_SQL_PORT && value.parse::<u64>().is_err() {
            return Err(ErrorCode::BadArguments(format!(
                "dictionary option {key} must be a positive integer",
            )));
        }
        if options.insert(key.clone(), value).is_some() {
            return Err(ErrorCode::BadArguments(format!(
                "dictionary option {key} duplicated",
            )));
        }
    } else {
        return Err(ErrorCode::BadArguments(format!(
            "dictionary option {key} is not a valid option, required options are [`host`, `port`, `username`, `password`, `db`, `table`]",
        )));
    }
    Ok(())
}

fn insert_dictionary_redis_option_with_validation(
    options: &mut BTreeMap<String, String>,
    key: String,
    value: String,
) -> Result<()> {
    if is_dict_required_redis_opt_key(&key) || is_dict_optional_redis_opt_key(&key) {
        if key == DICT_OPT_KEY_REDIS_PORT {
            if value.parse::<u64>().is_err() {
                return Err(ErrorCode::BadArguments(format!(
                    "dictionary option {key} must be a positive integer",
                )));
            }
        } else if key == DICT_OPT_KEY_REDIS_DB_INDEX && !value.parse::<u8>().is_ok_and(|v| v <= 15)
        {
            return Err(ErrorCode::BadArguments(format!(
                "dictionary option {key} must be between 0 to 15",
            )));
        }
        if options.insert(key.clone(), value).is_some() {
            return Err(ErrorCode::BadArguments(format!(
                "dictionary option {key} duplicated",
            )));
        }
    } else {
        return Err(ErrorCode::BadArguments(format!(
            "dictionary option {key} is not a valid option, required options are [`host`, `port`], optional options are [`username`, `password`, `db_index`]",
        )));
    }
    Ok(())
}

fn validate_dictionary_options(
    source: &str,
    source_options: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>> {
    let mut options: BTreeMap<String, String> = BTreeMap::new();
    match source {
        "mysql" => {
            for (key, value) in source_options {
                insert_dictionary_sql_option_with_validation(
                    &mut options,
                    key.to_lowercase(),
                    value.to_string(),
                )?;
            }
            let option_keys = options.keys().map(|k| k.as_str()).collect();
            let diff_keys = DICT_REQUIRED_SQL_OPTION_KEYS
                .difference(&option_keys)
                .collect::<Vec<_>>()
                .into_iter()
                .join(", ");
            if !diff_keys.is_empty() {
                return Err(ErrorCode::BadArguments(format!(
                    "dictionary miss options {diff_keys}, required options are [`host`, `port`, `username`, `password`, `db`, `table`]",
                )));
            }
        }
        "redis" => {
            for (key, value) in source_options {
                insert_dictionary_redis_option_with_validation(
                    &mut options,
                    key.to_lowercase(),
                    value.to_string(),
                )?;
            }
            let option_keys = options.keys().map(|k| k.as_str()).collect();
            let diff_keys = DICT_REQUIRED_REDIS_OPTION_KEYS
                .difference(&option_keys)
                .collect::<Vec<_>>()
                .into_iter()
                .join(", ");
            if !diff_keys.is_empty() {
                return Err(ErrorCode::BadArguments(format!(
                    "dictionary miss options {diff_keys}, required options are [`host`, `port`], optional options are [`username`, `password`, `db_index`]",
                )));
            }
        }
        _ => unreachable!(),
    }

    Ok(options)
}

fn validate_mysql_fields(schema: &TableSchema) -> Result<()> {
    for field in schema.fields() {
        if !matches!(
            field.data_type().remove_nullable(),
            TableDataType::Boolean | TableDataType::String | TableDataType::Number(_)
        ) {
            return Err(ErrorCode::BadArguments(
                "The type of Mysql field must be in [`boolean`, `string`, `number`]",
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
                "The type of Redis field must be `string`",
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

        let source = self.normalize_object_identifier(source_name).to_lowercase();

        if source != "mysql" && source != "redis" {
            return Err(ErrorCode::BadArguments(format!(
                "The specified source '{}' is not currently supported",
                source,
            )));
        }

        // Check for options
        let options = validate_dictionary_options(&source, source_options)?;

        // Check for data source fields.
        let (schema, _, _) = self.analyze_create_table_schema_by_columns(columns).await?;
        match source.as_str() {
            "redis" => validate_redis_fields(&schema)?,
            "mysql" => validate_mysql_fields(&schema)?,
            _ => unreachable!(),
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

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_dictionaries(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &ShowDictionariesStmt,
    ) -> Result<Plan> {
        let ShowDictionariesStmt { database, limit } = stmt;

        let mut select_builder = SelectBuilder::from("default.system.dictionaries");

        select_builder
            .with_column("database AS Database")
            .with_column("name AS Dictionary")
            .with_column("key_names AS Key_Names")
            .with_column("key_types AS key_Types")
            .with_column("attribute_names AS Attribute_Names")
            .with_column("attribute_types AS Attribute_Types")
            .with_column("source AS Source")
            .with_column("comment AS Comment");

        select_builder
            .with_order_by("database")
            .with_order_by("name");

        let database = self.check_database_exist(&None, database).await?;
        select_builder.with_filter(format!("database = '{}'", database.clone()));

        match limit {
            None => (),
            Some(ShowLimit::Like { pattern }) => {
                select_builder.with_filter(format!("name LIKE '{pattern}'"));
            }
            Some(ShowLimit::Where { selection }) => {
                select_builder.with_filter(format!("({selection})"));
            }
        };
        let query = select_builder.build();
        debug!("show dictionaries rewrite to: {:?}", query);
        self.bind_rewrite_to_query(
            bind_context,
            query.as_str(),
            RewriteKind::ShowDictionaries(database.clone()),
        )
        .await
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_rename_dictionary(
        &mut self,
        stmt: &RenameDictionaryStmt,
    ) -> Result<Plan> {
        let RenameDictionaryStmt {
            if_exists,
            catalog,
            database,
            dictionary,
            new_catalog,
            new_database,
            new_dictionary,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let (catalog, database, dictionary) =
            self.normalize_object_identifier_triple(catalog, database, dictionary);

        let (new_catalog, new_database, new_dictionary) =
            self.normalize_object_identifier_triple(new_catalog, new_database, new_dictionary);

        if new_catalog != catalog {
            return Err(ErrorCode::BadArguments(
                "Rename dictionary not allow modify catalog",
            ));
        }

        let catalog_info = self.ctx.get_catalog(&catalog).await?;
        let db = catalog_info.get_database(&tenant, &database).await?;
        let database_id = db.get_db_info().database_id.db_id;

        let new_db = catalog_info.get_database(&tenant, &new_database).await?;
        let new_database_id = new_db.get_db_info().database_id.db_id;

        Ok(Plan::RenameDictionary(Box::new(RenameDictionaryPlan {
            tenant,
            if_exists: *if_exists,
            catalog,
            database_id,
            dictionary,
            new_database_id,
            new_dictionary,
        })))
    }
}
