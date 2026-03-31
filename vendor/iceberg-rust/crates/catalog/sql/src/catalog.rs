// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::time::Duration;

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{TableMetadata, TableMetadataBuilder};
use iceberg::table::Table;
use iceberg::{
    Catalog, CatalogBuilder, Error, ErrorKind, MetadataLocation, Namespace, NamespaceIdent, Result,
    TableCommit, TableCreation, TableIdent,
};
use sqlx::any::{AnyPoolOptions, AnyQueryResult, AnyRow, install_default_drivers};
use sqlx::{Any, AnyPool, Row, Transaction};

use crate::error::{
    from_sqlx_error, no_such_namespace_err, no_such_table_err, table_already_exists_err,
};

/// catalog URI
pub const SQL_CATALOG_PROP_URI: &str = "uri";
/// catalog warehouse location
pub const SQL_CATALOG_PROP_WAREHOUSE: &str = "warehouse";
/// catalog sql bind style
pub const SQL_CATALOG_PROP_BIND_STYLE: &str = "sql_bind_style";

static CATALOG_TABLE_NAME: &str = "iceberg_tables";
static CATALOG_FIELD_CATALOG_NAME: &str = "catalog_name";
static CATALOG_FIELD_TABLE_NAME: &str = "table_name";
static CATALOG_FIELD_TABLE_NAMESPACE: &str = "table_namespace";
static CATALOG_FIELD_METADATA_LOCATION_PROP: &str = "metadata_location";
static CATALOG_FIELD_PREVIOUS_METADATA_LOCATION_PROP: &str = "previous_metadata_location";
static CATALOG_FIELD_RECORD_TYPE: &str = "iceberg_type";
static CATALOG_FIELD_TABLE_RECORD_TYPE: &str = "TABLE";

static NAMESPACE_TABLE_NAME: &str = "iceberg_namespace_properties";
static NAMESPACE_FIELD_NAME: &str = "namespace";
static NAMESPACE_FIELD_PROPERTY_KEY: &str = "property_key";
static NAMESPACE_FIELD_PROPERTY_VALUE: &str = "property_value";

static NAMESPACE_LOCATION_PROPERTY_KEY: &str = "location";

static MAX_CONNECTIONS: u32 = 10; // Default the SQL pool to 10 connections if not provided
static IDLE_TIMEOUT: u64 = 10; // Default the maximum idle timeout per connection to 10s before it is closed
static TEST_BEFORE_ACQUIRE: bool = true; // Default the health-check of each connection to enabled prior to returning

/// Builder for [`SqlCatalog`]
#[derive(Debug)]
pub struct SqlCatalogBuilder(SqlCatalogConfig);

impl Default for SqlCatalogBuilder {
    fn default() -> Self {
        Self(SqlCatalogConfig {
            uri: "".to_string(),
            name: "".to_string(),
            warehouse_location: "".to_string(),
            sql_bind_style: SqlBindStyle::DollarNumeric,
            props: HashMap::new(),
        })
    }
}

impl SqlCatalogBuilder {
    /// Configure the database URI
    ///
    /// If `SQL_CATALOG_PROP_URI` has a value set in `props` during `SqlCatalogBuilder::load`,
    /// that value takes precedence, and the value specified by this method will not be used.
    pub fn uri(mut self, uri: impl Into<String>) -> Self {
        self.0.uri = uri.into();
        self
    }

    /// Configure the warehouse location
    ///
    /// If `SQL_CATALOG_PROP_WAREHOUSE` has a value set in `props` during `SqlCatalogBuilder::load`,
    /// that value takes precedence, and the value specified by this method will not be used.
    pub fn warehouse_location(mut self, location: impl Into<String>) -> Self {
        self.0.warehouse_location = location.into();
        self
    }

    /// Configure the bound SQL Statement
    ///
    /// If `SQL_CATALOG_PROP_BIND_STYLE` has a value set in `props` during `SqlCatalogBuilder::load`,
    /// that value takes precedence, and the value specified by this method will not be used.
    pub fn sql_bind_style(mut self, sql_bind_style: SqlBindStyle) -> Self {
        self.0.sql_bind_style = sql_bind_style;
        self
    }

    /// Configure the any properties
    ///
    /// If the same key has values set in `props` during `SqlCatalogBuilder::load`,
    /// those values will take precedence.
    pub fn props(mut self, props: HashMap<String, String>) -> Self {
        for (k, v) in props {
            self.0.props.insert(k, v);
        }
        self
    }

    /// Set a new property on the property to be configured.
    /// When multiple methods are executed with the same key,
    /// the later-set value takes precedence.
    ///
    /// If the same key has values set in `props` during `SqlCatalogBuilder::load`,
    /// those values will take precedence.
    pub fn prop(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.0.props.insert(key.into(), value.into());
        self
    }
}

impl CatalogBuilder for SqlCatalogBuilder {
    type C = SqlCatalog;

    fn load(
        mut self,
        name: impl Into<String>,
        props: HashMap<String, String>,
    ) -> impl Future<Output = Result<Self::C>> + Send {
        let name = name.into();

        for (k, v) in props {
            self.0.props.insert(k, v);
        }

        if let Some(uri) = self.0.props.remove(SQL_CATALOG_PROP_URI) {
            self.0.uri = uri;
        }
        if let Some(warehouse_location) = self.0.props.remove(SQL_CATALOG_PROP_WAREHOUSE) {
            self.0.warehouse_location = warehouse_location;
        }

        let mut valid_sql_bind_style = true;
        if let Some(sql_bind_style) = self.0.props.remove(SQL_CATALOG_PROP_BIND_STYLE) {
            if let Ok(sql_bind_style) = SqlBindStyle::from_str(&sql_bind_style) {
                self.0.sql_bind_style = sql_bind_style;
            } else {
                valid_sql_bind_style = false;
            }
        }

        async move {
            if name.trim().is_empty() {
                Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Catalog name cannot be empty",
                ))
            } else if !valid_sql_bind_style {
                Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "`{}` values are valid only if they're `{}` or `{}`",
                        SQL_CATALOG_PROP_BIND_STYLE,
                        SqlBindStyle::DollarNumeric,
                        SqlBindStyle::QMark
                    ),
                ))
            } else {
                SqlCatalog::new(self.0).await
            }
        }
    }
}

/// A struct representing the SQL catalog configuration.
///
/// This struct contains various parameters that are used to configure a SQL catalog,
/// such as the database URI, warehouse location, and file I/O settings.
/// You are required to provide a `SqlBindStyle`, which determines how SQL statements will be bound to values in the catalog.
/// The options available for this parameter include:
/// - `SqlBindStyle::DollarNumeric`: Binds SQL statements using `$1`, `$2`, etc., as placeholders. This is for PostgreSQL databases.
/// - `SqlBindStyle::QuestionMark`: Binds SQL statements using `?` as a placeholder. This is for MySQL and SQLite databases.
#[derive(Debug)]
struct SqlCatalogConfig {
    uri: String,
    name: String,
    warehouse_location: String,
    sql_bind_style: SqlBindStyle,
    props: HashMap<String, String>,
}

#[derive(Debug)]
/// Sql catalog implementation.
pub struct SqlCatalog {
    name: String,
    connection: AnyPool,
    warehouse_location: String,
    fileio: FileIO,
    sql_bind_style: SqlBindStyle,
}

#[derive(Debug, PartialEq, strum::EnumString, strum::Display)]
/// Set the SQL parameter bind style to either $1..$N (Postgres style) or ? (SQLite/MySQL/MariaDB)
pub enum SqlBindStyle {
    /// DollarNumeric uses parameters of the form `$1..$N``, which is the Postgres style
    DollarNumeric,
    /// QMark uses parameters of the form `?` which is the style for other dialects (SQLite/MySQL/MariaDB)
    QMark,
}

impl SqlCatalog {
    /// Create new sql catalog instance
    async fn new(config: SqlCatalogConfig) -> Result<Self> {
        let fileio = FileIO::from_path(&config.warehouse_location)?.build()?;
        install_default_drivers();
        let max_connections: u32 = config
            .props
            .get("pool.max-connections")
            .map(|v| v.parse().unwrap())
            .unwrap_or(MAX_CONNECTIONS);
        let idle_timeout: u64 = config
            .props
            .get("pool.idle-timeout")
            .map(|v| v.parse().unwrap())
            .unwrap_or(IDLE_TIMEOUT);
        let test_before_acquire: bool = config
            .props
            .get("pool.test-before-acquire")
            .map(|v| v.parse().unwrap())
            .unwrap_or(TEST_BEFORE_ACQUIRE);

        let pool = AnyPoolOptions::new()
            .max_connections(max_connections)
            .idle_timeout(Duration::from_secs(idle_timeout))
            .test_before_acquire(test_before_acquire)
            .connect(&config.uri)
            .await
            .map_err(from_sqlx_error)?;

        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {CATALOG_TABLE_NAME} (
                {CATALOG_FIELD_CATALOG_NAME} VARCHAR(255) NOT NULL,
                {CATALOG_FIELD_TABLE_NAMESPACE} VARCHAR(255) NOT NULL,
                {CATALOG_FIELD_TABLE_NAME} VARCHAR(255) NOT NULL,
                {CATALOG_FIELD_METADATA_LOCATION_PROP} VARCHAR(1000),
                {CATALOG_FIELD_PREVIOUS_METADATA_LOCATION_PROP} VARCHAR(1000),
                {CATALOG_FIELD_RECORD_TYPE} VARCHAR(5),
                PRIMARY KEY ({CATALOG_FIELD_CATALOG_NAME}, {CATALOG_FIELD_TABLE_NAMESPACE}, {CATALOG_FIELD_TABLE_NAME}))"
        ))
        .execute(&pool)
        .await
        .map_err(from_sqlx_error)?;

        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {NAMESPACE_TABLE_NAME} (
                {CATALOG_FIELD_CATALOG_NAME} VARCHAR(255) NOT NULL,
                {NAMESPACE_FIELD_NAME} VARCHAR(255) NOT NULL,
                {NAMESPACE_FIELD_PROPERTY_KEY} VARCHAR(255),
                {NAMESPACE_FIELD_PROPERTY_VALUE} VARCHAR(1000),
                PRIMARY KEY ({CATALOG_FIELD_CATALOG_NAME}, {NAMESPACE_FIELD_NAME}, {NAMESPACE_FIELD_PROPERTY_KEY}))"
        ))
        .execute(&pool)
        .await
        .map_err(from_sqlx_error)?;

        Ok(SqlCatalog {
            name: config.name.to_owned(),
            connection: pool,
            warehouse_location: config.warehouse_location,
            fileio,
            sql_bind_style: config.sql_bind_style,
        })
    }

    /// SQLX Any does not implement PostgresSQL bindings, so we have to do this.
    fn replace_placeholders(&self, query: &str) -> String {
        match self.sql_bind_style {
            SqlBindStyle::DollarNumeric => {
                let mut count = 1;
                query
                    .chars()
                    .fold(String::with_capacity(query.len()), |mut acc, c| {
                        if c == '?' {
                            acc.push('$');
                            acc.push_str(&count.to_string());
                            count += 1;
                        } else {
                            acc.push(c);
                        }
                        acc
                    })
            }
            _ => query.to_owned(),
        }
    }

    /// Fetch a vec of AnyRows from a given query
    async fn fetch_rows(&self, query: &str, args: Vec<Option<&str>>) -> Result<Vec<AnyRow>> {
        let query_with_placeholders = self.replace_placeholders(query);

        let mut sqlx_query = sqlx::query(&query_with_placeholders);
        for arg in args {
            sqlx_query = sqlx_query.bind(arg);
        }

        sqlx_query
            .fetch_all(&self.connection)
            .await
            .map_err(from_sqlx_error)
    }

    /// Execute statements in a transaction, provided or not
    async fn execute(
        &self,
        query: &str,
        args: Vec<Option<&str>>,
        transaction: Option<&mut Transaction<'_, Any>>,
    ) -> Result<AnyQueryResult> {
        let query_with_placeholders = self.replace_placeholders(query);

        let mut sqlx_query = sqlx::query(&query_with_placeholders);
        for arg in args {
            sqlx_query = sqlx_query.bind(arg);
        }

        match transaction {
            Some(t) => sqlx_query.execute(&mut **t).await.map_err(from_sqlx_error),
            None => {
                let mut tx = self.connection.begin().await.map_err(from_sqlx_error)?;
                let result = sqlx_query.execute(&mut *tx).await.map_err(from_sqlx_error);
                let _ = tx.commit().await.map_err(from_sqlx_error);
                result
            }
        }
    }
}

#[async_trait]
impl Catalog for SqlCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        // UNION will remove duplicates.
        let all_namespaces_stmt = format!(
            "SELECT {CATALOG_FIELD_TABLE_NAMESPACE}
             FROM {CATALOG_TABLE_NAME}
             WHERE {CATALOG_FIELD_CATALOG_NAME} = ?
             UNION
             SELECT {NAMESPACE_FIELD_NAME}
             FROM {NAMESPACE_TABLE_NAME}
             WHERE {CATALOG_FIELD_CATALOG_NAME} = ?"
        );

        let namespace_rows = self
            .fetch_rows(&all_namespaces_stmt, vec![
                Some(&self.name),
                Some(&self.name),
            ])
            .await?;

        let mut namespaces = HashSet::<NamespaceIdent>::with_capacity(namespace_rows.len());

        if let Some(parent) = parent {
            if self.namespace_exists(parent).await? {
                let parent_str = parent.join(".");

                for row in namespace_rows.iter() {
                    let nsp = row.try_get::<String, _>(0).map_err(from_sqlx_error)?;
                    // if parent = a, then we only want to see a.b, a.c returned.
                    if nsp != parent_str && nsp.starts_with(&parent_str) {
                        namespaces.insert(NamespaceIdent::from_strs(nsp.split("."))?);
                    }
                }

                Ok(namespaces.into_iter().collect::<Vec<NamespaceIdent>>())
            } else {
                no_such_namespace_err(parent)
            }
        } else {
            for row in namespace_rows.iter() {
                let nsp = row.try_get::<String, _>(0).map_err(from_sqlx_error)?;
                let mut levels = nsp.split(".").collect::<Vec<&str>>();
                if !levels.is_empty() {
                    let first_level = levels.drain(..1).collect::<Vec<&str>>();
                    namespaces.insert(NamespaceIdent::from_strs(first_level)?);
                }
            }

            Ok(namespaces.into_iter().collect::<Vec<NamespaceIdent>>())
        }
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        let exists = self.namespace_exists(namespace).await?;

        if exists {
            return Err(Error::new(
                iceberg::ErrorKind::Unexpected,
                format!("Namespace {namespace:?} already exists"),
            ));
        }

        let namespace_str = namespace.join(".");
        let insert = format!(
            "INSERT INTO {NAMESPACE_TABLE_NAME} ({CATALOG_FIELD_CATALOG_NAME}, {NAMESPACE_FIELD_NAME}, {NAMESPACE_FIELD_PROPERTY_KEY}, {NAMESPACE_FIELD_PROPERTY_VALUE})
             VALUES (?, ?, ?, ?)");
        if !properties.is_empty() {
            let mut insert_properties = properties.clone();
            insert_properties.insert("exists".to_string(), "true".to_string());

            let mut query_args = Vec::with_capacity(insert_properties.len() * 4);
            let mut insert_stmt = insert.clone();
            for (index, (key, value)) in insert_properties.iter().enumerate() {
                query_args.extend_from_slice(&[
                    Some(self.name.as_str()),
                    Some(namespace_str.as_str()),
                    Some(key.as_str()),
                    Some(value.as_str()),
                ]);
                if index > 0 {
                    insert_stmt.push_str(", (?, ?, ?, ?)");
                }
            }

            self.execute(&insert_stmt, query_args, None).await?;

            Ok(Namespace::with_properties(
                namespace.clone(),
                insert_properties,
            ))
        } else {
            // set a default property of exists = true
            self.execute(
                &insert,
                vec![
                    Some(&self.name),
                    Some(&namespace_str),
                    Some("exists"),
                    Some("true"),
                ],
                None,
            )
            .await?;
            Ok(Namespace::with_properties(namespace.clone(), properties))
        }
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        let exists = self.namespace_exists(namespace).await?;
        if exists {
            let namespace_props = self
                .fetch_rows(
                    &format!(
                        "SELECT
                            {NAMESPACE_FIELD_NAME},
                            {NAMESPACE_FIELD_PROPERTY_KEY},
                            {NAMESPACE_FIELD_PROPERTY_VALUE}
                            FROM {NAMESPACE_TABLE_NAME}
                            WHERE {CATALOG_FIELD_CATALOG_NAME} = ?
                            AND {NAMESPACE_FIELD_NAME} = ?"
                    ),
                    vec![Some(&self.name), Some(&namespace.join("."))],
                )
                .await?;

            let mut properties = HashMap::with_capacity(namespace_props.len());

            for row in namespace_props {
                let key = row
                    .try_get::<String, _>(NAMESPACE_FIELD_PROPERTY_KEY)
                    .map_err(from_sqlx_error)?;
                let value = row
                    .try_get::<String, _>(NAMESPACE_FIELD_PROPERTY_VALUE)
                    .map_err(from_sqlx_error)?;

                properties.insert(key, value);
            }

            Ok(Namespace::with_properties(namespace.clone(), properties))
        } else {
            no_such_namespace_err(namespace)
        }
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        let namespace_str = namespace.join(".");

        let table_namespaces = self
            .fetch_rows(
                &format!(
                    "SELECT 1 FROM {CATALOG_TABLE_NAME}
                     WHERE {CATALOG_FIELD_CATALOG_NAME} = ?
                      AND {CATALOG_FIELD_TABLE_NAMESPACE} = ?
                     LIMIT 1"
                ),
                vec![Some(&self.name), Some(&namespace_str)],
            )
            .await?;

        if !table_namespaces.is_empty() {
            Ok(true)
        } else {
            let namespaces = self
                .fetch_rows(
                    &format!(
                        "SELECT 1 FROM {NAMESPACE_TABLE_NAME}
                         WHERE {CATALOG_FIELD_CATALOG_NAME} = ?
                          AND {NAMESPACE_FIELD_NAME} = ?
                         LIMIT 1"
                    ),
                    vec![Some(&self.name), Some(&namespace_str)],
                )
                .await?;
            if !namespaces.is_empty() {
                Ok(true)
            } else {
                Ok(false)
            }
        }
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        let exists = self.namespace_exists(namespace).await?;
        if exists {
            let existing_properties = self.get_namespace(namespace).await?.properties().clone();
            let namespace_str = namespace.join(".");

            let mut updates = vec![];
            let mut inserts = vec![];

            for (key, value) in properties.iter() {
                if existing_properties.contains_key(key) {
                    if existing_properties.get(key) != Some(value) {
                        updates.push((key, value));
                    }
                } else {
                    inserts.push((key, value));
                }
            }

            let mut tx = self.connection.begin().await.map_err(from_sqlx_error)?;
            let update_stmt = format!(
                "UPDATE {NAMESPACE_TABLE_NAME} SET {NAMESPACE_FIELD_PROPERTY_VALUE} = ?
                 WHERE {CATALOG_FIELD_CATALOG_NAME} = ? 
                 AND {NAMESPACE_FIELD_NAME} = ?
                 AND {NAMESPACE_FIELD_PROPERTY_KEY} = ?"
            );

            let insert_stmt = format!(
                "INSERT INTO {NAMESPACE_TABLE_NAME} ({CATALOG_FIELD_CATALOG_NAME}, {NAMESPACE_FIELD_NAME}, {NAMESPACE_FIELD_PROPERTY_KEY}, {NAMESPACE_FIELD_PROPERTY_VALUE})
                 VALUES (?, ?, ?, ?)"
            );

            for (key, value) in updates {
                self.execute(
                    &update_stmt,
                    vec![
                        Some(value),
                        Some(&self.name),
                        Some(&namespace_str),
                        Some(key),
                    ],
                    Some(&mut tx),
                )
                .await?;
            }

            for (key, value) in inserts {
                self.execute(
                    &insert_stmt,
                    vec![
                        Some(&self.name),
                        Some(&namespace_str),
                        Some(key),
                        Some(value),
                    ],
                    Some(&mut tx),
                )
                .await?;
            }

            let _ = tx.commit().await.map_err(from_sqlx_error)?;

            Ok(())
        } else {
            no_such_namespace_err(namespace)
        }
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        let exists = self.namespace_exists(namespace).await?;
        if exists {
            // if there are tables in the namespace, don't allow drop.
            let tables = self.list_tables(namespace).await?;
            if !tables.is_empty() {
                return Err(Error::new(
                    iceberg::ErrorKind::Unexpected,
                    format!(
                        "Namespace {:?} is not empty. {} tables exist.",
                        namespace,
                        tables.len()
                    ),
                ));
            }

            self.execute(
                &format!(
                    "DELETE FROM {NAMESPACE_TABLE_NAME}
                     WHERE {NAMESPACE_FIELD_NAME} = ?
                      AND {CATALOG_FIELD_CATALOG_NAME} = ?"
                ),
                vec![Some(&namespace.join(".")), Some(&self.name)],
                None,
            )
            .await?;

            Ok(())
        } else {
            no_such_namespace_err(namespace)
        }
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        let exists = self.namespace_exists(namespace).await?;
        if exists {
            let rows = self
                .fetch_rows(
                    &format!(
                        "SELECT {CATALOG_FIELD_TABLE_NAME},
                                {CATALOG_FIELD_TABLE_NAMESPACE}
                         FROM {CATALOG_TABLE_NAME}
                         WHERE {CATALOG_FIELD_TABLE_NAMESPACE} = ?
                          AND {CATALOG_FIELD_CATALOG_NAME} = ?
                          AND (
                                {CATALOG_FIELD_RECORD_TYPE} = '{CATALOG_FIELD_TABLE_RECORD_TYPE}' 
                                OR {CATALOG_FIELD_RECORD_TYPE} IS NULL
                          )",
                    ),
                    vec![Some(&namespace.join(".")), Some(&self.name)],
                )
                .await?;

            let mut tables = HashSet::<TableIdent>::with_capacity(rows.len());

            for row in rows.iter() {
                let tbl = row
                    .try_get::<String, _>(CATALOG_FIELD_TABLE_NAME)
                    .map_err(from_sqlx_error)?;
                let ns_strs = row
                    .try_get::<String, _>(CATALOG_FIELD_TABLE_NAMESPACE)
                    .map_err(from_sqlx_error)?;
                let ns = NamespaceIdent::from_strs(ns_strs.split("."))?;
                tables.insert(TableIdent::new(ns, tbl));
            }

            Ok(tables.into_iter().collect::<Vec<TableIdent>>())
        } else {
            no_such_namespace_err(namespace)
        }
    }

    async fn table_exists(&self, identifier: &TableIdent) -> Result<bool> {
        let namespace = identifier.namespace().join(".");
        let table_name = identifier.name();
        let table_counts = self
            .fetch_rows(
                &format!(
                    "SELECT 1
                     FROM {CATALOG_TABLE_NAME}
                     WHERE {CATALOG_FIELD_TABLE_NAMESPACE} = ?
                      AND {CATALOG_FIELD_CATALOG_NAME} = ?
                      AND {CATALOG_FIELD_TABLE_NAME} = ?
                      AND (
                        {CATALOG_FIELD_RECORD_TYPE} = '{CATALOG_FIELD_TABLE_RECORD_TYPE}' 
                        OR {CATALOG_FIELD_RECORD_TYPE} IS NULL
                      )"
                ),
                vec![Some(&namespace), Some(&self.name), Some(table_name)],
            )
            .await?;

        if !table_counts.is_empty() {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn drop_table(&self, identifier: &TableIdent) -> Result<()> {
        if !self.table_exists(identifier).await? {
            return no_such_table_err(identifier);
        }

        self.execute(
            &format!(
                "DELETE FROM {CATALOG_TABLE_NAME}
                 WHERE {CATALOG_FIELD_CATALOG_NAME} = ?
                  AND {CATALOG_FIELD_TABLE_NAME} = ?
                  AND {CATALOG_FIELD_TABLE_NAMESPACE} = ?
                  AND (
                    {CATALOG_FIELD_RECORD_TYPE} = '{CATALOG_FIELD_TABLE_RECORD_TYPE}' 
                    OR {CATALOG_FIELD_RECORD_TYPE} IS NULL
                  )"
            ),
            vec![
                Some(&self.name),
                Some(identifier.name()),
                Some(&identifier.namespace().join(".")),
            ],
            None,
        )
        .await?;

        Ok(())
    }

    async fn load_table(&self, identifier: &TableIdent) -> Result<Table> {
        if !self.table_exists(identifier).await? {
            return no_such_table_err(identifier);
        }

        let rows = self
            .fetch_rows(
                &format!(
                    "SELECT {CATALOG_FIELD_METADATA_LOCATION_PROP}
                     FROM {CATALOG_TABLE_NAME}
                     WHERE {CATALOG_FIELD_CATALOG_NAME} = ?
                      AND {CATALOG_FIELD_TABLE_NAME} = ?
                      AND {CATALOG_FIELD_TABLE_NAMESPACE} = ?
                      AND (
                        {CATALOG_FIELD_RECORD_TYPE} = '{CATALOG_FIELD_TABLE_RECORD_TYPE}' 
                        OR {CATALOG_FIELD_RECORD_TYPE} IS NULL
                      )"
                ),
                vec![
                    Some(&self.name),
                    Some(identifier.name()),
                    Some(&identifier.namespace().join(".")),
                ],
            )
            .await?;

        if rows.is_empty() {
            return no_such_table_err(identifier);
        }

        let row = &rows[0];
        let tbl_metadata_location = row
            .try_get::<String, _>(CATALOG_FIELD_METADATA_LOCATION_PROP)
            .map_err(from_sqlx_error)?;

        let metadata = TableMetadata::read_from(&self.fileio, &tbl_metadata_location).await?;

        Ok(Table::builder()
            .file_io(self.fileio.clone())
            .identifier(identifier.clone())
            .metadata_location(tbl_metadata_location)
            .metadata(metadata)
            .build()?)
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        if !self.namespace_exists(namespace).await? {
            return no_such_namespace_err(namespace);
        }

        let tbl_name = creation.name.clone();
        let tbl_ident = TableIdent::new(namespace.clone(), tbl_name.clone());

        if self.table_exists(&tbl_ident).await? {
            return table_already_exists_err(&tbl_ident);
        }

        let (tbl_creation, location) = match creation.location.clone() {
            Some(location) => (creation, location),
            None => {
                // fall back to namespace-specific location
                // and then to warehouse location
                let nsp_properties = self.get_namespace(namespace).await?.properties().clone();
                let nsp_location = match nsp_properties.get(NAMESPACE_LOCATION_PROPERTY_KEY) {
                    Some(location) => location.clone(),
                    None => {
                        format!(
                            "{}/{}",
                            self.warehouse_location.clone(),
                            namespace.join("/")
                        )
                    }
                };

                let tbl_location = format!("{}/{}", nsp_location, tbl_ident.name());

                (
                    TableCreation {
                        location: Some(tbl_location.clone()),
                        ..creation
                    },
                    tbl_location,
                )
            }
        };

        let tbl_metadata = TableMetadataBuilder::from_table_creation(tbl_creation)?
            .build()?
            .metadata;
        let tbl_metadata_location =
            MetadataLocation::new_with_table_location(location.clone()).to_string();

        tbl_metadata
            .write_to(&self.fileio, &tbl_metadata_location)
            .await?;

        self.execute(&format!(
            "INSERT INTO {CATALOG_TABLE_NAME}
             ({CATALOG_FIELD_CATALOG_NAME}, {CATALOG_FIELD_TABLE_NAMESPACE}, {CATALOG_FIELD_TABLE_NAME}, {CATALOG_FIELD_METADATA_LOCATION_PROP}, {CATALOG_FIELD_RECORD_TYPE})
             VALUES (?, ?, ?, ?, ?)
            "), vec![Some(&self.name), Some(&namespace.join(".")), Some(&tbl_name.clone()), Some(&tbl_metadata_location), Some(CATALOG_FIELD_TABLE_RECORD_TYPE)], None).await?;

        Ok(Table::builder()
            .file_io(self.fileio.clone())
            .metadata_location(tbl_metadata_location)
            .identifier(tbl_ident)
            .metadata(tbl_metadata)
            .build()?)
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        if src == dest {
            return Ok(());
        }

        if !self.table_exists(src).await? {
            return no_such_table_err(src);
        }

        if !self.namespace_exists(dest.namespace()).await? {
            return no_such_namespace_err(dest.namespace());
        }

        if self.table_exists(dest).await? {
            return table_already_exists_err(dest);
        }

        self.execute(
            &format!(
                "UPDATE {CATALOG_TABLE_NAME}
                 SET {CATALOG_FIELD_TABLE_NAME} = ?, {CATALOG_FIELD_TABLE_NAMESPACE} = ?
                 WHERE {CATALOG_FIELD_CATALOG_NAME} = ?
                  AND {CATALOG_FIELD_TABLE_NAME} = ?
                  AND {CATALOG_FIELD_TABLE_NAMESPACE} = ?
                  AND (
                    {CATALOG_FIELD_RECORD_TYPE} = '{CATALOG_FIELD_TABLE_RECORD_TYPE}'
                    OR {CATALOG_FIELD_RECORD_TYPE} IS NULL
                )"
            ),
            vec![
                Some(dest.name()),
                Some(&dest.namespace().join(".")),
                Some(&self.name),
                Some(src.name()),
                Some(&src.namespace().join(".")),
            ],
            None,
        )
        .await?;

        Ok(())
    }

    async fn register_table(
        &self,
        table_ident: &TableIdent,
        metadata_location: String,
    ) -> Result<Table> {
        if self.table_exists(table_ident).await? {
            return table_already_exists_err(table_ident);
        }

        let metadata = TableMetadata::read_from(&self.fileio, &metadata_location).await?;

        let namespace = table_ident.namespace();
        let tbl_name = table_ident.name().to_string();

        self.execute(&format!(
            "INSERT INTO {CATALOG_TABLE_NAME}
             ({CATALOG_FIELD_CATALOG_NAME}, {CATALOG_FIELD_TABLE_NAMESPACE}, {CATALOG_FIELD_TABLE_NAME}, {CATALOG_FIELD_METADATA_LOCATION_PROP}, {CATALOG_FIELD_RECORD_TYPE})
             VALUES (?, ?, ?, ?, ?)
            "), vec![Some(&self.name), Some(&namespace.join(".")), Some(&tbl_name), Some(&metadata_location), Some(CATALOG_FIELD_TABLE_RECORD_TYPE)], None).await?;

        Ok(Table::builder()
            .identifier(table_ident.clone())
            .metadata_location(metadata_location)
            .metadata(metadata)
            .file_io(self.fileio.clone())
            .build()?)
    }

    /// Updates an existing table within the SQL catalog.
    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        let table_ident = commit.identifier().clone();
        let current_table = self.load_table(&table_ident).await?;
        let current_metadata_location = current_table.metadata_location_result()?.to_string();

        let staged_table = commit.apply(current_table)?;
        let staged_metadata_location = staged_table.metadata_location_result()?;

        staged_table
            .metadata()
            .write_to(staged_table.file_io(), &staged_metadata_location)
            .await?;

        let update_result = self
            .execute(
                &format!(
                    "UPDATE {CATALOG_TABLE_NAME}
                     SET {CATALOG_FIELD_METADATA_LOCATION_PROP} = ?, {CATALOG_FIELD_PREVIOUS_METADATA_LOCATION_PROP} = ?
                     WHERE {CATALOG_FIELD_CATALOG_NAME} = ?
                      AND {CATALOG_FIELD_TABLE_NAME} = ?
                      AND {CATALOG_FIELD_TABLE_NAMESPACE} = ?
                      AND (
                        {CATALOG_FIELD_RECORD_TYPE} = '{CATALOG_FIELD_TABLE_RECORD_TYPE}'
                        OR {CATALOG_FIELD_RECORD_TYPE} IS NULL
                      )
                      AND {CATALOG_FIELD_METADATA_LOCATION_PROP} = ?"
                ),
                vec![
                    Some(staged_metadata_location),
                    Some(current_metadata_location.as_str()),
                    Some(&self.name),
                    Some(table_ident.name()),
                    Some(&table_ident.namespace().join(".")),
                    Some(current_metadata_location.as_str()),
                ],
                None,
            )
            .await?;

        if update_result.rows_affected() == 0 {
            return Err(Error::new(
                ErrorKind::CatalogCommitConflicts,
                format!("Commit conflicted for table: {table_ident}"),
            )
            .with_retryable(true));
        }

        Ok(staged_table)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::hash::Hash;

    use iceberg::spec::{NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder, Type};
    use iceberg::table::Table;
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use iceberg::{Catalog, CatalogBuilder, Namespace, NamespaceIdent, TableCreation, TableIdent};
    use itertools::Itertools;
    use regex::Regex;
    use sqlx::migrate::MigrateDatabase;
    use tempfile::TempDir;

    use crate::catalog::{
        NAMESPACE_LOCATION_PROPERTY_KEY, SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI,
        SQL_CATALOG_PROP_WAREHOUSE,
    };
    use crate::{SqlBindStyle, SqlCatalogBuilder};

    const UUID_REGEX_STR: &str = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";

    fn temp_path() -> String {
        let temp_dir = TempDir::new().unwrap();
        temp_dir.path().to_str().unwrap().to_string()
    }

    fn to_set<T: std::cmp::Eq + Hash>(vec: Vec<T>) -> HashSet<T> {
        HashSet::from_iter(vec)
    }

    fn default_properties() -> HashMap<String, String> {
        HashMap::from([("exists".to_string(), "true".to_string())])
    }

    async fn new_sql_catalog(warehouse_location: String) -> impl Catalog {
        let sql_lite_uri = format!("sqlite:{}", temp_path());
        sqlx::Sqlite::create_database(&sql_lite_uri).await.unwrap();

        let props = HashMap::from_iter([
            (SQL_CATALOG_PROP_URI.to_string(), sql_lite_uri.to_string()),
            (SQL_CATALOG_PROP_WAREHOUSE.to_string(), warehouse_location),
            (
                SQL_CATALOG_PROP_BIND_STYLE.to_string(),
                SqlBindStyle::DollarNumeric.to_string(),
            ),
        ]);
        SqlCatalogBuilder::default()
            .load("iceberg", props)
            .await
            .unwrap()
    }

    async fn create_namespace<C: Catalog>(catalog: &C, namespace_ident: &NamespaceIdent) {
        let _ = catalog
            .create_namespace(namespace_ident, HashMap::new())
            .await
            .unwrap();
    }

    async fn create_namespaces<C: Catalog>(catalog: &C, namespace_idents: &Vec<&NamespaceIdent>) {
        for namespace_ident in namespace_idents {
            let _ = create_namespace(catalog, namespace_ident).await;
        }
    }

    fn simple_table_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "foo", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap()
    }

    async fn create_table<C: Catalog>(catalog: &C, table_ident: &TableIdent) {
        let _ = catalog
            .create_table(
                &table_ident.namespace,
                TableCreation::builder()
                    .name(table_ident.name().into())
                    .schema(simple_table_schema())
                    .location(temp_path())
                    .build(),
            )
            .await
            .unwrap();
    }

    async fn create_tables<C: Catalog>(catalog: &C, table_idents: Vec<&TableIdent>) {
        for table_ident in table_idents {
            create_table(catalog, table_ident).await;
        }
    }

    fn assert_table_eq(table: &Table, expected_table_ident: &TableIdent, expected_schema: &Schema) {
        assert_eq!(table.identifier(), expected_table_ident);

        let metadata = table.metadata();

        assert_eq!(metadata.current_schema().as_ref(), expected_schema);

        let expected_partition_spec = PartitionSpec::builder(expected_schema.clone())
            .with_spec_id(0)
            .build()
            .unwrap();

        assert_eq!(
            metadata
                .partition_specs_iter()
                .map(|p| p.as_ref())
                .collect_vec(),
            vec![&expected_partition_spec]
        );

        let expected_sorted_order = SortOrder::builder()
            .with_order_id(0)
            .with_fields(vec![])
            .build(expected_schema)
            .unwrap();

        assert_eq!(
            metadata
                .sort_orders_iter()
                .map(|s| s.as_ref())
                .collect_vec(),
            vec![&expected_sorted_order]
        );

        assert_eq!(metadata.properties(), &HashMap::new());

        assert!(!table.readonly());
    }

    fn assert_table_metadata_location_matches(table: &Table, regex_str: &str) {
        let actual = table.metadata_location().unwrap().to_string();
        let regex = Regex::new(regex_str).unwrap();
        assert!(regex.is_match(&actual))
    }

    #[tokio::test]
    async fn test_initialized() {
        let warehouse_loc = temp_path();
        new_sql_catalog(warehouse_loc.clone()).await;
        // catalog instantiation should not fail even if tables exist
        new_sql_catalog(warehouse_loc.clone()).await;
        new_sql_catalog(warehouse_loc.clone()).await;
    }

    #[tokio::test]
    async fn test_builder_method() {
        let sql_lite_uri = format!("sqlite:{}", temp_path());
        sqlx::Sqlite::create_database(&sql_lite_uri).await.unwrap();
        let warehouse_location = temp_path();

        let catalog = SqlCatalogBuilder::default()
            .uri(sql_lite_uri.to_string())
            .warehouse_location(warehouse_location.clone())
            .sql_bind_style(SqlBindStyle::QMark)
            .load("iceberg", HashMap::default())
            .await;
        assert!(catalog.is_ok());

        let catalog = catalog.unwrap();
        assert!(catalog.warehouse_location == warehouse_location);
        assert!(catalog.sql_bind_style == SqlBindStyle::QMark);
    }

    /// Overwriting an sqlite database with a non-existent path causes
    /// catalog generation to fail
    #[tokio::test]
    async fn test_builder_props_non_existent_path_fails() {
        let sql_lite_uri = format!("sqlite:{}", temp_path());
        let sql_lite_uri2 = format!("sqlite:{}", temp_path());
        sqlx::Sqlite::create_database(&sql_lite_uri).await.unwrap();
        let warehouse_location = temp_path();

        let catalog = SqlCatalogBuilder::default()
            .uri(sql_lite_uri)
            .warehouse_location(warehouse_location)
            .load(
                "iceberg",
                HashMap::from_iter([(SQL_CATALOG_PROP_URI.to_string(), sql_lite_uri2)]),
            )
            .await;
        assert!(catalog.is_err());
    }

    /// Even when an invalid URI is specified in a builder method,
    /// it can be successfully overridden with a valid URI in props
    /// for catalog generation to succeed.
    #[tokio::test]
    async fn test_builder_props_set_valid_uri() {
        let sql_lite_uri = format!("sqlite:{}", temp_path());
        let sql_lite_uri2 = format!("sqlite:{}", temp_path());
        sqlx::Sqlite::create_database(&sql_lite_uri).await.unwrap();
        let warehouse_location = temp_path();

        let catalog = SqlCatalogBuilder::default()
            .uri(sql_lite_uri2)
            .warehouse_location(warehouse_location)
            .load(
                "iceberg",
                HashMap::from_iter([(SQL_CATALOG_PROP_URI.to_string(), sql_lite_uri.clone())]),
            )
            .await;
        assert!(catalog.is_ok());
    }

    /// values assigned via props take precedence
    #[tokio::test]
    async fn test_builder_props_take_precedence() {
        let sql_lite_uri = format!("sqlite:{}", temp_path());
        sqlx::Sqlite::create_database(&sql_lite_uri).await.unwrap();
        let warehouse_location = temp_path();
        let warehouse_location2 = temp_path();

        let catalog = SqlCatalogBuilder::default()
            .warehouse_location(warehouse_location2)
            .sql_bind_style(SqlBindStyle::DollarNumeric)
            .load(
                "iceberg",
                HashMap::from_iter([
                    (SQL_CATALOG_PROP_URI.to_string(), sql_lite_uri),
                    (
                        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
                        warehouse_location.clone(),
                    ),
                    (
                        SQL_CATALOG_PROP_BIND_STYLE.to_string(),
                        SqlBindStyle::QMark.to_string(),
                    ),
                ]),
            )
            .await;

        assert!(catalog.is_ok());

        let catalog = catalog.unwrap();
        assert!(catalog.warehouse_location == warehouse_location);
        assert!(catalog.sql_bind_style == SqlBindStyle::QMark);
    }

    /// values assigned via props take precedence
    #[tokio::test]
    async fn test_builder_props_take_precedence_props() {
        let sql_lite_uri = format!("sqlite:{}", temp_path());
        let sql_lite_uri2 = format!("sqlite:{}", temp_path());
        sqlx::Sqlite::create_database(&sql_lite_uri).await.unwrap();
        let warehouse_location = temp_path();
        let warehouse_location2 = temp_path();

        let props = HashMap::from_iter([
            (SQL_CATALOG_PROP_URI.to_string(), sql_lite_uri.clone()),
            (
                SQL_CATALOG_PROP_WAREHOUSE.to_string(),
                warehouse_location.clone(),
            ),
            (
                SQL_CATALOG_PROP_BIND_STYLE.to_string(),
                SqlBindStyle::QMark.to_string(),
            ),
        ]);
        let props2 = HashMap::from_iter([
            (SQL_CATALOG_PROP_URI.to_string(), sql_lite_uri2.clone()),
            (
                SQL_CATALOG_PROP_WAREHOUSE.to_string(),
                warehouse_location2.clone(),
            ),
            (
                SQL_CATALOG_PROP_BIND_STYLE.to_string(),
                SqlBindStyle::DollarNumeric.to_string(),
            ),
        ]);

        let catalog = SqlCatalogBuilder::default()
            .props(props2)
            .load("iceberg", props)
            .await;

        assert!(catalog.is_ok());

        let catalog = catalog.unwrap();
        assert!(catalog.warehouse_location == warehouse_location);
        assert!(catalog.sql_bind_style == SqlBindStyle::QMark);
    }

    /// values assigned via props take precedence
    #[tokio::test]
    async fn test_builder_props_take_precedence_prop() {
        let sql_lite_uri = format!("sqlite:{}", temp_path());
        let sql_lite_uri2 = format!("sqlite:{}", temp_path());
        sqlx::Sqlite::create_database(&sql_lite_uri).await.unwrap();
        let warehouse_location = temp_path();
        let warehouse_location2 = temp_path();

        let props = HashMap::from_iter([
            (SQL_CATALOG_PROP_URI.to_string(), sql_lite_uri.clone()),
            (
                SQL_CATALOG_PROP_WAREHOUSE.to_string(),
                warehouse_location.clone(),
            ),
            (
                SQL_CATALOG_PROP_BIND_STYLE.to_string(),
                SqlBindStyle::QMark.to_string(),
            ),
        ]);

        let catalog = SqlCatalogBuilder::default()
            .prop(SQL_CATALOG_PROP_URI.to_string(), sql_lite_uri2)
            .prop(SQL_CATALOG_PROP_WAREHOUSE.to_string(), warehouse_location2)
            .prop(
                SQL_CATALOG_PROP_BIND_STYLE.to_string(),
                SqlBindStyle::DollarNumeric.to_string(),
            )
            .load("iceberg", props)
            .await;

        assert!(catalog.is_ok());

        let catalog = catalog.unwrap();
        assert!(catalog.warehouse_location == warehouse_location);
        assert!(catalog.sql_bind_style == SqlBindStyle::QMark);
    }

    /// invalid value for `SqlBindStyle` causes catalog creation to fail
    #[tokio::test]
    async fn test_builder_props_invalid_bind_style_fails() {
        let sql_lite_uri = format!("sqlite:{}", temp_path());
        sqlx::Sqlite::create_database(&sql_lite_uri).await.unwrap();
        let warehouse_location = temp_path();

        let catalog = SqlCatalogBuilder::default()
            .load(
                "iceberg",
                HashMap::from_iter([
                    (SQL_CATALOG_PROP_URI.to_string(), sql_lite_uri),
                    (SQL_CATALOG_PROP_WAREHOUSE.to_string(), warehouse_location),
                    (SQL_CATALOG_PROP_BIND_STYLE.to_string(), "AAA".to_string()),
                ]),
            )
            .await;

        assert!(catalog.is_err());
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_empty_vector() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;

        assert_eq!(catalog.list_namespaces(None).await.unwrap(), vec![]);
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_multiple_namespaces() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let namespace_ident_1 = NamespaceIdent::new("a".into());
        let namespace_ident_2 = NamespaceIdent::new("b".into());
        create_namespaces(&catalog, &vec![&namespace_ident_1, &namespace_ident_2]).await;

        assert_eq!(
            to_set(catalog.list_namespaces(None).await.unwrap()),
            to_set(vec![namespace_ident_1, namespace_ident_2])
        );
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_only_top_level_namespaces() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let namespace_ident_1 = NamespaceIdent::new("a".into());
        let namespace_ident_2 = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_3 = NamespaceIdent::new("b".into());
        create_namespaces(&catalog, &vec![
            &namespace_ident_1,
            &namespace_ident_2,
            &namespace_ident_3,
        ])
        .await;

        assert_eq!(
            to_set(catalog.list_namespaces(None).await.unwrap()),
            to_set(vec![namespace_ident_1, namespace_ident_3])
        );
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_no_namespaces_under_parent() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let namespace_ident_1 = NamespaceIdent::new("a".into());
        let namespace_ident_2 = NamespaceIdent::new("b".into());
        create_namespaces(&catalog, &vec![&namespace_ident_1, &namespace_ident_2]).await;

        assert_eq!(
            catalog
                .list_namespaces(Some(&namespace_ident_1))
                .await
                .unwrap(),
            vec![]
        );
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_namespace_under_parent() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let namespace_ident_1 = NamespaceIdent::new("a".into());
        let namespace_ident_2 = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_3 = NamespaceIdent::new("c".into());
        create_namespaces(&catalog, &vec![
            &namespace_ident_1,
            &namespace_ident_2,
            &namespace_ident_3,
        ])
        .await;

        assert_eq!(
            to_set(catalog.list_namespaces(None).await.unwrap()),
            to_set(vec![namespace_ident_1.clone(), namespace_ident_3])
        );

        assert_eq!(
            catalog
                .list_namespaces(Some(&namespace_ident_1))
                .await
                .unwrap(),
            vec![NamespaceIdent::from_strs(vec!["a", "b"]).unwrap()]
        );
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_multiple_namespaces_under_parent() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let namespace_ident_1 = NamespaceIdent::new("a".to_string());
        let namespace_ident_2 = NamespaceIdent::from_strs(vec!["a", "a"]).unwrap();
        let namespace_ident_3 = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_4 = NamespaceIdent::from_strs(vec!["a", "c"]).unwrap();
        let namespace_ident_5 = NamespaceIdent::new("b".into());
        create_namespaces(&catalog, &vec![
            &namespace_ident_1,
            &namespace_ident_2,
            &namespace_ident_3,
            &namespace_ident_4,
            &namespace_ident_5,
        ])
        .await;

        assert_eq!(
            to_set(
                catalog
                    .list_namespaces(Some(&namespace_ident_1))
                    .await
                    .unwrap()
            ),
            to_set(vec![
                NamespaceIdent::from_strs(vec!["a", "a"]).unwrap(),
                NamespaceIdent::from_strs(vec!["a", "b"]).unwrap(),
                NamespaceIdent::from_strs(vec!["a", "c"]).unwrap(),
            ])
        );
    }

    #[tokio::test]
    async fn test_namespace_exists_returns_false() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        assert!(
            !catalog
                .namespace_exists(&NamespaceIdent::new("b".into()))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_namespace_exists_returns_true() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        assert!(catalog.namespace_exists(&namespace_ident).await.unwrap());
    }

    #[tokio::test]
    async fn test_create_namespace_with_properties() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let namespace_ident = NamespaceIdent::new("abc".into());

        let mut properties = default_properties();
        properties.insert("k".into(), "v".into());

        assert_eq!(
            catalog
                .create_namespace(&namespace_ident, properties.clone())
                .await
                .unwrap(),
            Namespace::with_properties(namespace_ident.clone(), properties.clone())
        );

        assert_eq!(
            catalog.get_namespace(&namespace_ident).await.unwrap(),
            Namespace::with_properties(namespace_ident, properties)
        );
    }

    #[tokio::test]
    async fn test_create_namespace_throws_error_if_namespace_already_exists() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        assert_eq!(
            catalog
                .create_namespace(&namespace_ident, HashMap::new())
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => Namespace {:?} already exists",
                &namespace_ident
            )
        );

        assert_eq!(
            catalog.get_namespace(&namespace_ident).await.unwrap(),
            Namespace::with_properties(namespace_ident, default_properties())
        );
    }

    #[tokio::test]
    async fn test_create_nested_namespace() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let parent_namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &parent_namespace_ident).await;

        let child_namespace_ident = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();

        assert_eq!(
            catalog
                .create_namespace(&child_namespace_ident, HashMap::new())
                .await
                .unwrap(),
            Namespace::new(child_namespace_ident.clone())
        );

        assert_eq!(
            catalog.get_namespace(&child_namespace_ident).await.unwrap(),
            Namespace::with_properties(child_namespace_ident, default_properties())
        );
    }

    #[tokio::test]
    async fn test_create_deeply_nested_namespace() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]).await;

        let namespace_ident_a_b_c = NamespaceIdent::from_strs(vec!["a", "b", "c"]).unwrap();

        assert_eq!(
            catalog
                .create_namespace(&namespace_ident_a_b_c, HashMap::new())
                .await
                .unwrap(),
            Namespace::new(namespace_ident_a_b_c.clone())
        );

        assert_eq!(
            catalog.get_namespace(&namespace_ident_a_b_c).await.unwrap(),
            Namespace::with_properties(namespace_ident_a_b_c, default_properties())
        );
    }

    #[tokio::test]
    async fn test_update_namespace_noop() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        catalog
            .update_namespace(&namespace_ident, HashMap::new())
            .await
            .unwrap();

        assert_eq!(
            *catalog
                .get_namespace(&namespace_ident)
                .await
                .unwrap()
                .properties(),
            HashMap::from_iter([("exists".to_string(), "true".to_string())])
        )
    }

    #[tokio::test]
    async fn test_update_namespace() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        let mut props = HashMap::from_iter([
            ("prop1".to_string(), "val1".to_string()),
            ("prop2".into(), "val2".into()),
        ]);

        catalog
            .update_namespace(&namespace_ident, props.clone())
            .await
            .unwrap();

        props.insert("exists".into(), "true".into());

        assert_eq!(
            *catalog
                .get_namespace(&namespace_ident)
                .await
                .unwrap()
                .properties(),
            props
        )
    }

    #[tokio::test]
    async fn test_update_nested_namespace() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let namespace_ident = NamespaceIdent::from_strs(["a", "b"]).unwrap();
        create_namespace(&catalog, &namespace_ident).await;

        let mut props = HashMap::from_iter([
            ("prop1".to_string(), "val1".to_string()),
            ("prop2".into(), "val2".into()),
        ]);

        catalog
            .update_namespace(&namespace_ident, props.clone())
            .await
            .unwrap();

        props.insert("exists".into(), "true".into());

        assert_eq!(
            *catalog
                .get_namespace(&namespace_ident)
                .await
                .unwrap()
                .properties(),
            props
        )
    }

    #[tokio::test]
    async fn test_update_namespace_errors_if_namespace_doesnt_exist() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let namespace_ident = NamespaceIdent::new("a".into());

        let props = HashMap::from_iter([
            ("prop1".to_string(), "val1".to_string()),
            ("prop2".into(), "val2".into()),
        ]);

        let err = catalog
            .update_namespace(&namespace_ident, props)
            .await
            .unwrap_err();

        assert_eq!(
            err.message(),
            format!("No such namespace: {namespace_ident:?}")
        );
    }

    #[tokio::test]
    async fn test_update_namespace_errors_if_nested_namespace_doesnt_exist() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let namespace_ident = NamespaceIdent::from_strs(["a", "b"]).unwrap();

        let props = HashMap::from_iter([
            ("prop1".to_string(), "val1".to_string()),
            ("prop2".into(), "val2".into()),
        ]);

        let err = catalog
            .update_namespace(&namespace_ident, props)
            .await
            .unwrap_err();

        assert_eq!(
            err.message(),
            format!("No such namespace: {namespace_ident:?}")
        );
    }

    #[tokio::test]
    async fn test_drop_namespace() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let namespace_ident = NamespaceIdent::new("abc".into());
        create_namespace(&catalog, &namespace_ident).await;

        catalog.drop_namespace(&namespace_ident).await.unwrap();

        assert!(!catalog.namespace_exists(&namespace_ident).await.unwrap())
    }

    #[tokio::test]
    async fn test_drop_nested_namespace() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]).await;

        catalog.drop_namespace(&namespace_ident_a_b).await.unwrap();

        assert!(
            !catalog
                .namespace_exists(&namespace_ident_a_b)
                .await
                .unwrap()
        );

        assert!(catalog.namespace_exists(&namespace_ident_a).await.unwrap());
    }

    #[tokio::test]
    async fn test_drop_deeply_nested_namespace() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_a_b_c = NamespaceIdent::from_strs(vec!["a", "b", "c"]).unwrap();
        create_namespaces(&catalog, &vec![
            &namespace_ident_a,
            &namespace_ident_a_b,
            &namespace_ident_a_b_c,
        ])
        .await;

        catalog
            .drop_namespace(&namespace_ident_a_b_c)
            .await
            .unwrap();

        assert!(
            !catalog
                .namespace_exists(&namespace_ident_a_b_c)
                .await
                .unwrap()
        );

        assert!(
            catalog
                .namespace_exists(&namespace_ident_a_b)
                .await
                .unwrap()
        );

        assert!(catalog.namespace_exists(&namespace_ident_a).await.unwrap());
    }

    #[tokio::test]
    async fn test_drop_namespace_throws_error_if_namespace_doesnt_exist() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;

        let non_existent_namespace_ident = NamespaceIdent::new("abc".into());
        assert_eq!(
            catalog
                .drop_namespace(&non_existent_namespace_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!("Unexpected => No such namespace: {non_existent_namespace_ident:?}")
        )
    }

    #[tokio::test]
    async fn test_drop_namespace_throws_error_if_nested_namespace_doesnt_exist() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        create_namespace(&catalog, &NamespaceIdent::new("a".into())).await;

        let non_existent_namespace_ident =
            NamespaceIdent::from_vec(vec!["a".into(), "b".into()]).unwrap();
        assert_eq!(
            catalog
                .drop_namespace(&non_existent_namespace_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!("Unexpected => No such namespace: {non_existent_namespace_ident:?}")
        )
    }

    #[tokio::test]
    async fn test_dropping_a_namespace_does_not_drop_namespaces_nested_under_that_one() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]).await;

        catalog.drop_namespace(&namespace_ident_a).await.unwrap();

        assert!(!catalog.namespace_exists(&namespace_ident_a).await.unwrap());

        assert!(
            catalog
                .namespace_exists(&namespace_ident_a_b)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_list_tables_returns_empty_vector() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        assert_eq!(catalog.list_tables(&namespace_ident).await.unwrap(), vec![]);
    }

    #[tokio::test]
    async fn test_list_tables_throws_error_if_namespace_doesnt_exist() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;

        let non_existent_namespace_ident = NamespaceIdent::new("n1".into());

        assert_eq!(
            catalog
                .list_tables(&non_existent_namespace_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!("Unexpected => No such namespace: {non_existent_namespace_ident:?}"),
        );
    }

    #[tokio::test]
    async fn test_create_table_with_location() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc.clone()).await;
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        let table_name = "abc";
        let location = warehouse_loc.clone();
        let table_creation = TableCreation::builder()
            .name(table_name.into())
            .location(location.clone())
            .schema(simple_table_schema())
            .build();

        let expected_table_ident = TableIdent::new(namespace_ident.clone(), table_name.into());

        assert_table_eq(
            &catalog
                .create_table(&namespace_ident, table_creation)
                .await
                .unwrap(),
            &expected_table_ident,
            &simple_table_schema(),
        );

        let table = catalog.load_table(&expected_table_ident).await.unwrap();

        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());

        assert!(
            table
                .metadata_location()
                .unwrap()
                .to_string()
                .starts_with(&location)
        )
    }

    #[tokio::test]
    async fn test_create_table_falls_back_to_namespace_location_if_table_location_is_missing() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;

        let namespace_ident = NamespaceIdent::new("a".into());
        let mut namespace_properties = HashMap::new();
        let namespace_location = temp_path();
        namespace_properties.insert(
            NAMESPACE_LOCATION_PROPERTY_KEY.to_string(),
            namespace_location.to_string(),
        );
        catalog
            .create_namespace(&namespace_ident, namespace_properties)
            .await
            .unwrap();

        let table_name = "tbl1";
        let expected_table_ident = TableIdent::new(namespace_ident.clone(), table_name.into());
        let expected_table_metadata_location_regex =
            format!("^{namespace_location}/tbl1/metadata/00000-{UUID_REGEX_STR}.metadata.json$",);

        let table = catalog
            .create_table(
                &namespace_ident,
                TableCreation::builder()
                    .name(table_name.into())
                    .schema(simple_table_schema())
                    // no location specified for table
                    .build(),
            )
            .await
            .unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(&table, &expected_table_metadata_location_regex);

        let table = catalog.load_table(&expected_table_ident).await.unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(&table, &expected_table_metadata_location_regex);
    }

    #[tokio::test]
    async fn test_create_table_in_nested_namespace_falls_back_to_nested_namespace_location_if_table_location_is_missing()
     {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;

        let namespace_ident = NamespaceIdent::new("a".into());
        let mut namespace_properties = HashMap::new();
        let namespace_location = temp_path();
        namespace_properties.insert(
            NAMESPACE_LOCATION_PROPERTY_KEY.to_string(),
            namespace_location.to_string(),
        );
        catalog
            .create_namespace(&namespace_ident, namespace_properties)
            .await
            .unwrap();

        let nested_namespace_ident = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let mut nested_namespace_properties = HashMap::new();
        let nested_namespace_location = temp_path();
        nested_namespace_properties.insert(
            NAMESPACE_LOCATION_PROPERTY_KEY.to_string(),
            nested_namespace_location.to_string(),
        );
        catalog
            .create_namespace(&nested_namespace_ident, nested_namespace_properties)
            .await
            .unwrap();

        let table_name = "tbl1";
        let expected_table_ident =
            TableIdent::new(nested_namespace_ident.clone(), table_name.into());
        let expected_table_metadata_location_regex = format!(
            "^{nested_namespace_location}/tbl1/metadata/00000-{UUID_REGEX_STR}.metadata.json$",
        );

        let table = catalog
            .create_table(
                &nested_namespace_ident,
                TableCreation::builder()
                    .name(table_name.into())
                    .schema(simple_table_schema())
                    // no location specified for table
                    .build(),
            )
            .await
            .unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(&table, &expected_table_metadata_location_regex);

        let table = catalog.load_table(&expected_table_ident).await.unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(&table, &expected_table_metadata_location_regex);
    }

    #[tokio::test]
    async fn test_create_table_falls_back_to_warehouse_location_if_both_table_location_and_namespace_location_are_missing()
     {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc.clone()).await;

        let namespace_ident = NamespaceIdent::new("a".into());
        // note: no location specified in namespace_properties
        let namespace_properties = HashMap::new();
        catalog
            .create_namespace(&namespace_ident, namespace_properties)
            .await
            .unwrap();

        let table_name = "tbl1";
        let expected_table_ident = TableIdent::new(namespace_ident.clone(), table_name.into());
        let expected_table_metadata_location_regex =
            format!("^{warehouse_loc}/a/tbl1/metadata/00000-{UUID_REGEX_STR}.metadata.json$");

        let table = catalog
            .create_table(
                &namespace_ident,
                TableCreation::builder()
                    .name(table_name.into())
                    .schema(simple_table_schema())
                    // no location specified for table
                    .build(),
            )
            .await
            .unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(&table, &expected_table_metadata_location_regex);

        let table = catalog.load_table(&expected_table_ident).await.unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(&table, &expected_table_metadata_location_regex);
    }

    #[tokio::test]
    async fn test_create_table_in_nested_namespace_falls_back_to_warehouse_location_if_both_table_location_and_namespace_location_are_missing()
     {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc.clone()).await;

        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        let nested_namespace_ident = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespace(&catalog, &nested_namespace_ident).await;

        let table_name = "tbl1";
        let expected_table_ident =
            TableIdent::new(nested_namespace_ident.clone(), table_name.into());
        let expected_table_metadata_location_regex =
            format!("^{warehouse_loc}/a/b/tbl1/metadata/00000-{UUID_REGEX_STR}.metadata.json$");

        let table = catalog
            .create_table(
                &nested_namespace_ident,
                TableCreation::builder()
                    .name(table_name.into())
                    .schema(simple_table_schema())
                    // no location specified for table
                    .build(),
            )
            .await
            .unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(&table, &expected_table_metadata_location_regex);

        let table = catalog.load_table(&expected_table_ident).await.unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(&table, &expected_table_metadata_location_regex);
    }

    #[tokio::test]
    async fn test_create_table_throws_error_if_table_with_same_name_already_exists() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc.clone()).await;
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;
        let table_name = "tbl1";
        let table_ident = TableIdent::new(namespace_ident.clone(), table_name.into());
        create_table(&catalog, &table_ident).await;

        let tmp_dir = TempDir::new().unwrap();
        let location = tmp_dir.path().to_str().unwrap().to_string();

        assert_eq!(
            catalog
                .create_table(
                    &namespace_ident,
                    TableCreation::builder()
                        .name(table_name.into())
                        .schema(simple_table_schema())
                        .location(location)
                        .build()
                )
                .await
                .unwrap_err()
                .to_string(),
            format!("Unexpected => Table {:?} already exists.", &table_ident)
        );
    }

    #[tokio::test]
    async fn test_rename_table_in_same_namespace() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;
        let src_table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        let dst_table_ident = TableIdent::new(namespace_ident.clone(), "tbl2".into());
        create_table(&catalog, &src_table_ident).await;

        catalog
            .rename_table(&src_table_ident, &dst_table_ident)
            .await
            .unwrap();

        assert_eq!(catalog.list_tables(&namespace_ident).await.unwrap(), vec![
            dst_table_ident
        ],);
    }

    #[tokio::test]
    async fn test_rename_table_across_namespaces() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let src_namespace_ident = NamespaceIdent::new("a".into());
        let dst_namespace_ident = NamespaceIdent::new("b".into());
        create_namespaces(&catalog, &vec![&src_namespace_ident, &dst_namespace_ident]).await;
        let src_table_ident = TableIdent::new(src_namespace_ident.clone(), "tbl1".into());
        let dst_table_ident = TableIdent::new(dst_namespace_ident.clone(), "tbl2".into());
        create_table(&catalog, &src_table_ident).await;

        catalog
            .rename_table(&src_table_ident, &dst_table_ident)
            .await
            .unwrap();

        assert_eq!(
            catalog.list_tables(&src_namespace_ident).await.unwrap(),
            vec![],
        );

        assert_eq!(
            catalog.list_tables(&dst_namespace_ident).await.unwrap(),
            vec![dst_table_ident],
        );
    }

    #[tokio::test]
    async fn test_rename_table_src_table_is_same_as_dst_table() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;
        let table_ident = TableIdent::new(namespace_ident.clone(), "tbl".into());
        create_table(&catalog, &table_ident).await;

        catalog
            .rename_table(&table_ident, &table_ident)
            .await
            .unwrap();

        assert_eq!(catalog.list_tables(&namespace_ident).await.unwrap(), vec![
            table_ident
        ],);
    }

    #[tokio::test]
    async fn test_rename_table_across_nested_namespaces() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_a_b_c = NamespaceIdent::from_strs(vec!["a", "b", "c"]).unwrap();
        create_namespaces(&catalog, &vec![
            &namespace_ident_a,
            &namespace_ident_a_b,
            &namespace_ident_a_b_c,
        ])
        .await;

        let src_table_ident = TableIdent::new(namespace_ident_a_b_c.clone(), "tbl1".into());
        create_tables(&catalog, vec![&src_table_ident]).await;

        let dst_table_ident = TableIdent::new(namespace_ident_a_b.clone(), "tbl1".into());
        catalog
            .rename_table(&src_table_ident, &dst_table_ident)
            .await
            .unwrap();

        assert!(!catalog.table_exists(&src_table_ident).await.unwrap());

        assert!(catalog.table_exists(&dst_table_ident).await.unwrap());
    }

    #[tokio::test]
    async fn test_rename_table_throws_error_if_dst_namespace_doesnt_exist() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let src_namespace_ident = NamespaceIdent::new("n1".into());
        let src_table_ident = TableIdent::new(src_namespace_ident.clone(), "tbl1".into());
        create_namespace(&catalog, &src_namespace_ident).await;
        create_table(&catalog, &src_table_ident).await;

        let non_existent_dst_namespace_ident = NamespaceIdent::new("n2".into());
        let dst_table_ident =
            TableIdent::new(non_existent_dst_namespace_ident.clone(), "tbl1".into());
        assert_eq!(
            catalog
                .rename_table(&src_table_ident, &dst_table_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!("Unexpected => No such namespace: {non_existent_dst_namespace_ident:?}"),
        );
    }

    #[tokio::test]
    async fn test_rename_table_throws_error_if_src_table_doesnt_exist() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;
        let src_table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        let dst_table_ident = TableIdent::new(namespace_ident.clone(), "tbl2".into());

        assert_eq!(
            catalog
                .rename_table(&src_table_ident, &dst_table_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!("Unexpected => No such table: {src_table_ident:?}"),
        );
    }

    #[tokio::test]
    async fn test_rename_table_throws_error_if_dst_table_already_exists() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;
        let src_table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        let dst_table_ident = TableIdent::new(namespace_ident.clone(), "tbl2".into());
        create_tables(&catalog, vec![&src_table_ident, &dst_table_ident]).await;

        assert_eq!(
            catalog
                .rename_table(&src_table_ident, &dst_table_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!("Unexpected => Table {:?} already exists.", &dst_table_ident),
        );
    }

    #[tokio::test]
    async fn test_drop_table_throws_error_if_table_not_exist() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc.clone()).await;
        let namespace_ident = NamespaceIdent::new("a".into());
        let table_name = "tbl1";
        let table_ident = TableIdent::new(namespace_ident.clone(), table_name.into());
        create_namespace(&catalog, &namespace_ident).await;

        let err = catalog
            .drop_table(&table_ident)
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(
            err,
            "Unexpected => No such table: TableIdent { namespace: NamespaceIdent([\"a\"]), name: \"tbl1\" }"
        );
    }

    #[tokio::test]
    async fn test_drop_table() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc.clone()).await;
        let namespace_ident = NamespaceIdent::new("a".into());
        let table_name = "tbl1";
        let table_ident = TableIdent::new(namespace_ident.clone(), table_name.into());
        create_namespace(&catalog, &namespace_ident).await;

        let location = warehouse_loc.clone();
        let table_creation = TableCreation::builder()
            .name(table_name.into())
            .location(location.clone())
            .schema(simple_table_schema())
            .build();

        catalog
            .create_table(&namespace_ident, table_creation)
            .await
            .unwrap();

        let table = catalog.load_table(&table_ident).await.unwrap();
        assert_table_eq(&table, &table_ident, &simple_table_schema());

        catalog.drop_table(&table_ident).await.unwrap();
        let err = catalog
            .load_table(&table_ident)
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(
            err,
            "Unexpected => No such table: TableIdent { namespace: NamespaceIdent([\"a\"]), name: \"tbl1\" }"
        );
    }

    #[tokio::test]
    async fn test_register_table_throws_error_if_table_with_same_name_already_exists() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc.clone()).await;
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;
        let table_name = "tbl1";
        let table_ident = TableIdent::new(namespace_ident.clone(), table_name.into());
        create_table(&catalog, &table_ident).await;

        assert_eq!(
            catalog
                .register_table(&table_ident, warehouse_loc)
                .await
                .unwrap_err()
                .to_string(),
            format!("Unexpected => Table {:?} already exists.", &table_ident)
        );
    }

    #[tokio::test]
    async fn test_register_table() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc.clone()).await;
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        let table_name = "abc";
        let location = warehouse_loc.clone();
        let table_creation = TableCreation::builder()
            .name(table_name.into())
            .location(location.clone())
            .schema(simple_table_schema())
            .build();

        let table_ident = TableIdent::new(namespace_ident.clone(), table_name.into());
        let expected_table = catalog
            .create_table(&namespace_ident, table_creation)
            .await
            .unwrap();

        let metadata_location = expected_table
            .metadata_location()
            .expect("Expected metadata location to be set")
            .to_string();

        assert_table_eq(&expected_table, &table_ident, &simple_table_schema());

        let _ = catalog.drop_table(&table_ident).await;

        let table = catalog
            .register_table(&table_ident, metadata_location.clone())
            .await
            .unwrap();

        assert_eq!(table.identifier(), expected_table.identifier());
        assert_eq!(table.metadata_location(), Some(metadata_location.as_str()));
    }

    #[tokio::test]
    async fn test_update_table() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;

        // Create a test namespace and table
        let namespace_ident = NamespaceIdent::new("ns1".into());
        create_namespace(&catalog, &namespace_ident).await;
        let table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        create_table(&catalog, &table_ident).await;

        let table = catalog.load_table(&table_ident).await.unwrap();

        // Store the original metadata location for comparison
        let original_metadata_location = table.metadata_location().unwrap().to_string();

        // Create a transaction to update the table
        let tx = Transaction::new(&table);
        let tx = tx
            .update_table_properties()
            .set("test_property".to_string(), "test_value".to_string())
            .apply(tx)
            .unwrap();

        // Commit the transaction to the catalog
        let updated_table = tx.commit(&catalog).await.unwrap();

        // Verify the update was successful
        assert_eq!(
            updated_table.metadata().properties().get("test_property"),
            Some(&"test_value".to_string())
        );
        // Verify the metadata location has been updated
        assert_ne!(
            updated_table.metadata_location().unwrap(),
            original_metadata_location.as_str()
        );

        // Load the table again from the catalog to verify changes were persisted
        let reloaded = catalog.load_table(&table_ident).await.unwrap();

        // Verify the reloaded table matches the updated table
        assert_eq!(
            reloaded.metadata().properties().get("test_property"),
            Some(&"test_value".to_string())
        );
        assert_eq!(
            reloaded.metadata_location(),
            updated_table.metadata_location()
        );
    }
}
