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

use std::collections::HashMap;
use std::future::Future;

use async_trait::async_trait;
use aws_sdk_s3tables::operation::create_table::CreateTableOutput;
use aws_sdk_s3tables::operation::get_namespace::GetNamespaceOutput;
use aws_sdk_s3tables::operation::get_table::GetTableOutput;
use aws_sdk_s3tables::operation::list_tables::ListTablesOutput;
use aws_sdk_s3tables::operation::update_table_metadata_location::UpdateTableMetadataLocationError;
use aws_sdk_s3tables::types::OpenTableFormat;
use iceberg::io::{FileIO, FileIOBuilder};
use iceberg::spec::{TableMetadata, TableMetadataBuilder};
use iceberg::table::Table;
use iceberg::{
    Catalog, CatalogBuilder, Error, ErrorKind, MetadataLocation, Namespace, NamespaceIdent, Result,
    TableCommit, TableCreation, TableIdent,
};

use crate::utils::create_sdk_config;

/// S3Tables table bucket ARN property
pub const S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN: &str = "table_bucket_arn";
/// S3Tables endpoint URL property
pub const S3TABLES_CATALOG_PROP_ENDPOINT_URL: &str = "endpoint_url";

/// S3Tables catalog configuration.
#[derive(Debug)]
struct S3TablesCatalogConfig {
    /// Catalog name.
    name: Option<String>,
    /// Unlike other buckets, S3Tables bucket is not a physical bucket, but a virtual bucket
    /// that is managed by s3tables. We can't directly access the bucket with path like
    /// s3://{bucket_name}/{file_path}, all the operations are done with respect of the bucket
    /// ARN.
    table_bucket_arn: String,
    /// Endpoint URL for the catalog.
    endpoint_url: Option<String>,
    /// Optional pre-configured AWS SDK client for S3Tables.
    client: Option<aws_sdk_s3tables::Client>,
    /// Properties for the catalog. The available properties are:
    /// - `profile_name`: The name of the AWS profile to use.
    /// - `region_name`: The AWS region to use.
    /// - `aws_access_key_id`: The AWS access key ID to use.
    /// - `aws_secret_access_key`: The AWS secret access key to use.
    /// - `aws_session_token`: The AWS session token to use.
    props: HashMap<String, String>,
}

/// Builder for [`S3TablesCatalog`].
#[derive(Debug)]
pub struct S3TablesCatalogBuilder(S3TablesCatalogConfig);

/// Default builder for [`S3TablesCatalog`].
impl Default for S3TablesCatalogBuilder {
    fn default() -> Self {
        Self(S3TablesCatalogConfig {
            name: None,
            table_bucket_arn: "".to_string(),
            endpoint_url: None,
            client: None,
            props: HashMap::new(),
        })
    }
}

/// Builder methods for [`S3TablesCatalog`].
impl S3TablesCatalogBuilder {
    /// Configure the catalog with a custom endpoint URL (useful for local testing/mocking).
    ///
    /// # Behavior with Properties
    ///
    /// If both this method and the `endpoint_url` property are provided during catalog loading,
    /// the property value will take precedence and overwrite the value set by this method.
    /// This follows the general pattern where properties specified in the `load()` method
    /// have higher priority than builder method configurations.
    pub fn with_endpoint_url(mut self, endpoint_url: impl Into<String>) -> Self {
        self.0.endpoint_url = Some(endpoint_url.into());
        self
    }

    /// Configure the catalog with a pre-built AWS SDK client.
    pub fn with_client(mut self, client: aws_sdk_s3tables::Client) -> Self {
        self.0.client = Some(client);
        self
    }

    /// Configure the catalog with a table bucket ARN.
    ///
    /// # Behavior with Properties
    ///
    /// If both this method and the `table_bucket_arn` property are provided during catalog loading,
    /// the property value will take precedence and overwrite the value set by this method.
    /// This follows the general pattern where properties specified in the `load()` method
    /// have higher priority than builder method configurations.
    pub fn with_table_bucket_arn(mut self, table_bucket_arn: impl Into<String>) -> Self {
        self.0.table_bucket_arn = table_bucket_arn.into();
        self
    }
}

impl CatalogBuilder for S3TablesCatalogBuilder {
    type C = S3TablesCatalog;

    fn load(
        mut self,
        name: impl Into<String>,
        props: HashMap<String, String>,
    ) -> impl Future<Output = Result<Self::C>> + Send {
        let catalog_name = name.into();
        self.0.name = Some(catalog_name.clone());

        if props.contains_key(S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN) {
            self.0.table_bucket_arn = props
                .get(S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN)
                .cloned()
                .unwrap_or_default();
        }

        if props.contains_key(S3TABLES_CATALOG_PROP_ENDPOINT_URL) {
            self.0.endpoint_url = props.get(S3TABLES_CATALOG_PROP_ENDPOINT_URL).cloned();
        }

        // Collect other remaining properties
        self.0.props = props
            .into_iter()
            .filter(|(k, _)| {
                k != S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN
                    && k != S3TABLES_CATALOG_PROP_ENDPOINT_URL
            })
            .collect();

        async move {
            if catalog_name.trim().is_empty() {
                Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Catalog name cannot be empty",
                ))
            } else if self.0.table_bucket_arn.is_empty() {
                Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Table bucket ARN is required",
                ))
            } else {
                S3TablesCatalog::new(self.0).await
            }
        }
    }
}

/// S3Tables catalog implementation.
#[derive(Debug)]
pub struct S3TablesCatalog {
    config: S3TablesCatalogConfig,
    s3tables_client: aws_sdk_s3tables::Client,
    file_io: FileIO,
}

impl S3TablesCatalog {
    /// Creates a new S3Tables catalog.
    async fn new(config: S3TablesCatalogConfig) -> Result<Self> {
        let s3tables_client = if let Some(client) = config.client.clone() {
            client
        } else {
            let aws_config = create_sdk_config(&config.props, config.endpoint_url.clone()).await;
            aws_sdk_s3tables::Client::new(&aws_config)
        };

        let file_io = FileIOBuilder::new("s3").with_props(&config.props).build()?;

        Ok(Self {
            config,
            s3tables_client,
            file_io,
        })
    }

    async fn load_table_with_version_token(
        &self,
        table_ident: &TableIdent,
    ) -> Result<(Table, String)> {
        let req = self
            .s3tables_client
            .get_table()
            .table_bucket_arn(self.config.table_bucket_arn.clone())
            .namespace(table_ident.namespace().to_url_string())
            .name(table_ident.name());
        let resp: GetTableOutput = req.send().await.map_err(from_aws_sdk_error)?;

        // when a table is created, it's possible that the metadata location is not set.
        let metadata_location = resp.metadata_location().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                format!(
                    "Table {} does not have metadata location",
                    table_ident.name()
                ),
            )
        })?;
        let metadata = TableMetadata::read_from(&self.file_io, metadata_location).await?;

        let table = Table::builder()
            .identifier(table_ident.clone())
            .metadata(metadata)
            .metadata_location(metadata_location)
            .file_io(self.file_io.clone())
            .build()?;
        Ok((table, resp.version_token))
    }
}

#[async_trait]
impl Catalog for S3TablesCatalog {
    /// List namespaces from s3tables catalog.
    ///
    /// S3Tables doesn't support nested namespaces. If parent is provided, it will
    /// return an empty list.
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        if parent.is_some() {
            return Ok(vec![]);
        }

        let mut result = Vec::new();
        let mut continuation_token = None;
        loop {
            let mut req = self
                .s3tables_client
                .list_namespaces()
                .table_bucket_arn(self.config.table_bucket_arn.clone());
            if let Some(token) = continuation_token {
                req = req.continuation_token(token);
            }
            let resp = req.send().await.map_err(from_aws_sdk_error)?;
            for ns in resp.namespaces() {
                result.push(NamespaceIdent::from_vec(ns.namespace().to_vec())?);
            }
            continuation_token = resp.continuation_token().map(|s| s.to_string());
            if continuation_token.is_none() {
                break;
            }
        }
        Ok(result)
    }

    /// Creates a new namespace with the given identifier and properties.
    ///
    /// Attempts to create a namespace defined by the `namespace`. The `properties`
    /// parameter is ignored.
    ///
    /// The following naming rules apply to namespaces:
    ///
    /// - Names must be between 3 (min) and 63 (max) characters long.
    /// - Names can consist only of lowercase letters, numbers, and underscores (_).
    /// - Names must begin and end with a letter or number.
    /// - Names must not contain hyphens (-) or periods (.).
    ///
    /// This function can return an error in the following situations:
    ///
    /// - Errors from the underlying database creation process, converted using
    /// `from_aws_sdk_error`.
    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        let req = self
            .s3tables_client
            .create_namespace()
            .table_bucket_arn(self.config.table_bucket_arn.clone())
            .namespace(namespace.to_url_string());
        req.send().await.map_err(from_aws_sdk_error)?;
        Ok(Namespace::with_properties(
            namespace.clone(),
            HashMap::new(),
        ))
    }

    /// Retrieves a namespace by its identifier.
    ///
    /// Validates the given namespace identifier and then queries the
    /// underlying database client to fetch the corresponding namespace data.
    /// Constructs a `Namespace` object with the retrieved data and returns it.
    ///
    /// This function can return an error in any of the following situations:
    /// - If there is an error querying the database, returned by
    /// `from_aws_sdk_error`.
    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        let req = self
            .s3tables_client
            .get_namespace()
            .table_bucket_arn(self.config.table_bucket_arn.clone())
            .namespace(namespace.to_url_string());
        let resp: GetNamespaceOutput = req.send().await.map_err(from_aws_sdk_error)?;
        let properties = HashMap::new();
        Ok(Namespace::with_properties(
            NamespaceIdent::from_vec(resp.namespace().to_vec())?,
            properties,
        ))
    }

    /// Checks if a namespace exists within the s3tables catalog.
    ///
    /// Validates the namespace identifier by querying the s3tables catalog
    /// to determine if the specified namespace exists.
    ///
    /// # Returns
    /// A `Result<bool>` indicating the outcome of the check:
    /// - `Ok(true)` if the namespace exists.
    /// - `Ok(false)` if the namespace does not exist, identified by a specific
    /// `IsNotFoundException` variant.
    /// - `Err(...)` if an error occurs during validation or the s3tables catalog
    /// query, with the error encapsulating the issue.
    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        let req = self
            .s3tables_client
            .get_namespace()
            .table_bucket_arn(self.config.table_bucket_arn.clone())
            .namespace(namespace.to_url_string());
        match req.send().await {
            Ok(_) => Ok(true),
            Err(err) => {
                if err.as_service_error().map(|e| e.is_not_found_exception()) == Some(true) {
                    Ok(false)
                } else {
                    Err(from_aws_sdk_error(err))
                }
            }
        }
    }

    /// Updates the properties of an existing namespace.
    ///
    /// S3Tables doesn't support updating namespace properties, so this function
    /// will always return an error.
    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Update namespace is not supported for s3tables catalog",
        ))
    }

    /// Drops an existing namespace from the s3tables catalog.
    ///
    /// Validates the namespace identifier and then deletes the corresponding
    /// namespace from the s3tables catalog.
    ///
    /// This function can return an error in the following situations:
    /// - Errors from the underlying database deletion process, converted using
    /// `from_aws_sdk_error`.
    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        let req = self
            .s3tables_client
            .delete_namespace()
            .table_bucket_arn(self.config.table_bucket_arn.clone())
            .namespace(namespace.to_url_string());
        req.send().await.map_err(from_aws_sdk_error)?;
        Ok(())
    }

    /// Lists all tables within a given namespace.
    ///
    /// Retrieves all tables associated with the specified namespace and returns
    /// their identifiers.
    ///
    /// This function can return an error in the following situations:
    /// - Errors from the underlying database query process, converted using
    /// `from_aws_sdk_error`.
    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        let mut result = Vec::new();
        let mut continuation_token = None;
        loop {
            let mut req = self
                .s3tables_client
                .list_tables()
                .table_bucket_arn(self.config.table_bucket_arn.clone())
                .namespace(namespace.to_url_string());
            if let Some(token) = continuation_token {
                req = req.continuation_token(token);
            }
            let resp: ListTablesOutput = req.send().await.map_err(from_aws_sdk_error)?;
            for table in resp.tables() {
                result.push(TableIdent::new(
                    NamespaceIdent::from_vec(table.namespace().to_vec())?,
                    table.name().to_string(),
                ));
            }
            continuation_token = resp.continuation_token().map(|s| s.to_string());
            if continuation_token.is_none() {
                break;
            }
        }
        Ok(result)
    }

    /// Creates a new table within a specified namespace.
    ///
    /// Attempts to create a table defined by the `creation` parameter. The metadata
    /// location is generated by the s3tables catalog, looks like:
    ///
    /// s3://{RANDOM WAREHOUSE LOCATION}/metadata/{VERSION}-{UUID}.metadata.json
    ///
    /// We have to get this random warehouse location after the table is created.
    ///
    /// This function can return an error in the following situations:
    /// - If the location of the table is set by user, identified by a specific
    /// `DataInvalid` variant.
    /// - Errors from the underlying database creation process, converted using
    /// `from_aws_sdk_error`.
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        mut creation: TableCreation,
    ) -> Result<Table> {
        let table_ident = TableIdent::new(namespace.clone(), creation.name.clone());

        // create table
        let create_resp: CreateTableOutput = self
            .s3tables_client
            .create_table()
            .table_bucket_arn(self.config.table_bucket_arn.clone())
            .namespace(namespace.to_url_string())
            .format(OpenTableFormat::Iceberg)
            .name(table_ident.name())
            .send()
            .await
            .map_err(from_aws_sdk_error)?;

        // prepare metadata location. the warehouse location is generated by s3tables catalog,
        // which looks like: s3://e6c9bf20-991a-46fb-kni5xs1q2yxi3xxdyxzjzigdeop1quse2b--table-s3
        let metadata_location = match &creation.location {
            Some(_) => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "The location of the table is generated by s3tables catalog, can't be set by user.",
                ));
            }
            None => {
                let get_resp: GetTableOutput = self
                    .s3tables_client
                    .get_table()
                    .table_bucket_arn(self.config.table_bucket_arn.clone())
                    .namespace(namespace.to_url_string())
                    .name(table_ident.name())
                    .send()
                    .await
                    .map_err(from_aws_sdk_error)?;
                let warehouse_location = get_resp.warehouse_location().to_string();
                MetadataLocation::new_with_table_location(warehouse_location).to_string()
            }
        };

        // write metadata to file
        creation.location = Some(metadata_location.clone());
        let metadata = TableMetadataBuilder::from_table_creation(creation)?
            .build()?
            .metadata;
        metadata.write_to(&self.file_io, &metadata_location).await?;

        // update metadata location
        self.s3tables_client
            .update_table_metadata_location()
            .table_bucket_arn(self.config.table_bucket_arn.clone())
            .namespace(namespace.to_url_string())
            .name(table_ident.name())
            .metadata_location(metadata_location.clone())
            .version_token(create_resp.version_token())
            .send()
            .await
            .map_err(from_aws_sdk_error)?;

        let table = Table::builder()
            .identifier(table_ident)
            .metadata_location(metadata_location)
            .metadata(metadata)
            .file_io(self.file_io.clone())
            .build()?;
        Ok(table)
    }

    /// Loads an existing table from the s3tables catalog.
    ///
    /// Retrieves the metadata location of the specified table and constructs a
    /// `Table` object with the retrieved metadata.
    ///
    /// This function can return an error in the following situations:
    /// - If the table does not have a metadata location, identified by a specific
    /// `Unexpected` variant.
    /// - Errors from the underlying database query process, converted using
    /// `from_aws_sdk_error`.
    async fn load_table(&self, table_ident: &TableIdent) -> Result<Table> {
        Ok(self.load_table_with_version_token(table_ident).await?.0)
    }

    /// Drops an existing table from the s3tables catalog.
    ///
    /// Validates the table identifier and then deletes the corresponding
    /// table from the s3tables catalog.
    ///
    /// This function can return an error in the following situations:
    /// - Errors from the underlying database deletion process, converted using
    /// `from_aws_sdk_error`.
    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        let req = self
            .s3tables_client
            .delete_table()
            .table_bucket_arn(self.config.table_bucket_arn.clone())
            .namespace(table.namespace().to_url_string())
            .name(table.name());
        req.send().await.map_err(from_aws_sdk_error)?;
        Ok(())
    }

    /// Checks if a table exists within the s3tables catalog.
    ///
    /// Validates the table identifier by querying the s3tables catalog
    /// to determine if the specified table exists.
    ///
    /// # Returns
    /// A `Result<bool>` indicating the outcome of the check:
    /// - `Ok(true)` if the table exists.
    /// - `Ok(false)` if the table does not exist, identified by a specific
    /// `IsNotFoundException` variant.
    /// - `Err(...)` if an error occurs during validation or the s3tables catalog
    /// query, with the error encapsulating the issue.
    async fn table_exists(&self, table_ident: &TableIdent) -> Result<bool> {
        let req = self
            .s3tables_client
            .get_table()
            .table_bucket_arn(self.config.table_bucket_arn.clone())
            .namespace(table_ident.namespace().to_url_string())
            .name(table_ident.name());
        match req.send().await {
            Ok(_) => Ok(true),
            Err(err) => {
                if err.as_service_error().map(|e| e.is_not_found_exception()) == Some(true) {
                    Ok(false)
                } else {
                    Err(from_aws_sdk_error(err))
                }
            }
        }
    }

    /// Renames an existing table within the s3tables catalog.
    ///
    /// Validates the source and destination table identifiers and then renames
    /// the source table to the destination table.
    ///
    /// This function can return an error in the following situations:
    /// - Errors from the underlying database renaming process, converted using
    /// `from_aws_sdk_error`.
    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        let req = self
            .s3tables_client
            .rename_table()
            .table_bucket_arn(self.config.table_bucket_arn.clone())
            .namespace(src.namespace().to_url_string())
            .name(src.name())
            .new_namespace_name(dest.namespace().to_url_string())
            .new_name(dest.name());
        req.send().await.map_err(from_aws_sdk_error)?;
        Ok(())
    }

    async fn register_table(
        &self,
        _table_ident: &TableIdent,
        _metadata_location: String,
    ) -> Result<Table> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Registering a table is not supported yet",
        ))
    }

    /// Updates an existing table within the s3tables catalog.
    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        let table_ident = commit.identifier().clone();
        let table_namespace = table_ident.namespace();
        let (current_table, version_token) =
            self.load_table_with_version_token(&table_ident).await?;

        let staged_table = commit.apply(current_table)?;
        let staged_metadata_location = staged_table.metadata_location_result()?;

        staged_table
            .metadata()
            .write_to(staged_table.file_io(), staged_metadata_location)
            .await?;

        let builder = self
            .s3tables_client
            .update_table_metadata_location()
            .table_bucket_arn(&self.config.table_bucket_arn)
            .namespace(table_namespace.to_url_string())
            .name(table_ident.name())
            .version_token(version_token)
            .metadata_location(staged_metadata_location);

        let _ = builder.send().await.map_err(|e| {
            let error = e.into_service_error();
            match error {
                UpdateTableMetadataLocationError::ConflictException(_) => Error::new(
                    ErrorKind::CatalogCommitConflicts,
                    format!("Commit conflicted for table: {table_ident}"),
                )
                .with_retryable(true),
                UpdateTableMetadataLocationError::NotFoundException(_) => Error::new(
                    ErrorKind::TableNotFound,
                    format!("Table {table_ident} is not found"),
                ),
                _ => Error::new(
                    ErrorKind::Unexpected,
                    "Operation failed for hitting aws sdk error",
                ),
            }
            .with_source(anyhow::Error::msg(format!("aws sdk error: {error:?}")))
        })?;

        Ok(staged_table)
    }
}

/// Format AWS SDK error into iceberg error
pub(crate) fn from_aws_sdk_error<T>(error: aws_sdk_s3tables::error::SdkError<T>) -> Error
where T: std::fmt::Debug {
    Error::new(
        ErrorKind::Unexpected,
        format!("Operation failed for hitting aws sdk error: {error:?}"),
    )
}

#[cfg(test)]
mod tests {
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use iceberg::transaction::{ApplyTransactionAction, Transaction};

    use super::*;

    async fn load_s3tables_catalog_from_env() -> Result<Option<S3TablesCatalog>> {
        let table_bucket_arn = match std::env::var("TABLE_BUCKET_ARN").ok() {
            Some(table_bucket_arn) => table_bucket_arn,
            None => return Ok(None),
        };

        let config = S3TablesCatalogConfig {
            name: None,
            table_bucket_arn,
            endpoint_url: None,
            client: None,
            props: HashMap::new(),
        };

        Ok(Some(S3TablesCatalog::new(config).await?))
    }

    #[tokio::test]
    async fn test_s3tables_list_namespace() {
        let catalog = match load_s3tables_catalog_from_env().await {
            Ok(Some(catalog)) => catalog,
            Ok(None) => return,
            Err(e) => panic!("Error loading catalog: {e}"),
        };

        let namespaces = catalog.list_namespaces(None).await.unwrap();
        assert!(!namespaces.is_empty());
    }

    #[tokio::test]
    async fn test_s3tables_list_tables() {
        let catalog = match load_s3tables_catalog_from_env().await {
            Ok(Some(catalog)) => catalog,
            Ok(None) => return,
            Err(e) => panic!("Error loading catalog: {e}"),
        };

        let tables = catalog
            .list_tables(&NamespaceIdent::new("aws_s3_metadata".to_string()))
            .await
            .unwrap();
        assert!(!tables.is_empty());
    }

    #[tokio::test]
    async fn test_s3tables_load_table() {
        let catalog = match load_s3tables_catalog_from_env().await {
            Ok(Some(catalog)) => catalog,
            Ok(None) => return,
            Err(e) => panic!("Error loading catalog: {e}"),
        };

        let table = catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new("aws_s3_metadata".to_string()),
                "query_storage_metadata".to_string(),
            ))
            .await
            .unwrap();
        println!("{table:?}");
    }

    #[tokio::test]
    async fn test_s3tables_create_delete_namespace() {
        let catalog = match load_s3tables_catalog_from_env().await {
            Ok(Some(catalog)) => catalog,
            Ok(None) => return,
            Err(e) => panic!("Error loading catalog: {e}"),
        };

        let namespace = NamespaceIdent::new("test_s3tables_create_delete_namespace".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        assert!(catalog.namespace_exists(&namespace).await.unwrap());
        catalog.drop_namespace(&namespace).await.unwrap();
        assert!(!catalog.namespace_exists(&namespace).await.unwrap());
    }

    #[tokio::test]
    async fn test_s3tables_create_delete_table() {
        let catalog = match load_s3tables_catalog_from_env().await {
            Ok(Some(catalog)) => catalog,
            Ok(None) => return,
            Err(e) => panic!("Error loading catalog: {e}"),
        };

        let creation = {
            let schema = Schema::builder()
                .with_schema_id(0)
                .with_fields(vec![
                    NestedField::required(1, "foo", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "bar", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap();
            TableCreation::builder()
                .name("test_s3tables_create_delete_table".to_string())
                .properties(HashMap::new())
                .schema(schema)
                .build()
        };

        let namespace = NamespaceIdent::new("test_s3tables_create_delete_table".to_string());
        let table_ident = TableIdent::new(
            namespace.clone(),
            "test_s3tables_create_delete_table".to_string(),
        );
        catalog.drop_namespace(&namespace).await.ok();
        catalog.drop_table(&table_ident).await.ok();

        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        catalog.create_table(&namespace, creation).await.unwrap();
        assert!(catalog.table_exists(&table_ident).await.unwrap());
        catalog.drop_table(&table_ident).await.unwrap();
        assert!(!catalog.table_exists(&table_ident).await.unwrap());
        catalog.drop_namespace(&namespace).await.unwrap();
    }

    #[tokio::test]
    async fn test_s3tables_update_table() {
        let catalog = match load_s3tables_catalog_from_env().await {
            Ok(Some(catalog)) => catalog,
            Ok(None) => return,
            Err(e) => panic!("Error loading catalog: {e}"),
        };

        // Create a test namespace and table
        let namespace = NamespaceIdent::new("test_s3tables_update_table".to_string());
        let table_ident =
            TableIdent::new(namespace.clone(), "test_s3tables_update_table".to_string());

        // Clean up any existing resources from previous test runs
        catalog.drop_table(&table_ident).await.ok();
        catalog.drop_namespace(&namespace).await.ok();

        // Create namespace and table
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();

        let creation = {
            let schema = Schema::builder()
                .with_schema_id(0)
                .with_fields(vec![
                    NestedField::required(1, "foo", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "bar", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap();
            TableCreation::builder()
                .name(table_ident.name().to_string())
                .properties(HashMap::new())
                .schema(schema)
                .build()
        };

        let table = catalog.create_table(&namespace, creation).await.unwrap();

        // Create a transaction to update the table
        let tx = Transaction::new(&table);

        // Store the original metadata location for comparison
        let original_metadata_location = table.metadata_location();

        // Update table properties using the transaction
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
            updated_table.metadata_location(),
            original_metadata_location,
            "Metadata location should be updated after commit"
        );

        // Load the table again from the catalog to verify changes were persisted
        let reloaded_table = catalog.load_table(&table_ident).await.unwrap();

        // Verify the reloaded table matches the updated table
        assert_eq!(
            reloaded_table.metadata().properties().get("test_property"),
            Some(&"test_value".to_string())
        );
        assert_eq!(
            reloaded_table.metadata_location(),
            updated_table.metadata_location(),
            "Reloaded table should have the same metadata location as the updated table"
        );
    }

    #[tokio::test]
    async fn test_builder_load_missing_bucket_arn() {
        let builder = S3TablesCatalogBuilder::default();
        let result = builder.load("s3tables", HashMap::new()).await;

        assert!(result.is_err());
        if let Err(err) = result {
            assert_eq!(err.kind(), ErrorKind::DataInvalid);
            assert_eq!(err.message(), "Table bucket ARN is required");
        }
    }

    #[tokio::test]
    async fn test_builder_with_endpoint_url_ok() {
        let builder = S3TablesCatalogBuilder::default().with_endpoint_url("http://localhost:4566");

        let result = builder
            .load(
                "s3tables",
                HashMap::from([
                    (
                        S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN.to_string(),
                        "arn:aws:s3tables:us-east-1:123456789012:bucket/test".to_string(),
                    ),
                    ("some_prop".to_string(), "some_value".to_string()),
                ]),
            )
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_builder_with_client_ok() {
        use aws_config::BehaviorVersion;

        let sdk_config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let client = aws_sdk_s3tables::Client::new(&sdk_config);

        let builder = S3TablesCatalogBuilder::default().with_client(client);
        let result = builder
            .load(
                "s3tables",
                HashMap::from([(
                    S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN.to_string(),
                    "arn:aws:s3tables:us-east-1:123456789012:bucket/test".to_string(),
                )]),
            )
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_builder_with_table_bucket_arn() {
        let test_arn = "arn:aws:s3tables:us-west-2:123456789012:bucket/test-bucket";
        let builder = S3TablesCatalogBuilder::default().with_table_bucket_arn(test_arn);

        let result = builder.load("s3tables", HashMap::new()).await;

        assert!(result.is_ok());
        let catalog = result.unwrap();
        assert_eq!(catalog.config.table_bucket_arn, test_arn);
    }

    #[tokio::test]
    async fn test_builder_empty_table_bucket_arn_edge_cases() {
        let mut props = HashMap::new();
        props.insert(
            S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN.to_string(),
            "".to_string(),
        );

        let builder = S3TablesCatalogBuilder::default();
        let result = builder.load("s3tables", props).await;

        assert!(result.is_err());
        if let Err(err) = result {
            assert_eq!(err.kind(), ErrorKind::DataInvalid);
            assert_eq!(err.message(), "Table bucket ARN is required");
        }
    }

    #[tokio::test]
    async fn test_endpoint_url_property_overrides_builder_method() {
        let test_arn = "arn:aws:s3tables:us-west-2:123456789012:bucket/test-bucket";
        let builder_endpoint = "http://localhost:4566";
        let property_endpoint = "http://localhost:8080";

        let builder = S3TablesCatalogBuilder::default()
            .with_table_bucket_arn(test_arn)
            .with_endpoint_url(builder_endpoint);

        let mut props = HashMap::new();
        props.insert(
            S3TABLES_CATALOG_PROP_ENDPOINT_URL.to_string(),
            property_endpoint.to_string(),
        );

        let result = builder.load("s3tables", props).await;

        assert!(result.is_ok());
        let catalog = result.unwrap();

        // Property value should override builder method value
        assert_eq!(
            catalog.config.endpoint_url,
            Some(property_endpoint.to_string())
        );
        assert_ne!(
            catalog.config.endpoint_url,
            Some(builder_endpoint.to_string())
        );
    }

    #[tokio::test]
    async fn test_endpoint_url_builder_method_only() {
        let test_arn = "arn:aws:s3tables:us-west-2:123456789012:bucket/test-bucket";
        let builder_endpoint = "http://localhost:4566";

        let builder = S3TablesCatalogBuilder::default()
            .with_table_bucket_arn(test_arn)
            .with_endpoint_url(builder_endpoint);

        let result = builder.load("s3tables", HashMap::new()).await;

        assert!(result.is_ok());
        let catalog = result.unwrap();

        assert_eq!(
            catalog.config.endpoint_url,
            Some(builder_endpoint.to_string())
        );
    }

    #[tokio::test]
    async fn test_endpoint_url_property_only() {
        let test_arn = "arn:aws:s3tables:us-west-2:123456789012:bucket/test-bucket";
        let property_endpoint = "http://localhost:8080";

        let builder = S3TablesCatalogBuilder::default().with_table_bucket_arn(test_arn);

        let mut props = HashMap::new();
        props.insert(
            S3TABLES_CATALOG_PROP_ENDPOINT_URL.to_string(),
            property_endpoint.to_string(),
        );

        let result = builder.load("s3tables", props).await;

        assert!(result.is_ok());
        let catalog = result.unwrap();

        assert_eq!(
            catalog.config.endpoint_url,
            Some(property_endpoint.to_string())
        );
    }

    #[tokio::test]
    async fn test_table_bucket_arn_property_overrides_builder_method() {
        let builder_arn = "arn:aws:s3tables:us-west-2:123456789012:bucket/builder-bucket";
        let property_arn = "arn:aws:s3tables:us-east-1:987654321098:bucket/property-bucket";

        let builder = S3TablesCatalogBuilder::default().with_table_bucket_arn(builder_arn);

        let mut props = HashMap::new();
        props.insert(
            S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN.to_string(),
            property_arn.to_string(),
        );

        let result = builder.load("s3tables", props).await;

        assert!(result.is_ok());
        let catalog = result.unwrap();

        assert_eq!(catalog.config.table_bucket_arn, property_arn);
        assert_ne!(catalog.config.table_bucket_arn, builder_arn);
    }

    #[tokio::test]
    async fn test_table_bucket_arn_builder_method_only() {
        let builder_arn = "arn:aws:s3tables:us-west-2:123456789012:bucket/builder-bucket";

        let builder = S3TablesCatalogBuilder::default().with_table_bucket_arn(builder_arn);

        let result = builder.load("s3tables", HashMap::new()).await;

        assert!(result.is_ok());
        let catalog = result.unwrap();

        assert_eq!(catalog.config.table_bucket_arn, builder_arn);
    }

    #[tokio::test]
    async fn test_table_bucket_arn_property_only() {
        let property_arn = "arn:aws:s3tables:us-east-1:987654321098:bucket/property-bucket";

        let builder = S3TablesCatalogBuilder::default();

        let mut props = HashMap::new();
        props.insert(
            S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN.to_string(),
            property_arn.to_string(),
        );

        let result = builder.load("s3tables", props).await;

        assert!(result.is_ok());
        let catalog = result.unwrap();

        assert_eq!(catalog.config.table_bucket_arn, property_arn);
    }

    #[tokio::test]
    async fn test_builder_empty_name_validation() {
        let test_arn = "arn:aws:s3tables:us-west-2:123456789012:bucket/test-bucket";
        let builder = S3TablesCatalogBuilder::default().with_table_bucket_arn(test_arn);

        let result = builder.load("", HashMap::new()).await;

        assert!(result.is_err());
        if let Err(err) = result {
            assert_eq!(err.kind(), ErrorKind::DataInvalid);
            assert_eq!(err.message(), "Catalog name cannot be empty");
        }
    }

    #[tokio::test]
    async fn test_builder_whitespace_only_name_validation() {
        let test_arn = "arn:aws:s3tables:us-west-2:123456789012:bucket/test-bucket";
        let builder = S3TablesCatalogBuilder::default().with_table_bucket_arn(test_arn);

        let result = builder.load("   \t\n  ", HashMap::new()).await;

        assert!(result.is_err());
        if let Err(err) = result {
            assert_eq!(err.kind(), ErrorKind::DataInvalid);
            assert_eq!(err.message(), "Catalog name cannot be empty");
        }
    }

    #[tokio::test]
    async fn test_builder_name_validation_with_missing_arn() {
        let builder = S3TablesCatalogBuilder::default();

        let result = builder.load("", HashMap::new()).await;

        assert!(result.is_err());
        if let Err(err) = result {
            assert_eq!(err.kind(), ErrorKind::DataInvalid);
            assert_eq!(err.message(), "Catalog name cannot be empty");
        }
    }
}
