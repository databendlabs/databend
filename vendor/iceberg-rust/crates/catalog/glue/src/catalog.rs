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
use std::fmt::Debug;

use anyhow::anyhow;
use async_trait::async_trait;
use aws_sdk_glue::operation::create_table::CreateTableError;
use aws_sdk_glue::operation::update_table::UpdateTableError;
use aws_sdk_glue::types::TableInput;
use iceberg::io::{
    FileIO, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN,
};
use iceberg::spec::{TableMetadata, TableMetadataBuilder};
use iceberg::table::Table;
use iceberg::{
    Catalog, CatalogBuilder, Error, ErrorKind, MetadataLocation, Namespace, NamespaceIdent, Result,
    TableCommit, TableCreation, TableIdent,
};

use crate::error::{from_aws_build_error, from_aws_sdk_error};
use crate::utils::{
    convert_to_database, convert_to_glue_table, convert_to_namespace, create_sdk_config,
    get_default_table_location, get_metadata_location, validate_namespace,
};
use crate::{
    AWS_ACCESS_KEY_ID, AWS_REGION_NAME, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, with_catalog_id,
};

/// Glue catalog URI
pub const GLUE_CATALOG_PROP_URI: &str = "uri";
/// Glue catalog id
pub const GLUE_CATALOG_PROP_CATALOG_ID: &str = "catalog_id";
/// Glue catalog warehouse location
pub const GLUE_CATALOG_PROP_WAREHOUSE: &str = "warehouse";

/// Builder for [`GlueCatalog`].
#[derive(Debug)]
pub struct GlueCatalogBuilder(GlueCatalogConfig);

impl Default for GlueCatalogBuilder {
    fn default() -> Self {
        Self(GlueCatalogConfig {
            name: None,
            uri: None,
            catalog_id: None,
            warehouse: "".to_string(),
            props: HashMap::new(),
        })
    }
}

impl CatalogBuilder for GlueCatalogBuilder {
    type C = GlueCatalog;

    fn load(
        mut self,
        name: impl Into<String>,
        props: HashMap<String, String>,
    ) -> impl Future<Output = Result<Self::C>> + Send {
        self.0.name = Some(name.into());

        if props.contains_key(GLUE_CATALOG_PROP_URI) {
            self.0.uri = props.get(GLUE_CATALOG_PROP_URI).cloned()
        }

        if props.contains_key(GLUE_CATALOG_PROP_CATALOG_ID) {
            self.0.catalog_id = props.get(GLUE_CATALOG_PROP_CATALOG_ID).cloned()
        }

        if props.contains_key(GLUE_CATALOG_PROP_WAREHOUSE) {
            self.0.warehouse = props
                .get(GLUE_CATALOG_PROP_WAREHOUSE)
                .cloned()
                .unwrap_or_default();
        }

        // Collect other remaining properties
        self.0.props = props
            .into_iter()
            .filter(|(k, _)| {
                k != GLUE_CATALOG_PROP_URI
                    && k != GLUE_CATALOG_PROP_CATALOG_ID
                    && k != GLUE_CATALOG_PROP_WAREHOUSE
            })
            .collect();

        async move {
            if self.0.name.is_none() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Catalog name is required",
                ));
            }
            if self.0.warehouse.is_empty() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Catalog warehouse is required",
                ));
            }

            GlueCatalog::new(self.0).await
        }
    }
}

#[derive(Debug)]
/// Glue Catalog configuration
pub(crate) struct GlueCatalogConfig {
    name: Option<String>,
    uri: Option<String>,
    catalog_id: Option<String>,
    warehouse: String,
    props: HashMap<String, String>,
}

struct GlueClient(aws_sdk_glue::Client);

/// Glue Catalog
pub struct GlueCatalog {
    config: GlueCatalogConfig,
    client: GlueClient,
    file_io: FileIO,
}

impl Debug for GlueCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlueCatalog")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl GlueCatalog {
    /// Create a new glue catalog
    async fn new(config: GlueCatalogConfig) -> Result<Self> {
        let sdk_config = create_sdk_config(&config.props, config.uri.as_ref()).await;
        let mut file_io_props = config.props.clone();
        if !file_io_props.contains_key(S3_ACCESS_KEY_ID)
            && let Some(access_key_id) = file_io_props.get(AWS_ACCESS_KEY_ID)
        {
            file_io_props.insert(S3_ACCESS_KEY_ID.to_string(), access_key_id.to_string());
        }
        if !file_io_props.contains_key(S3_SECRET_ACCESS_KEY)
            && let Some(secret_access_key) = file_io_props.get(AWS_SECRET_ACCESS_KEY)
        {
            file_io_props.insert(
                S3_SECRET_ACCESS_KEY.to_string(),
                secret_access_key.to_string(),
            );
        }
        if !file_io_props.contains_key(S3_REGION)
            && let Some(region) = file_io_props.get(AWS_REGION_NAME)
        {
            file_io_props.insert(S3_REGION.to_string(), region.to_string());
        }
        if !file_io_props.contains_key(S3_SESSION_TOKEN)
            && let Some(session_token) = file_io_props.get(AWS_SESSION_TOKEN)
        {
            file_io_props.insert(S3_SESSION_TOKEN.to_string(), session_token.to_string());
        }
        if !file_io_props.contains_key(S3_ENDPOINT)
            && let Some(aws_endpoint) = config.uri.as_ref()
        {
            file_io_props.insert(S3_ENDPOINT.to_string(), aws_endpoint.to_string());
        }

        let client = aws_sdk_glue::Client::new(&sdk_config);

        let file_io = FileIO::from_path(&config.warehouse)?
            .with_props(file_io_props)
            .build()?;

        Ok(GlueCatalog {
            config,
            client: GlueClient(client),
            file_io,
        })
    }
    /// Get the catalogs `FileIO`
    pub fn file_io(&self) -> FileIO {
        self.file_io.clone()
    }

    /// Loads a table from the Glue Catalog along with its version_id for optimistic locking.
    ///
    /// # Returns
    /// A `Result` wrapping a tuple of (`Table`, `Option<String>`) where the String is the version_id
    /// from Glue that should be used for optimistic concurrency control when updating the table.
    ///
    /// # Errors
    /// This function may return an error in several scenarios, including:
    /// - Failure to validate the namespace.
    /// - Failure to retrieve the table from the Glue Catalog.
    /// - Absence of metadata location information in the table's properties.
    /// - Issues reading or deserializing the table's metadata file.
    async fn load_table_with_version_id(
        &self,
        table: &TableIdent,
    ) -> Result<(Table, Option<String>)> {
        let db_name = validate_namespace(table.namespace())?;
        let table_name = table.name();

        let builder = self
            .client
            .0
            .get_table()
            .database_name(&db_name)
            .name(table_name);
        let builder = with_catalog_id!(builder, self.config);

        let glue_table_output = builder.send().await.map_err(from_aws_sdk_error)?;

        let glue_table = glue_table_output.table().ok_or_else(|| {
            Error::new(
                ErrorKind::TableNotFound,
                format!(
                    "Table object for database: {db_name} and table: {table_name} does not exist"
                ),
            )
        })?;

        let version_id = glue_table.version_id.clone();
        let metadata_location = get_metadata_location(&glue_table.parameters)?;

        let metadata = TableMetadata::read_from(&self.file_io, &metadata_location).await?;

        let table = Table::builder()
            .file_io(self.file_io())
            .metadata_location(metadata_location)
            .metadata(metadata)
            .identifier(TableIdent::new(
                NamespaceIdent::new(db_name),
                table_name.to_owned(),
            ))
            .build()?;

        Ok((table, version_id))
    }
}

#[async_trait]
impl Catalog for GlueCatalog {
    /// List namespaces from glue catalog.
    ///
    /// Glue doesn't support nested namespaces.
    /// We will return an empty list if parent is some.
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        if parent.is_some() {
            return Ok(vec![]);
        }

        let mut database_list: Vec<NamespaceIdent> = Vec::new();
        let mut next_token: Option<String> = None;

        loop {
            let builder = match &next_token {
                Some(token) => self.client.0.get_databases().next_token(token),
                None => self.client.0.get_databases(),
            };
            let builder = with_catalog_id!(builder, self.config);
            let resp = builder.send().await.map_err(from_aws_sdk_error)?;

            let dbs: Vec<NamespaceIdent> = resp
                .database_list()
                .iter()
                .map(|db| NamespaceIdent::new(db.name().to_string()))
                .collect();

            database_list.extend(dbs);

            next_token = resp.next_token().map(ToOwned::to_owned);
            if next_token.is_none() {
                break;
            }
        }

        Ok(database_list)
    }

    /// Creates a new namespace with the given identifier and properties.
    ///
    /// Attempts to create a namespace defined by the `namespace`
    /// parameter and configured with the specified `properties`.
    ///
    /// This function can return an error in the following situations:
    ///
    /// - Errors from `validate_namespace` if the namespace identifier does not
    /// meet validation criteria.
    /// - Errors from `convert_to_database` if the properties cannot be
    /// successfully converted into a database configuration.
    /// - Errors from the underlying database creation process, converted using
    /// `from_sdk_error`.
    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        let db_input = convert_to_database(namespace, &properties)?;

        let builder = self.client.0.create_database().database_input(db_input);
        let builder = with_catalog_id!(builder, self.config);

        builder.send().await.map_err(from_aws_sdk_error)?;

        Ok(Namespace::with_properties(namespace.clone(), properties))
    }

    /// Retrieves a namespace by its identifier.
    ///
    /// Validates the given namespace identifier and then queries the
    /// underlying database client to fetch the corresponding namespace data.
    /// Constructs a `Namespace` object with the retrieved data and returns it.
    ///
    /// This function can return an error in any of the following situations:
    /// - If the provided namespace identifier fails validation checks
    /// - If there is an error querying the database, returned by
    /// `from_sdk_error`.
    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        let db_name = validate_namespace(namespace)?;

        let builder = self.client.0.get_database().name(&db_name);
        let builder = with_catalog_id!(builder, self.config);

        let resp = builder.send().await.map_err(from_aws_sdk_error)?;

        match resp.database() {
            Some(db) => {
                let namespace = convert_to_namespace(db);
                Ok(namespace)
            }
            None => Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Database with name: {db_name} does not exist"),
            )),
        }
    }

    /// Checks if a namespace exists within the Glue Catalog.
    ///
    /// Validates the namespace identifier by querying the Glue Catalog
    /// to determine if the specified namespace (database) exists.
    ///
    /// # Returns
    /// A `Result<bool>` indicating the outcome of the check:
    /// - `Ok(true)` if the namespace exists.
    /// - `Ok(false)` if the namespace does not exist, identified by a specific
    /// `EntityNotFoundException` variant.
    /// - `Err(...)` if an error occurs during validation or the Glue Catalog
    /// query, with the error encapsulating the issue.
    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        let db_name = validate_namespace(namespace)?;

        let builder = self.client.0.get_database().name(&db_name);
        let builder = with_catalog_id!(builder, self.config);

        let resp = builder.send().await;

        match resp {
            Ok(_) => Ok(true),
            Err(err) => {
                if err
                    .as_service_error()
                    .map(|e| e.is_entity_not_found_exception())
                    == Some(true)
                {
                    return Ok(false);
                }
                Err(from_aws_sdk_error(err))
            }
        }
    }

    /// Asynchronously updates properties of an existing namespace.
    ///
    /// Converts the given namespace identifier and properties into a database
    /// representation and then attempts to update the corresponding namespace
    /// in the Glue Catalog.
    ///
    /// # Returns
    /// Returns `Ok(())` if the namespace update is successful. If the
    /// namespace cannot be updated due to missing information or an error
    /// during the update process, an `Err(...)` is returned.
    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        let db_name = validate_namespace(namespace)?;
        let db_input = convert_to_database(namespace, &properties)?;

        let builder = self
            .client
            .0
            .update_database()
            .name(&db_name)
            .database_input(db_input);
        let builder = with_catalog_id!(builder, self.config);

        builder.send().await.map_err(from_aws_sdk_error)?;

        Ok(())
    }

    /// Asynchronously drops a namespace from the Glue Catalog.
    ///
    /// Checks if the namespace is empty. If it still contains tables the
    /// namespace will not be dropped, but an error is returned instead.
    ///
    /// # Returns
    /// A `Result<()>` indicating the outcome:
    /// - `Ok(())` signifies successful namespace deletion.
    /// - `Err(...)` signifies failure to drop the namespace due to validation
    /// errors, connectivity issues, or Glue Catalog constraints.
    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        let db_name = validate_namespace(namespace)?;
        let table_list = self.list_tables(namespace).await?;

        if !table_list.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Database with name: {} is not empty", &db_name),
            ));
        }

        let builder = self.client.0.delete_database().name(db_name);
        let builder = with_catalog_id!(builder, self.config);

        builder.send().await.map_err(from_aws_sdk_error)?;

        Ok(())
    }

    /// Asynchronously lists all tables within a specified namespace.
    ///
    /// # Returns
    /// A `Result<Vec<TableIdent>>`, which is:
    /// - `Ok(vec![...])` containing a vector of `TableIdent` instances, each
    /// representing a table within the specified namespace.
    /// - `Err(...)` if an error occurs during namespace validation or while
    /// querying the database.
    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        let db_name = validate_namespace(namespace)?;

        let mut table_list: Vec<TableIdent> = Vec::new();
        let mut next_token: Option<String> = None;

        loop {
            let builder = match &next_token {
                Some(token) => self
                    .client
                    .0
                    .get_tables()
                    .database_name(&db_name)
                    .next_token(token),
                None => self.client.0.get_tables().database_name(&db_name),
            };
            let builder = with_catalog_id!(builder, self.config);
            let resp = builder.send().await.map_err(from_aws_sdk_error)?;

            let tables: Vec<_> = resp
                .table_list()
                .iter()
                .map(|tbl| TableIdent::new(namespace.clone(), tbl.name().to_string()))
                .collect();

            table_list.extend(tables);

            next_token = resp.next_token().map(ToOwned::to_owned);
            if next_token.is_none() {
                break;
            }
        }

        Ok(table_list)
    }

    /// Creates a new table within a specified namespace using the provided
    /// table creation settings.
    ///
    /// # Returns
    /// A `Result` wrapping a `Table` object representing the newly created
    /// table.
    ///
    /// # Errors
    /// This function may return an error in several cases, including invalid
    /// namespace identifiers, failure to determine a default storage location,
    /// issues generating or writing table metadata, and errors communicating
    /// with the Glue Catalog.
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        mut creation: TableCreation,
    ) -> Result<Table> {
        let db_name = validate_namespace(namespace)?;
        let table_name = creation.name.clone();

        let location = match &creation.location {
            Some(location) => location.clone(),
            None => {
                let ns = self.get_namespace(namespace).await?;
                let location =
                    get_default_table_location(&ns, &db_name, &table_name, &self.config.warehouse);
                creation.location = Some(location.clone());
                location
            }
        };
        let metadata = TableMetadataBuilder::from_table_creation(creation)?
            .build()?
            .metadata;
        let metadata_location =
            MetadataLocation::new_with_table_location(location.clone()).to_string();

        metadata.write_to(&self.file_io, &metadata_location).await?;

        let glue_table = convert_to_glue_table(
            &table_name,
            metadata_location.clone(),
            &metadata,
            metadata.properties(),
            None,
        )?;

        let builder = self
            .client
            .0
            .create_table()
            .database_name(&db_name)
            .table_input(glue_table);
        let builder = with_catalog_id!(builder, self.config);

        builder.send().await.map_err(from_aws_sdk_error)?;

        Table::builder()
            .file_io(self.file_io())
            .metadata_location(metadata_location)
            .metadata(metadata)
            .identifier(TableIdent::new(NamespaceIdent::new(db_name), table_name))
            .build()
    }

    /// Loads a table from the Glue Catalog and constructs a `Table` object
    /// based on its metadata.
    ///
    /// # Returns
    /// A `Result` wrapping a `Table` object that represents the loaded table.
    ///
    /// # Errors
    /// This function may return an error in several scenarios, including:
    /// - Failure to validate the namespace.
    /// - Failure to retrieve the table from the Glue Catalog.
    /// - Absence of metadata location information in the table's properties.
    /// - Issues reading or deserializing the table's metadata file.
    async fn load_table(&self, table: &TableIdent) -> Result<Table> {
        let (table, _) = self.load_table_with_version_id(table).await?;
        Ok(table)
    }

    /// Asynchronously drops a table from the database.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The namespace provided in `table` cannot be validated
    /// or does not exist.
    /// - The underlying database client encounters an error while
    /// attempting to drop the table. This includes scenarios where
    /// the table does not exist.
    /// - Any network or communication error occurs with the database backend.
    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        let db_name = validate_namespace(table.namespace())?;
        let table_name = table.name();

        let builder = self
            .client
            .0
            .delete_table()
            .database_name(&db_name)
            .name(table_name);
        let builder = with_catalog_id!(builder, self.config);

        builder.send().await.map_err(from_aws_sdk_error)?;

        Ok(())
    }

    /// Asynchronously checks the existence of a specified table
    /// in the database.
    ///
    /// # Returns
    /// - `Ok(true)` if the table exists in the database.
    /// - `Ok(false)` if the table does not exist in the database.
    /// - `Err(...)` if an error occurs during the process
    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        let db_name = validate_namespace(table.namespace())?;
        let table_name = table.name();

        let builder = self
            .client
            .0
            .get_table()
            .database_name(&db_name)
            .name(table_name);
        let builder = with_catalog_id!(builder, self.config);

        let resp = builder.send().await;

        match resp {
            Ok(_) => Ok(true),
            Err(err) => {
                if err
                    .as_service_error()
                    .map(|e| e.is_entity_not_found_exception())
                    == Some(true)
                {
                    return Ok(false);
                }
                Err(from_aws_sdk_error(err))
            }
        }
    }

    /// Asynchronously renames a table within the database
    /// or moves it between namespaces (databases).
    ///
    /// # Returns
    /// - `Ok(())` on successful rename or move of the table.
    /// - `Err(...)` if an error occurs during the process.
    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        let src_db_name = validate_namespace(src.namespace())?;
        let dest_db_name = validate_namespace(dest.namespace())?;

        let src_table_name = src.name();
        let dest_table_name = dest.name();

        let builder = self
            .client
            .0
            .get_table()
            .database_name(&src_db_name)
            .name(src_table_name);
        let builder = with_catalog_id!(builder, self.config);

        let glue_table_output = builder.send().await.map_err(from_aws_sdk_error)?;

        match glue_table_output.table() {
            None => Err(Error::new(
                ErrorKind::TableNotFound,
                format!(
                    "'Table' object for database: {src_db_name} and table: {src_table_name} does not exist"
                ),
            )),
            Some(table) => {
                let rename_table_input = TableInput::builder()
                    .name(dest_table_name)
                    .set_parameters(table.parameters.clone())
                    .set_storage_descriptor(table.storage_descriptor.clone())
                    .set_table_type(table.table_type.clone())
                    .set_description(table.description.clone())
                    .build()
                    .map_err(from_aws_build_error)?;

                let builder = self
                    .client
                    .0
                    .create_table()
                    .database_name(&dest_db_name)
                    .table_input(rename_table_input);
                let builder = with_catalog_id!(builder, self.config);

                builder.send().await.map_err(from_aws_sdk_error)?;

                let drop_src_table_result = self.drop_table(src).await;

                match drop_src_table_result {
                    Ok(_) => Ok(()),
                    Err(_) => {
                        let err_msg_src_table =
                            format!("Failed to drop old table {src_db_name}.{src_table_name}.");

                        let drop_dest_table_result = self.drop_table(dest).await;

                        match drop_dest_table_result {
                            Ok(_) => Err(Error::new(
                                ErrorKind::Unexpected,
                                format!(
                                    "{err_msg_src_table} Rolled back table creation for {dest_db_name}.{dest_table_name}."
                                ),
                            )),
                            Err(_) => Err(Error::new(
                                ErrorKind::Unexpected,
                                format!(
                                    "{err_msg_src_table} Failed to roll back table creation for {dest_db_name}.{dest_table_name}. Please clean up manually."
                                ),
                            )),
                        }
                    }
                }
            }
        }
    }

    /// registers an existing table into the Glue Catalog.
    ///
    /// Converts the provided table identifier and metadata location into a
    /// Glue-compatible table representation, and attempts to create the
    /// corresponding table in the Glue Catalog.
    ///
    /// # Returns
    /// Returns `Ok(Table)` if the table is successfully registered and loaded.
    /// If the registration fails due to validation issues, existing table conflicts,
    /// metadata problems, or errors during the registration or loading process,
    /// an `Err(...)` is returned.
    async fn register_table(
        &self,
        table_ident: &TableIdent,
        metadata_location: String,
    ) -> Result<Table> {
        let db_name = validate_namespace(table_ident.namespace())?;
        let table_name = table_ident.name();
        let metadata = TableMetadata::read_from(&self.file_io, &metadata_location).await?;

        let table_input = convert_to_glue_table(
            table_name,
            metadata_location.clone(),
            &metadata,
            metadata.properties(),
            None,
        )?;

        let builder = self
            .client
            .0
            .create_table()
            .database_name(&db_name)
            .table_input(table_input);
        let builder = with_catalog_id!(builder, self.config);

        builder.send().await.map_err(|e| {
            let error = e.into_service_error();
            match error {
                CreateTableError::EntityNotFoundException(_) => Error::new(
                    ErrorKind::NamespaceNotFound,
                    format!("Database {db_name} does not exist"),
                ),
                CreateTableError::AlreadyExistsException(_) => Error::new(
                    ErrorKind::TableAlreadyExists,
                    format!("Table {table_ident} already exists"),
                ),
                _ => Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to register table {table_ident} due to AWS SDK error"),
                ),
            }
            .with_source(anyhow!("aws sdk error: {error:?}"))
        })?;

        Ok(Table::builder()
            .identifier(table_ident.clone())
            .metadata_location(metadata_location)
            .metadata(metadata)
            .file_io(self.file_io())
            .build()?)
    }

    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        let table_ident = commit.identifier().clone();
        let table_namespace = validate_namespace(table_ident.namespace())?;

        let (current_table, current_version_id) =
            self.load_table_with_version_id(&table_ident).await?;
        let current_metadata_location = current_table.metadata_location_result()?.to_string();

        let staged_table = commit.apply(current_table)?;
        let staged_metadata_location = staged_table.metadata_location_result()?;

        // Write new metadata
        staged_table
            .metadata()
            .write_to(staged_table.file_io(), staged_metadata_location)
            .await?;

        // Persist staged table to Glue with optimistic locking
        let mut builder = self
            .client
            .0
            .update_table()
            .database_name(table_namespace)
            .set_skip_archive(Some(true)) // todo make this configurable
            .table_input(convert_to_glue_table(
                table_ident.name(),
                staged_metadata_location.to_string(),
                staged_table.metadata(),
                staged_table.metadata().properties(),
                Some(current_metadata_location),
            )?);

        // Add VersionId for optimistic locking
        if let Some(version_id) = current_version_id {
            builder = builder.version_id(version_id);
        }

        let builder = with_catalog_id!(builder, self.config);
        let _ = builder.send().await.map_err(|e| {
            let error = e.into_service_error();
            match error {
                UpdateTableError::EntityNotFoundException(_) => Error::new(
                    ErrorKind::TableNotFound,
                    format!("Table {table_ident} is not found"),
                ),
                UpdateTableError::ConcurrentModificationException(_) => Error::new(
                    ErrorKind::CatalogCommitConflicts,
                    format!("Commit failed for table: {table_ident}"),
                )
                .with_retryable(true),
                _ => Error::new(
                    ErrorKind::Unexpected,
                    format!("Operation failed for table: {table_ident} for hitting aws sdk error"),
                ),
            }
            .with_source(anyhow!("aws sdk error: {error:?}"))
        })?;

        Ok(staged_table)
    }
}
