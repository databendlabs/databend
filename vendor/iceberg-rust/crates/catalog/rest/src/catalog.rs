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

//! This module contains the iceberg REST catalog implementation.

use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::str::FromStr;

use async_trait::async_trait;
use iceberg::io::{self, FileIO};
use iceberg::table::Table;
use iceberg::{
    Catalog, CatalogBuilder, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit,
    TableCreation, TableIdent,
};
use itertools::Itertools;
use reqwest::header::{
    HeaderMap, HeaderName, HeaderValue, {self},
};
use reqwest::{Client, Method, StatusCode, Url};
use tokio::sync::OnceCell;
use typed_builder::TypedBuilder;

use crate::client::{
    HttpClient, deserialize_catalog_response, deserialize_unexpected_catalog_error,
};
use crate::types::{
    CatalogConfig, CommitTableRequest, CommitTableResponse, CreateNamespaceRequest,
    CreateTableRequest, ListNamespaceResponse, ListTablesResponse, LoadTableResult,
    NamespaceResponse, RegisterTableRequest, RenameTableRequest,
};

/// REST catalog URI
pub const REST_CATALOG_PROP_URI: &str = "uri";
/// REST catalog warehouse location
pub const REST_CATALOG_PROP_WAREHOUSE: &str = "warehouse";

const ICEBERG_REST_SPEC_VERSION: &str = "0.14.1";
const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
const PATH_V1: &str = "v1";

/// Builder for [`RestCatalog`].
#[derive(Debug)]
pub struct RestCatalogBuilder(RestCatalogConfig);

impl Default for RestCatalogBuilder {
    fn default() -> Self {
        Self(RestCatalogConfig {
            name: None,
            uri: "".to_string(),
            warehouse: None,
            props: HashMap::new(),
            client: None,
        })
    }
}

impl CatalogBuilder for RestCatalogBuilder {
    type C = RestCatalog;

    fn load(
        mut self,
        name: impl Into<String>,
        props: HashMap<String, String>,
    ) -> impl Future<Output = Result<Self::C>> + Send {
        self.0.name = Some(name.into());

        if props.contains_key(REST_CATALOG_PROP_URI) {
            self.0.uri = props
                .get(REST_CATALOG_PROP_URI)
                .cloned()
                .unwrap_or_default();
        }

        if props.contains_key(REST_CATALOG_PROP_WAREHOUSE) {
            self.0.warehouse = props.get(REST_CATALOG_PROP_WAREHOUSE).cloned()
        }

        // Collect other remaining properties
        self.0.props = props
            .into_iter()
            .filter(|(k, _)| k != REST_CATALOG_PROP_URI && k != REST_CATALOG_PROP_WAREHOUSE)
            .collect();

        let result = {
            if self.0.name.is_none() {
                Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Catalog name is required",
                ))
            } else if self.0.uri.is_empty() {
                Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Catalog uri is required",
                ))
            } else {
                Ok(RestCatalog::new(self.0))
            }
        };

        std::future::ready(result)
    }
}

impl RestCatalogBuilder {
    /// Configures the catalog with a custom HTTP client.
    pub fn with_client(mut self, client: Client) -> Self {
        self.0.client = Some(client);
        self
    }
}

/// Rest catalog configuration.
#[derive(Clone, Debug, TypedBuilder)]
pub(crate) struct RestCatalogConfig {
    #[builder(default, setter(strip_option))]
    name: Option<String>,

    uri: String,

    #[builder(default, setter(strip_option(fallback = warehouse_opt)))]
    warehouse: Option<String>,

    #[builder(default)]
    props: HashMap<String, String>,

    #[builder(default)]
    client: Option<Client>,
}

impl RestCatalogConfig {
    fn url_prefixed(&self, parts: &[&str]) -> String {
        [&self.uri, PATH_V1]
            .into_iter()
            .chain(self.props.get("prefix").map(|s| &**s))
            .chain(parts.iter().cloned())
            .join("/")
    }

    fn config_endpoint(&self) -> String {
        [&self.uri, PATH_V1, "config"].join("/")
    }

    pub(crate) fn get_token_endpoint(&self) -> String {
        if let Some(oauth2_uri) = self.props.get("oauth2-server-uri") {
            oauth2_uri.to_string()
        } else {
            [&self.uri, PATH_V1, "oauth", "tokens"].join("/")
        }
    }

    fn namespaces_endpoint(&self) -> String {
        self.url_prefixed(&["namespaces"])
    }

    fn namespace_endpoint(&self, ns: &NamespaceIdent) -> String {
        self.url_prefixed(&["namespaces", &ns.to_url_string()])
    }

    fn tables_endpoint(&self, ns: &NamespaceIdent) -> String {
        self.url_prefixed(&["namespaces", &ns.to_url_string(), "tables"])
    }

    fn rename_table_endpoint(&self) -> String {
        self.url_prefixed(&["tables", "rename"])
    }

    fn register_table_endpoint(&self, ns: &NamespaceIdent) -> String {
        self.url_prefixed(&["namespaces", &ns.to_url_string(), "register"])
    }

    fn table_endpoint(&self, table: &TableIdent) -> String {
        self.url_prefixed(&[
            "namespaces",
            &table.namespace.to_url_string(),
            "tables",
            &table.name,
        ])
    }

    /// Get the client from the config.
    pub(crate) fn client(&self) -> Option<Client> {
        self.client.clone()
    }

    /// Get the token from the config.
    ///
    /// The client can use this token to send requests.
    pub(crate) fn token(&self) -> Option<String> {
        self.props.get("token").cloned()
    }

    /// Get the credentials from the config. The client can use these credentials to fetch a new
    /// token.
    ///
    /// ## Output
    ///
    /// - `None`: No credential is set.
    /// - `Some(None, client_secret)`: No client_id is set, use client_secret directly.
    /// - `Some(Some(client_id), client_secret)`: Both client_id and client_secret are set.
    pub(crate) fn credential(&self) -> Option<(Option<String>, String)> {
        let cred = self.props.get("credential")?;

        match cred.split_once(':') {
            Some((client_id, client_secret)) => {
                Some((Some(client_id.to_string()), client_secret.to_string()))
            }
            None => Some((None, cred.to_string())),
        }
    }

    /// Get the extra headers from config, which includes:
    ///
    /// - `content-type`
    /// - `x-client-version`
    /// - `user-agent`
    /// - All headers specified by `header.xxx` in props.
    pub(crate) fn extra_headers(&self) -> Result<HeaderMap> {
        let mut headers = HeaderMap::from_iter([
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            ),
            (
                HeaderName::from_static("x-client-version"),
                HeaderValue::from_static(ICEBERG_REST_SPEC_VERSION),
            ),
            (
                header::USER_AGENT,
                HeaderValue::from_str(&format!("iceberg-rs/{CARGO_PKG_VERSION}")).unwrap(),
            ),
        ]);

        for (key, value) in self
            .props
            .iter()
            .filter_map(|(k, v)| k.strip_prefix("header.").map(|k| (k, v)))
        {
            headers.insert(
                HeaderName::from_str(key).map_err(|e| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid header name: {key}"),
                    )
                    .with_source(e)
                })?,
                HeaderValue::from_str(value).map_err(|e| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid header value: {value}"),
                    )
                    .with_source(e)
                })?,
            );
        }

        Ok(headers)
    }

    /// Get the optional OAuth headers from the config.
    pub(crate) fn extra_oauth_params(&self) -> HashMap<String, String> {
        let mut params = HashMap::new();

        if let Some(scope) = self.props.get("scope") {
            params.insert("scope".to_string(), scope.to_string());
        } else {
            params.insert("scope".to_string(), "catalog".to_string());
        }

        let optional_params = ["audience", "resource"];
        for param_name in optional_params {
            if let Some(value) = self.props.get(param_name) {
                params.insert(param_name.to_string(), value.to_string());
            }
        }

        params
    }

    /// Merge the `RestCatalogConfig` with the a [`CatalogConfig`] (fetched from the REST server).
    pub(crate) fn merge_with_config(mut self, mut config: CatalogConfig) -> Self {
        if let Some(uri) = config.overrides.remove("uri") {
            self.uri = uri;
        }

        let mut props = config.defaults;
        props.extend(self.props);
        props.extend(config.overrides);

        self.props = props;
        self
    }
}

#[derive(Debug)]
struct RestContext {
    client: HttpClient,
    /// Runtime config is fetched from rest server and stored here.
    ///
    /// It's could be different from the user config.
    config: RestCatalogConfig,
}

/// Rest catalog implementation.
#[derive(Debug)]
pub struct RestCatalog {
    /// User config is stored as-is and never be changed.
    ///
    /// It could be different from the config fetched from the server and used at runtime.
    user_config: RestCatalogConfig,
    ctx: OnceCell<RestContext>,
    /// Extensions for the FileIOBuilder.
    file_io_extensions: io::Extensions,
}

impl RestCatalog {
    /// Creates a `RestCatalog` from a [`RestCatalogConfig`].
    fn new(config: RestCatalogConfig) -> Self {
        Self {
            user_config: config,
            ctx: OnceCell::new(),
            file_io_extensions: io::Extensions::default(),
        }
    }

    /// Add an extension to the file IO builder.
    pub fn with_file_io_extension<T: Any + Send + Sync>(mut self, ext: T) -> Self {
        self.file_io_extensions.add(ext);
        self
    }

    /// Gets the [`RestContext`] from the catalog.
    async fn context(&self) -> Result<&RestContext> {
        self.ctx
            .get_or_try_init(|| async {
                let client = HttpClient::new(&self.user_config)?;
                let catalog_config = RestCatalog::load_config(&client, &self.user_config).await?;
                let config = self.user_config.clone().merge_with_config(catalog_config);
                let client = client.update_with(&config)?;

                Ok(RestContext { config, client })
            })
            .await
    }

    /// Load the runtime config from the server by `user_config`.
    ///
    /// It's required for a REST catalog to update its config after creation.
    async fn load_config(
        client: &HttpClient,
        user_config: &RestCatalogConfig,
    ) -> Result<CatalogConfig> {
        let mut request_builder = client.request(Method::GET, user_config.config_endpoint());

        if let Some(warehouse_location) = &user_config.warehouse {
            request_builder = request_builder.query(&[("warehouse", warehouse_location)]);
        }

        let request = request_builder.build()?;

        let http_response = client.query_catalog(request).await?;

        match http_response.status() {
            StatusCode::OK => deserialize_catalog_response(http_response).await,
            _ => Err(deserialize_unexpected_catalog_error(http_response).await),
        }
    }

    async fn load_file_io(
        &self,
        metadata_location: Option<&str>,
        extra_config: Option<HashMap<String, String>>,
    ) -> Result<FileIO> {
        let mut props = self.context().await?.config.props.clone();
        if let Some(config) = extra_config {
            props.extend(config);
        }

        // If the warehouse is a logical identifier instead of a URL we don't want
        // to raise an exception
        let warehouse_path = match self.context().await?.config.warehouse.as_deref() {
            Some(url) if Url::parse(url).is_ok() => Some(url),
            Some(_) => None,
            None => None,
        };

        let file_io = match metadata_location.or(warehouse_path) {
            Some(url) => FileIO::from_path(url)?
                .with_props(props)
                .with_extensions(self.file_io_extensions.clone())
                .build()?,
            None => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Unable to load file io, neither warehouse nor metadata location is set!",
                ))?;
            }
        };

        Ok(file_io)
    }

    /// Invalidate the current token without generating a new one. On the next request, the client
    /// will attempt to generate a new token.
    pub async fn invalidate_token(&self) -> Result<()> {
        self.context().await?.client.invalidate_token().await
    }

    /// Invalidate the current token and set a new one. Generates a new token before invalidating
    /// the current token, meaning the old token will be used until this function acquires the lock
    /// and overwrites the token.
    ///
    /// If credential is invalid, or the request fails, this method will return an error and leave
    /// the current token unchanged.
    pub async fn regenerate_token(&self) -> Result<()> {
        self.context().await?.client.regenerate_token().await
    }
}

/// All requests and expected responses are derived from the REST catalog API spec:
/// https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml
#[async_trait]
impl Catalog for RestCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        let context = self.context().await?;
        let endpoint = context.config.namespaces_endpoint();
        let mut namespaces = Vec::new();
        let mut next_token = None;

        loop {
            let mut request = context.client.request(Method::GET, endpoint.clone());

            // Filter on `parent={namespace}` if a parent namespace exists.
            if let Some(ns) = parent {
                request = request.query(&[("parent", ns.to_url_string())]);
            }

            if let Some(token) = next_token {
                request = request.query(&[("pageToken", token)]);
            }

            let http_response = context.client.query_catalog(request.build()?).await?;

            match http_response.status() {
                StatusCode::OK => {
                    let response =
                        deserialize_catalog_response::<ListNamespaceResponse>(http_response)
                            .await?;

                    namespaces.extend(response.namespaces);

                    match response.next_page_token {
                        Some(token) => next_token = Some(token),
                        None => break,
                    }
                }
                StatusCode::NOT_FOUND => {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "The parent parameter of the namespace provided does not exist",
                    ));
                }
                _ => return Err(deserialize_unexpected_catalog_error(http_response).await),
            }
        }

        Ok(namespaces)
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        let context = self.context().await?;

        let request = context
            .client
            .request(Method::POST, context.config.namespaces_endpoint())
            .json(&CreateNamespaceRequest {
                namespace: namespace.clone(),
                properties,
            })
            .build()?;

        let http_response = context.client.query_catalog(request).await?;

        match http_response.status() {
            StatusCode::OK => {
                let response =
                    deserialize_catalog_response::<NamespaceResponse>(http_response).await?;
                Ok(Namespace::from(response))
            }
            StatusCode::CONFLICT => Err(Error::new(
                ErrorKind::Unexpected,
                "Tried to create a namespace that already exists",
            )),
            _ => Err(deserialize_unexpected_catalog_error(http_response).await),
        }
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        let context = self.context().await?;

        let request = context
            .client
            .request(Method::GET, context.config.namespace_endpoint(namespace))
            .build()?;

        let http_response = context.client.query_catalog(request).await?;

        match http_response.status() {
            StatusCode::OK => {
                let response =
                    deserialize_catalog_response::<NamespaceResponse>(http_response).await?;
                Ok(Namespace::from(response))
            }
            StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::Unexpected,
                "Tried to get a namespace that does not exist",
            )),
            _ => Err(deserialize_unexpected_catalog_error(http_response).await),
        }
    }

    async fn namespace_exists(&self, ns: &NamespaceIdent) -> Result<bool> {
        let context = self.context().await?;

        let request = context
            .client
            .request(Method::HEAD, context.config.namespace_endpoint(ns))
            .build()?;

        let http_response = context.client.query_catalog(request).await?;

        match http_response.status() {
            StatusCode::NO_CONTENT | StatusCode::OK => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            _ => Err(deserialize_unexpected_catalog_error(http_response).await),
        }
    }

    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Updating namespace not supported yet!",
        ))
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        let context = self.context().await?;

        let request = context
            .client
            .request(Method::DELETE, context.config.namespace_endpoint(namespace))
            .build()?;

        let http_response = context.client.query_catalog(request).await?;

        match http_response.status() {
            StatusCode::NO_CONTENT | StatusCode::OK => Ok(()),
            StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::Unexpected,
                "Tried to drop a namespace that does not exist",
            )),
            _ => Err(deserialize_unexpected_catalog_error(http_response).await),
        }
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        let context = self.context().await?;
        let endpoint = context.config.tables_endpoint(namespace);
        let mut identifiers = Vec::new();
        let mut next_token = None;

        loop {
            let mut request = context.client.request(Method::GET, endpoint.clone());

            if let Some(token) = next_token {
                request = request.query(&[("pageToken", token)]);
            }

            let http_response = context.client.query_catalog(request.build()?).await?;

            match http_response.status() {
                StatusCode::OK => {
                    let response =
                        deserialize_catalog_response::<ListTablesResponse>(http_response).await?;

                    identifiers.extend(response.identifiers);

                    match response.next_page_token {
                        Some(token) => next_token = Some(token),
                        None => break,
                    }
                }
                StatusCode::NOT_FOUND => {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "Tried to list tables of a namespace that does not exist",
                    ));
                }
                _ => return Err(deserialize_unexpected_catalog_error(http_response).await),
            }
        }

        Ok(identifiers)
    }

    /// Create a new table inside the namespace.
    ///
    /// In the resulting table, if there are any config properties that
    /// are present in both the response from the REST server and the
    /// config provided when creating this `RestCatalog` instance then
    /// the value provided locally to the `RestCatalog` will take precedence.
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        let context = self.context().await?;

        let table_ident = TableIdent::new(namespace.clone(), creation.name.clone());

        let request = context
            .client
            .request(Method::POST, context.config.tables_endpoint(namespace))
            .json(&CreateTableRequest {
                name: creation.name,
                location: creation.location,
                schema: creation.schema,
                partition_spec: creation.partition_spec,
                write_order: creation.sort_order,
                stage_create: Some(false),
                properties: creation.properties,
            })
            .build()?;

        let http_response = context.client.query_catalog(request).await?;

        let response = match http_response.status() {
            StatusCode::OK => {
                deserialize_catalog_response::<LoadTableResult>(http_response).await?
            }
            StatusCode::NOT_FOUND => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Tried to create a table under a namespace that does not exist",
                ));
            }
            StatusCode::CONFLICT => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "The table already exists",
                ));
            }
            _ => return Err(deserialize_unexpected_catalog_error(http_response).await),
        };

        let metadata_location = response.metadata_location.as_ref().ok_or(Error::new(
            ErrorKind::DataInvalid,
            "Metadata location missing in `create_table` response!",
        ))?;

        let config = response
            .config
            .into_iter()
            .chain(self.user_config.props.clone())
            .collect();

        let file_io = self
            .load_file_io(Some(metadata_location), Some(config))
            .await?;

        let table_builder = Table::builder()
            .identifier(table_ident.clone())
            .file_io(file_io)
            .metadata(response.metadata);

        if let Some(metadata_location) = response.metadata_location {
            table_builder.metadata_location(metadata_location).build()
        } else {
            table_builder.build()
        }
    }

    /// Load table from the catalog.
    ///
    /// If there are any config properties that are present in both the response from the REST
    /// server and the config provided when creating this `RestCatalog` instance, then the value
    /// provided locally to the `RestCatalog` will take precedence.
    async fn load_table(&self, table_ident: &TableIdent) -> Result<Table> {
        let context = self.context().await?;

        let request = context
            .client
            .request(Method::GET, context.config.table_endpoint(table_ident))
            .build()?;

        let http_response = context.client.query_catalog(request).await?;

        let response = match http_response.status() {
            StatusCode::OK | StatusCode::NOT_MODIFIED => {
                deserialize_catalog_response::<LoadTableResult>(http_response).await?
            }
            StatusCode::NOT_FOUND => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Tried to load a table that does not exist",
                ));
            }
            _ => return Err(deserialize_unexpected_catalog_error(http_response).await),
        };

        let config = response
            .config
            .into_iter()
            .chain(self.user_config.props.clone())
            .collect();

        let file_io = self
            .load_file_io(response.metadata_location.as_deref(), Some(config))
            .await?;

        let table_builder = Table::builder()
            .identifier(table_ident.clone())
            .file_io(file_io)
            .metadata(response.metadata);

        if let Some(metadata_location) = response.metadata_location {
            table_builder.metadata_location(metadata_location).build()
        } else {
            table_builder.build()
        }
    }

    /// Drop a table from the catalog.
    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        let context = self.context().await?;

        let request = context
            .client
            .request(Method::DELETE, context.config.table_endpoint(table))
            .build()?;

        let http_response = context.client.query_catalog(request).await?;

        match http_response.status() {
            StatusCode::NO_CONTENT | StatusCode::OK => Ok(()),
            StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::Unexpected,
                "Tried to drop a table that does not exist",
            )),
            _ => Err(deserialize_unexpected_catalog_error(http_response).await),
        }
    }

    /// Check if a table exists in the catalog.
    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        let context = self.context().await?;

        let request = context
            .client
            .request(Method::HEAD, context.config.table_endpoint(table))
            .build()?;

        let http_response = context.client.query_catalog(request).await?;

        match http_response.status() {
            StatusCode::NO_CONTENT | StatusCode::OK => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            _ => Err(deserialize_unexpected_catalog_error(http_response).await),
        }
    }

    /// Rename a table in the catalog.
    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        let context = self.context().await?;

        let request = context
            .client
            .request(Method::POST, context.config.rename_table_endpoint())
            .json(&RenameTableRequest {
                source: src.clone(),
                destination: dest.clone(),
            })
            .build()?;

        let http_response = context.client.query_catalog(request).await?;

        match http_response.status() {
            StatusCode::NO_CONTENT | StatusCode::OK => Ok(()),
            StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::Unexpected,
                "Tried to rename a table that does not exist (is the namespace correct?)",
            )),
            StatusCode::CONFLICT => Err(Error::new(
                ErrorKind::Unexpected,
                "Tried to rename a table to a name that already exists",
            )),
            _ => Err(deserialize_unexpected_catalog_error(http_response).await),
        }
    }

    async fn register_table(
        &self,
        table_ident: &TableIdent,
        metadata_location: String,
    ) -> Result<Table> {
        let context = self.context().await?;

        let request = context
            .client
            .request(
                Method::POST,
                context
                    .config
                    .register_table_endpoint(table_ident.namespace()),
            )
            .json(&RegisterTableRequest {
                name: table_ident.name.clone(),
                metadata_location: metadata_location.clone(),
                overwrite: Some(false),
            })
            .build()?;

        let http_response = context.client.query_catalog(request).await?;

        let response: LoadTableResult = match http_response.status() {
            StatusCode::OK => {
                deserialize_catalog_response::<LoadTableResult>(http_response).await?
            }
            StatusCode::NOT_FOUND => {
                return Err(Error::new(
                    ErrorKind::NamespaceNotFound,
                    "The namespace specified does not exist.",
                ));
            }
            StatusCode::CONFLICT => {
                return Err(Error::new(
                    ErrorKind::TableAlreadyExists,
                    "The given table already exists.",
                ));
            }
            _ => return Err(deserialize_unexpected_catalog_error(http_response).await),
        };

        let metadata_location = response.metadata_location.as_ref().ok_or(Error::new(
            ErrorKind::DataInvalid,
            "Metadata location missing in `register_table` response!",
        ))?;

        let file_io = self.load_file_io(Some(metadata_location), None).await?;

        Table::builder()
            .identifier(table_ident.clone())
            .file_io(file_io)
            .metadata(response.metadata)
            .metadata_location(metadata_location.clone())
            .build()
    }

    async fn update_table(&self, mut commit: TableCommit) -> Result<Table> {
        let context = self.context().await?;

        let request = context
            .client
            .request(
                Method::POST,
                context.config.table_endpoint(commit.identifier()),
            )
            .json(&CommitTableRequest {
                identifier: Some(commit.identifier().clone()),
                requirements: commit.take_requirements(),
                updates: commit.take_updates(),
            })
            .build()?;

        let http_response = context.client.query_catalog(request).await?;

        let response: CommitTableResponse = match http_response.status() {
            StatusCode::OK => deserialize_catalog_response(http_response).await?,
            StatusCode::NOT_FOUND => {
                return Err(Error::new(
                    ErrorKind::TableNotFound,
                    "Tried to update a table that does not exist",
                ));
            }
            StatusCode::CONFLICT => {
                return Err(Error::new(
                    ErrorKind::CatalogCommitConflicts,
                    "CatalogCommitConflicts, one or more requirements failed. The client may retry.",
                )
                .with_retryable(true));
            }
            StatusCode::INTERNAL_SERVER_ERROR => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "An unknown server-side problem occurred; the commit state is unknown.",
                ));
            }
            StatusCode::BAD_GATEWAY => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "A gateway or proxy received an invalid response from the upstream server; the commit state is unknown.",
                ));
            }
            StatusCode::GATEWAY_TIMEOUT => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "A server-side gateway timeout occurred; the commit state is unknown.",
                ));
            }
            _ => return Err(deserialize_unexpected_catalog_error(http_response).await),
        };

        let file_io = self
            .load_file_io(Some(&response.metadata_location), None)
            .await?;

        Table::builder()
            .identifier(commit.identifier().clone())
            .file_io(file_io)
            .metadata(response.metadata)
            .metadata_location(response.metadata_location)
            .build()
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::Arc;

    use chrono::{TimeZone, Utc};
    use iceberg::spec::{
        FormatVersion, NestedField, NullOrder, Operation, PrimitiveType, Schema, Snapshot,
        SnapshotLog, SortDirection, SortField, SortOrder, Summary, Transform, Type,
        UnboundPartitionField, UnboundPartitionSpec,
    };
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use mockito::{Mock, Server, ServerGuard};
    use serde_json::json;
    use uuid::uuid;

    use super::*;

    #[tokio::test]
    async fn test_update_config() {
        let mut server = Server::new_async().await;

        let config_mock = server
            .mock("GET", "/v1/config")
            .with_status(200)
            .with_body(
                r#"{
                "overrides": {
                    "warehouse": "s3://iceberg-catalog"
                },
                "defaults": {}
            }"#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        assert_eq!(
            catalog
                .context()
                .await
                .unwrap()
                .config
                .props
                .get("warehouse"),
            Some(&"s3://iceberg-catalog".to_string())
        );

        config_mock.assert_async().await;
    }

    async fn create_config_mock(server: &mut ServerGuard) -> Mock {
        server
            .mock("GET", "/v1/config")
            .with_status(200)
            .with_body(
                r#"{
                "overrides": {
                    "warehouse": "s3://iceberg-catalog"
                },
                "defaults": {}
            }"#,
            )
            .create_async()
            .await
    }

    async fn create_oauth_mock(server: &mut ServerGuard) -> Mock {
        create_oauth_mock_with_path(server, "/v1/oauth/tokens", "ey000000000000", 200).await
    }

    async fn create_oauth_mock_with_path(
        server: &mut ServerGuard,
        path: &str,
        token: &str,
        status: usize,
    ) -> Mock {
        let body = format!(
            r#"{{
                "access_token": "{token}",
                "token_type": "Bearer",
                "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
                "expires_in": 86400
            }}"#
        );
        server
            .mock("POST", path)
            .with_status(status)
            .with_body(body)
            .expect(1)
            .create_async()
            .await
    }

    #[tokio::test]
    async fn test_oauth() {
        let mut server = Server::new_async().await;
        let oauth_mock = create_oauth_mock(&mut server).await;
        let config_mock = create_config_mock(&mut server).await;

        let mut props = HashMap::new();
        props.insert("credential".to_string(), "client1:secret1".to_string());

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder()
                .uri(server.url())
                .props(props)
                .build(),
        );

        let token = catalog.context().await.unwrap().client.token().await;
        oauth_mock.assert_async().await;
        config_mock.assert_async().await;
        assert_eq!(token, Some("ey000000000000".to_string()));
    }

    #[tokio::test]
    async fn test_oauth_with_optional_param() {
        let mut props = HashMap::new();
        props.insert("credential".to_string(), "client1:secret1".to_string());
        props.insert("scope".to_string(), "custom_scope".to_string());
        props.insert("audience".to_string(), "custom_audience".to_string());
        props.insert("resource".to_string(), "custom_resource".to_string());

        let mut server = Server::new_async().await;
        let oauth_mock = server
            .mock("POST", "/v1/oauth/tokens")
            .match_body(mockito::Matcher::Regex("scope=custom_scope".to_string()))
            .match_body(mockito::Matcher::Regex(
                "audience=custom_audience".to_string(),
            ))
            .match_body(mockito::Matcher::Regex(
                "resource=custom_resource".to_string(),
            ))
            .with_status(200)
            .with_body(
                r#"{
                "access_token": "ey000000000000",
                "token_type": "Bearer",
                "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
                "expires_in": 86400
                }"#,
            )
            .expect(1)
            .create_async()
            .await;

        let config_mock = create_config_mock(&mut server).await;

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder()
                .uri(server.url())
                .props(props)
                .build(),
        );

        let token = catalog.context().await.unwrap().client.token().await;

        oauth_mock.assert_async().await;
        config_mock.assert_async().await;
        assert_eq!(token, Some("ey000000000000".to_string()));
    }

    #[tokio::test]
    async fn test_invalidate_token() {
        let mut server = Server::new_async().await;
        let oauth_mock = create_oauth_mock(&mut server).await;
        let config_mock = create_config_mock(&mut server).await;

        let mut props = HashMap::new();
        props.insert("credential".to_string(), "client1:secret1".to_string());

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder()
                .uri(server.url())
                .props(props)
                .build(),
        );

        let token = catalog.context().await.unwrap().client.token().await;
        oauth_mock.assert_async().await;
        config_mock.assert_async().await;
        assert_eq!(token, Some("ey000000000000".to_string()));

        let oauth_mock =
            create_oauth_mock_with_path(&mut server, "/v1/oauth/tokens", "ey000000000001", 200)
                .await;
        catalog.invalidate_token().await.unwrap();
        let token = catalog.context().await.unwrap().client.token().await;
        oauth_mock.assert_async().await;
        assert_eq!(token, Some("ey000000000001".to_string()));
    }

    #[tokio::test]
    async fn test_invalidate_token_failing_request() {
        let mut server = Server::new_async().await;
        let oauth_mock = create_oauth_mock(&mut server).await;
        let config_mock = create_config_mock(&mut server).await;

        let mut props = HashMap::new();
        props.insert("credential".to_string(), "client1:secret1".to_string());

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder()
                .uri(server.url())
                .props(props)
                .build(),
        );

        let token = catalog.context().await.unwrap().client.token().await;
        oauth_mock.assert_async().await;
        config_mock.assert_async().await;
        assert_eq!(token, Some("ey000000000000".to_string()));

        let oauth_mock =
            create_oauth_mock_with_path(&mut server, "/v1/oauth/tokens", "ey000000000001", 500)
                .await;
        catalog.invalidate_token().await.unwrap();
        let token = catalog.context().await.unwrap().client.token().await;
        oauth_mock.assert_async().await;
        assert_eq!(token, None);
    }

    #[tokio::test]
    async fn test_regenerate_token() {
        let mut server = Server::new_async().await;
        let oauth_mock = create_oauth_mock(&mut server).await;
        let config_mock = create_config_mock(&mut server).await;

        let mut props = HashMap::new();
        props.insert("credential".to_string(), "client1:secret1".to_string());

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder()
                .uri(server.url())
                .props(props)
                .build(),
        );

        let token = catalog.context().await.unwrap().client.token().await;
        oauth_mock.assert_async().await;
        config_mock.assert_async().await;
        assert_eq!(token, Some("ey000000000000".to_string()));

        let oauth_mock =
            create_oauth_mock_with_path(&mut server, "/v1/oauth/tokens", "ey000000000001", 200)
                .await;
        catalog.regenerate_token().await.unwrap();
        oauth_mock.assert_async().await;
        let token = catalog.context().await.unwrap().client.token().await;
        assert_eq!(token, Some("ey000000000001".to_string()));
    }

    #[tokio::test]
    async fn test_regenerate_token_failing_request() {
        let mut server = Server::new_async().await;
        let oauth_mock = create_oauth_mock(&mut server).await;
        let config_mock = create_config_mock(&mut server).await;

        let mut props = HashMap::new();
        props.insert("credential".to_string(), "client1:secret1".to_string());

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder()
                .uri(server.url())
                .props(props)
                .build(),
        );

        let token = catalog.context().await.unwrap().client.token().await;
        oauth_mock.assert_async().await;
        config_mock.assert_async().await;
        assert_eq!(token, Some("ey000000000000".to_string()));

        let oauth_mock =
            create_oauth_mock_with_path(&mut server, "/v1/oauth/tokens", "ey000000000001", 500)
                .await;
        let invalidate_result = catalog.regenerate_token().await;
        assert!(invalidate_result.is_err());
        oauth_mock.assert_async().await;
        let token = catalog.context().await.unwrap().client.token().await;

        // original token is left intact
        assert_eq!(token, Some("ey000000000000".to_string()));
    }

    #[tokio::test]
    async fn test_http_headers() {
        let server = Server::new_async().await;
        let mut props = HashMap::new();
        props.insert("credential".to_string(), "client1:secret1".to_string());

        let config = RestCatalogConfig::builder()
            .uri(server.url())
            .props(props)
            .build();
        let headers: HeaderMap = config.extra_headers().unwrap();

        let expected_headers = HeaderMap::from_iter([
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            ),
            (
                HeaderName::from_static("x-client-version"),
                HeaderValue::from_static(ICEBERG_REST_SPEC_VERSION),
            ),
            (
                header::USER_AGENT,
                HeaderValue::from_str(&format!("iceberg-rs/{CARGO_PKG_VERSION}")).unwrap(),
            ),
        ]);
        assert_eq!(headers, expected_headers);
    }

    #[tokio::test]
    async fn test_http_headers_with_custom_headers() {
        let server = Server::new_async().await;
        let mut props = HashMap::new();
        props.insert("credential".to_string(), "client1:secret1".to_string());
        props.insert(
            "header.content-type".to_string(),
            "application/yaml".to_string(),
        );
        props.insert(
            "header.customized-header".to_string(),
            "some/value".to_string(),
        );

        let config = RestCatalogConfig::builder()
            .uri(server.url())
            .props(props)
            .build();
        let headers: HeaderMap = config.extra_headers().unwrap();

        let expected_headers = HeaderMap::from_iter([
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/yaml"),
            ),
            (
                HeaderName::from_static("x-client-version"),
                HeaderValue::from_static(ICEBERG_REST_SPEC_VERSION),
            ),
            (
                header::USER_AGENT,
                HeaderValue::from_str(&format!("iceberg-rs/{CARGO_PKG_VERSION}")).unwrap(),
            ),
            (
                HeaderName::from_static("customized-header"),
                HeaderValue::from_static("some/value"),
            ),
        ]);
        assert_eq!(headers, expected_headers);
    }

    #[tokio::test]
    async fn test_oauth_with_oauth2_server_uri() {
        let mut server = Server::new_async().await;
        let config_mock = create_config_mock(&mut server).await;

        let mut auth_server = Server::new_async().await;
        let auth_server_path = "/some/path";
        let oauth_mock =
            create_oauth_mock_with_path(&mut auth_server, auth_server_path, "ey000000000000", 200)
                .await;

        let mut props = HashMap::new();
        props.insert("credential".to_string(), "client1:secret1".to_string());
        props.insert(
            "oauth2-server-uri".to_string(),
            format!("{}{}", auth_server.url(), auth_server_path).to_string(),
        );

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder()
                .uri(server.url())
                .props(props)
                .build(),
        );

        let token = catalog.context().await.unwrap().client.token().await;

        oauth_mock.assert_async().await;
        config_mock.assert_async().await;
        assert_eq!(token, Some("ey000000000000".to_string()));
    }

    #[tokio::test]
    async fn test_config_override() {
        let mut server = Server::new_async().await;
        let mut redirect_server = Server::new_async().await;
        let new_uri = redirect_server.url();

        let config_mock = server
            .mock("GET", "/v1/config")
            .with_status(200)
            .with_body(
                json!(
                    {
                        "overrides": {
                            "uri": new_uri,
                            "warehouse": "s3://iceberg-catalog",
                            "prefix": "ice/warehouses/my"
                        },
                        "defaults": {},
                    }
                )
                .to_string(),
            )
            .create_async()
            .await;

        let list_ns_mock = redirect_server
            .mock("GET", "/v1/ice/warehouses/my/namespaces")
            .with_body(
                r#"{
                    "namespaces": []
                }"#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        let _namespaces = catalog.list_namespaces(None).await.unwrap();

        config_mock.assert_async().await;
        list_ns_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_list_namespace() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let list_ns_mock = server
            .mock("GET", "/v1/namespaces")
            .with_body(
                r#"{
                "namespaces": [
                    ["ns1", "ns11"],
                    ["ns2"]
                ]
            }"#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        let namespaces = catalog.list_namespaces(None).await.unwrap();

        let expected_ns = vec![
            NamespaceIdent::from_vec(vec!["ns1".to_string(), "ns11".to_string()]).unwrap(),
            NamespaceIdent::from_vec(vec!["ns2".to_string()]).unwrap(),
        ];

        assert_eq!(expected_ns, namespaces);

        config_mock.assert_async().await;
        list_ns_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_list_namespace_with_pagination() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let list_ns_mock_page1 = server
            .mock("GET", "/v1/namespaces")
            .with_body(
                r#"{
                "namespaces": [
                    ["ns1", "ns11"],
                    ["ns2"]
                ],
                "next-page-token": "token123"
            }"#,
            )
            .create_async()
            .await;

        let list_ns_mock_page2 = server
            .mock("GET", "/v1/namespaces?pageToken=token123")
            .with_body(
                r#"{
                "namespaces": [
                    ["ns3"],
                    ["ns4", "ns41"]
                ]
            }"#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        let namespaces = catalog.list_namespaces(None).await.unwrap();

        let expected_ns = vec![
            NamespaceIdent::from_vec(vec!["ns1".to_string(), "ns11".to_string()]).unwrap(),
            NamespaceIdent::from_vec(vec!["ns2".to_string()]).unwrap(),
            NamespaceIdent::from_vec(vec!["ns3".to_string()]).unwrap(),
            NamespaceIdent::from_vec(vec!["ns4".to_string(), "ns41".to_string()]).unwrap(),
        ];

        assert_eq!(expected_ns, namespaces);

        config_mock.assert_async().await;
        list_ns_mock_page1.assert_async().await;
        list_ns_mock_page2.assert_async().await;
    }

    #[tokio::test]
    async fn test_list_namespace_with_multiple_pages() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        // Page 1
        let list_ns_mock_page1 = server
            .mock("GET", "/v1/namespaces")
            .with_body(
                r#"{
                "namespaces": [
                    ["ns1", "ns11"],
                    ["ns2"]
                ],
                "next-page-token": "page2"
            }"#,
            )
            .create_async()
            .await;

        // Page 2
        let list_ns_mock_page2 = server
            .mock("GET", "/v1/namespaces?pageToken=page2")
            .with_body(
                r#"{
                "namespaces": [
                    ["ns3"],
                    ["ns4", "ns41"]
                ],
                "next-page-token": "page3"
            }"#,
            )
            .create_async()
            .await;

        // Page 3
        let list_ns_mock_page3 = server
            .mock("GET", "/v1/namespaces?pageToken=page3")
            .with_body(
                r#"{
                "namespaces": [
                    ["ns5", "ns51", "ns511"]
                ],
                "next-page-token": "page4"
            }"#,
            )
            .create_async()
            .await;

        // Page 4
        let list_ns_mock_page4 = server
            .mock("GET", "/v1/namespaces?pageToken=page4")
            .with_body(
                r#"{
                "namespaces": [
                    ["ns6"],
                    ["ns7"]
                ],
                "next-page-token": "page5"
            }"#,
            )
            .create_async()
            .await;

        // Page 5 (final page)
        let list_ns_mock_page5 = server
            .mock("GET", "/v1/namespaces?pageToken=page5")
            .with_body(
                r#"{
                "namespaces": [
                    ["ns8", "ns81"]
                ]
            }"#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        let namespaces = catalog.list_namespaces(None).await.unwrap();

        let expected_ns = vec![
            NamespaceIdent::from_vec(vec!["ns1".to_string(), "ns11".to_string()]).unwrap(),
            NamespaceIdent::from_vec(vec!["ns2".to_string()]).unwrap(),
            NamespaceIdent::from_vec(vec!["ns3".to_string()]).unwrap(),
            NamespaceIdent::from_vec(vec!["ns4".to_string(), "ns41".to_string()]).unwrap(),
            NamespaceIdent::from_vec(vec![
                "ns5".to_string(),
                "ns51".to_string(),
                "ns511".to_string(),
            ])
            .unwrap(),
            NamespaceIdent::from_vec(vec!["ns6".to_string()]).unwrap(),
            NamespaceIdent::from_vec(vec!["ns7".to_string()]).unwrap(),
            NamespaceIdent::from_vec(vec!["ns8".to_string(), "ns81".to_string()]).unwrap(),
        ];

        assert_eq!(expected_ns, namespaces);

        // Verify all page requests were made
        config_mock.assert_async().await;
        list_ns_mock_page1.assert_async().await;
        list_ns_mock_page2.assert_async().await;
        list_ns_mock_page3.assert_async().await;
        list_ns_mock_page4.assert_async().await;
        list_ns_mock_page5.assert_async().await;
    }

    #[tokio::test]
    async fn test_create_namespace() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let create_ns_mock = server
            .mock("POST", "/v1/namespaces")
            .with_body(
                r#"{
                "namespace": [ "ns1", "ns11"],
                "properties" : {
                    "key1": "value1"
                }
            }"#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        let namespaces = catalog
            .create_namespace(
                &NamespaceIdent::from_vec(vec!["ns1".to_string(), "ns11".to_string()]).unwrap(),
                HashMap::from([("key1".to_string(), "value1".to_string())]),
            )
            .await
            .unwrap();

        let expected_ns = Namespace::with_properties(
            NamespaceIdent::from_vec(vec!["ns1".to_string(), "ns11".to_string()]).unwrap(),
            HashMap::from([("key1".to_string(), "value1".to_string())]),
        );

        assert_eq!(expected_ns, namespaces);

        config_mock.assert_async().await;
        create_ns_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_get_namespace() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let get_ns_mock = server
            .mock("GET", "/v1/namespaces/ns1")
            .with_body(
                r#"{
                "namespace": [ "ns1"],
                "properties" : {
                    "key1": "value1"
                }
            }"#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        let namespaces = catalog
            .get_namespace(&NamespaceIdent::new("ns1".to_string()))
            .await
            .unwrap();

        let expected_ns = Namespace::with_properties(
            NamespaceIdent::new("ns1".to_string()),
            HashMap::from([("key1".to_string(), "value1".to_string())]),
        );

        assert_eq!(expected_ns, namespaces);

        config_mock.assert_async().await;
        get_ns_mock.assert_async().await;
    }

    #[tokio::test]
    async fn check_namespace_exists() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let get_ns_mock = server
            .mock("HEAD", "/v1/namespaces/ns1")
            .with_status(204)
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        assert!(
            catalog
                .namespace_exists(&NamespaceIdent::new("ns1".to_string()))
                .await
                .unwrap()
        );

        config_mock.assert_async().await;
        get_ns_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_drop_namespace() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let drop_ns_mock = server
            .mock("DELETE", "/v1/namespaces/ns1")
            .with_status(204)
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        catalog
            .drop_namespace(&NamespaceIdent::new("ns1".to_string()))
            .await
            .unwrap();

        config_mock.assert_async().await;
        drop_ns_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_list_tables() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let list_tables_mock = server
            .mock("GET", "/v1/namespaces/ns1/tables")
            .with_status(200)
            .with_body(
                r#"{
                "identifiers": [
                    {
                        "namespace": ["ns1"],
                        "name": "table1"
                    },
                    {
                        "namespace": ["ns1"],
                        "name": "table2"
                    }
                ]
            }"#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        let tables = catalog
            .list_tables(&NamespaceIdent::new("ns1".to_string()))
            .await
            .unwrap();

        let expected_tables = vec![
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table1".to_string()),
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table2".to_string()),
        ];

        assert_eq!(tables, expected_tables);

        config_mock.assert_async().await;
        list_tables_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_list_tables_with_pagination() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let list_tables_mock_page1 = server
            .mock("GET", "/v1/namespaces/ns1/tables")
            .with_status(200)
            .with_body(
                r#"{
                "identifiers": [
                    {
                        "namespace": ["ns1"],
                        "name": "table1"
                    },
                    {
                        "namespace": ["ns1"],
                        "name": "table2"
                    }
                ],
                "next-page-token": "token456"
            }"#,
            )
            .create_async()
            .await;

        let list_tables_mock_page2 = server
            .mock("GET", "/v1/namespaces/ns1/tables?pageToken=token456")
            .with_status(200)
            .with_body(
                r#"{
                "identifiers": [
                    {
                        "namespace": ["ns1"],
                        "name": "table3"
                    },
                    {
                        "namespace": ["ns1"],
                        "name": "table4"
                    }
                ]
            }"#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        let tables = catalog
            .list_tables(&NamespaceIdent::new("ns1".to_string()))
            .await
            .unwrap();

        let expected_tables = vec![
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table1".to_string()),
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table2".to_string()),
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table3".to_string()),
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table4".to_string()),
        ];

        assert_eq!(tables, expected_tables);

        config_mock.assert_async().await;
        list_tables_mock_page1.assert_async().await;
        list_tables_mock_page2.assert_async().await;
    }

    #[tokio::test]
    async fn test_list_tables_with_multiple_pages() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        // Page 1
        let list_tables_mock_page1 = server
            .mock("GET", "/v1/namespaces/ns1/tables")
            .with_status(200)
            .with_body(
                r#"{
                "identifiers": [
                    {
                        "namespace": ["ns1"],
                        "name": "table1"
                    },
                    {
                        "namespace": ["ns1"],
                        "name": "table2"
                    }
                ],
                "next-page-token": "page2"
            }"#,
            )
            .create_async()
            .await;

        // Page 2
        let list_tables_mock_page2 = server
            .mock("GET", "/v1/namespaces/ns1/tables?pageToken=page2")
            .with_status(200)
            .with_body(
                r#"{
                "identifiers": [
                    {
                        "namespace": ["ns1"],
                        "name": "table3"
                    },
                    {
                        "namespace": ["ns1"],
                        "name": "table4"
                    }
                ],
                "next-page-token": "page3"
            }"#,
            )
            .create_async()
            .await;

        // Page 3
        let list_tables_mock_page3 = server
            .mock("GET", "/v1/namespaces/ns1/tables?pageToken=page3")
            .with_status(200)
            .with_body(
                r#"{
                "identifiers": [
                    {
                        "namespace": ["ns1"],
                        "name": "table5"
                    }
                ],
                "next-page-token": "page4"
            }"#,
            )
            .create_async()
            .await;

        // Page 4
        let list_tables_mock_page4 = server
            .mock("GET", "/v1/namespaces/ns1/tables?pageToken=page4")
            .with_status(200)
            .with_body(
                r#"{
                "identifiers": [
                    {
                        "namespace": ["ns1"],
                        "name": "table6"
                    },
                    {
                        "namespace": ["ns1"],
                        "name": "table7"
                    }
                ],
                "next-page-token": "page5"
            }"#,
            )
            .create_async()
            .await;

        // Page 5 (final page)
        let list_tables_mock_page5 = server
            .mock("GET", "/v1/namespaces/ns1/tables?pageToken=page5")
            .with_status(200)
            .with_body(
                r#"{
                "identifiers": [
                    {
                        "namespace": ["ns1"],
                        "name": "table8"
                    }
                ]
            }"#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        let tables = catalog
            .list_tables(&NamespaceIdent::new("ns1".to_string()))
            .await
            .unwrap();

        let expected_tables = vec![
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table1".to_string()),
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table2".to_string()),
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table3".to_string()),
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table4".to_string()),
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table5".to_string()),
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table6".to_string()),
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table7".to_string()),
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table8".to_string()),
        ];

        assert_eq!(tables, expected_tables);

        // Verify all page requests were made
        config_mock.assert_async().await;
        list_tables_mock_page1.assert_async().await;
        list_tables_mock_page2.assert_async().await;
        list_tables_mock_page3.assert_async().await;
        list_tables_mock_page4.assert_async().await;
        list_tables_mock_page5.assert_async().await;
    }

    #[tokio::test]
    async fn test_drop_tables() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let delete_table_mock = server
            .mock("DELETE", "/v1/namespaces/ns1/tables/table1")
            .with_status(204)
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        catalog
            .drop_table(&TableIdent::new(
                NamespaceIdent::new("ns1".to_string()),
                "table1".to_string(),
            ))
            .await
            .unwrap();

        config_mock.assert_async().await;
        delete_table_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_check_table_exists() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let check_table_exists_mock = server
            .mock("HEAD", "/v1/namespaces/ns1/tables/table1")
            .with_status(204)
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        assert!(
            catalog
                .table_exists(&TableIdent::new(
                    NamespaceIdent::new("ns1".to_string()),
                    "table1".to_string(),
                ))
                .await
                .unwrap()
        );

        config_mock.assert_async().await;
        check_table_exists_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_rename_table() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let rename_table_mock = server
            .mock("POST", "/v1/tables/rename")
            .with_status(204)
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        catalog
            .rename_table(
                &TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table1".to_string()),
                &TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table2".to_string()),
            )
            .await
            .unwrap();

        config_mock.assert_async().await;
        rename_table_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_load_table() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let rename_table_mock = server
            .mock("GET", "/v1/namespaces/ns1/tables/test1")
            .with_status(200)
            .with_body_from_file(format!(
                "{}/testdata/{}",
                env!("CARGO_MANIFEST_DIR"),
                "load_table_response.json"
            ))
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        let table = catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new("ns1".to_string()),
                "test1".to_string(),
            ))
            .await
            .unwrap();

        assert_eq!(
            &TableIdent::from_strs(vec!["ns1", "test1"]).unwrap(),
            table.identifier()
        );
        assert_eq!(
            "s3://warehouse/database/table/metadata/00001-5f2f8166-244c-4eae-ac36-384ecdec81fc.gz.metadata.json",
            table.metadata_location().unwrap()
        );
        assert_eq!(FormatVersion::V1, table.metadata().format_version());
        assert_eq!("s3://warehouse/database/table", table.metadata().location());
        assert_eq!(
            uuid!("b55d9dda-6561-423a-8bfc-787980ce421f"),
            table.metadata().uuid()
        );
        assert_eq!(
            Utc.timestamp_millis_opt(1646787054459).unwrap(),
            table.metadata().last_updated_timestamp().unwrap()
        );
        assert_eq!(
            vec![&Arc::new(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                        NestedField::optional(2, "data", Type::Primitive(PrimitiveType::String))
                            .into(),
                    ])
                    .build()
                    .unwrap()
            )],
            table.metadata().schemas_iter().collect::<Vec<_>>()
        );
        assert_eq!(
            &HashMap::from([
                ("owner".to_string(), "bryan".to_string()),
                (
                    "write.metadata.compression-codec".to_string(),
                    "gzip".to_string()
                )
            ]),
            table.metadata().properties()
        );
        assert_eq!(vec![&Arc::new(Snapshot::builder()
            .with_snapshot_id(3497810964824022504)
            .with_timestamp_ms(1646787054459)
            .with_manifest_list("s3://warehouse/database/table/metadata/snap-3497810964824022504-1-c4f68204-666b-4e50-a9df-b10c34bf6b82.avro")
            .with_sequence_number(0)
            .with_schema_id(0)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::from_iter([
                    ("spark.app.id", "local-1646787004168"),
                    ("added-data-files", "1"),
                    ("added-records", "1"),
                    ("added-files-size", "697"),
                    ("changed-partition-count", "1"),
                    ("total-records", "1"),
                    ("total-files-size", "697"),
                    ("total-data-files", "1"),
                    ("total-delete-files", "0"),
                    ("total-position-deletes", "0"),
                    ("total-equality-deletes", "0")
                ].iter().map(|p| (p.0.to_string(), p.1.to_string()))),
            }).build()
        )], table.metadata().snapshots().collect::<Vec<_>>());
        assert_eq!(
            &[SnapshotLog {
                timestamp_ms: 1646787054459,
                snapshot_id: 3497810964824022504,
            }],
            table.metadata().history()
        );
        assert_eq!(
            vec![&Arc::new(SortOrder {
                order_id: 0,
                fields: vec![],
            })],
            table.metadata().sort_orders_iter().collect::<Vec<_>>()
        );

        config_mock.assert_async().await;
        rename_table_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_load_table_404() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let rename_table_mock = server
            .mock("GET", "/v1/namespaces/ns1/tables/test1")
            .with_status(404)
            .with_body(r#"
{
    "error": {
        "message": "Table does not exist: ns1.test1 in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
        "type": "NoSuchNamespaceErrorException",
        "code": 404
    }
}
            "#)
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        let table = catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new("ns1".to_string()),
                "test1".to_string(),
            ))
            .await;

        assert!(table.is_err());
        assert!(table.err().unwrap().message().contains("does not exist"));

        config_mock.assert_async().await;
        rename_table_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_create_table() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let create_table_mock = server
            .mock("POST", "/v1/namespaces/ns1/tables")
            .with_status(200)
            .with_body_from_file(format!(
                "{}/testdata/{}",
                env!("CARGO_MANIFEST_DIR"),
                "create_table_response.json"
            ))
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        let table_creation = TableCreation::builder()
            .name("test1".to_string())
            .schema(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                        NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean))
                            .into(),
                    ])
                    .with_schema_id(1)
                    .with_identifier_field_ids(vec![2])
                    .build()
                    .unwrap(),
            )
            .properties(HashMap::from([("owner".to_string(), "testx".to_string())]))
            .partition_spec(
                UnboundPartitionSpec::builder()
                    .add_partition_fields(vec![
                        UnboundPartitionField::builder()
                            .source_id(1)
                            .transform(Transform::Truncate(3))
                            .name("id".to_string())
                            .build(),
                    ])
                    .unwrap()
                    .build(),
            )
            .sort_order(
                SortOrder::builder()
                    .with_sort_field(
                        SortField::builder()
                            .source_id(2)
                            .transform(Transform::Identity)
                            .direction(SortDirection::Ascending)
                            .null_order(NullOrder::First)
                            .build(),
                    )
                    .build_unbound()
                    .unwrap(),
            )
            .build();

        let table = catalog
            .create_table(&NamespaceIdent::from_strs(["ns1"]).unwrap(), table_creation)
            .await
            .unwrap();

        assert_eq!(
            &TableIdent::from_strs(vec!["ns1", "test1"]).unwrap(),
            table.identifier()
        );
        assert_eq!(
            "s3://warehouse/database/table/metadata.json",
            table.metadata_location().unwrap()
        );
        assert_eq!(FormatVersion::V1, table.metadata().format_version());
        assert_eq!("s3://warehouse/database/table", table.metadata().location());
        assert_eq!(
            uuid!("bf289591-dcc0-4234-ad4f-5c3eed811a29"),
            table.metadata().uuid()
        );
        assert_eq!(
            1657810967051,
            table
                .metadata()
                .last_updated_timestamp()
                .unwrap()
                .timestamp_millis()
        );
        assert_eq!(
            vec![&Arc::new(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                        NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean))
                            .into(),
                    ])
                    .with_schema_id(0)
                    .with_identifier_field_ids(vec![2])
                    .build()
                    .unwrap()
            )],
            table.metadata().schemas_iter().collect::<Vec<_>>()
        );
        assert_eq!(
            &HashMap::from([
                (
                    "write.delete.parquet.compression-codec".to_string(),
                    "zstd".to_string()
                ),
                (
                    "write.metadata.compression-codec".to_string(),
                    "gzip".to_string()
                ),
                (
                    "write.summary.partition-limit".to_string(),
                    "100".to_string()
                ),
                (
                    "write.parquet.compression-codec".to_string(),
                    "zstd".to_string()
                ),
            ]),
            table.metadata().properties()
        );
        assert!(table.metadata().current_snapshot().is_none());
        assert!(table.metadata().history().is_empty());
        assert_eq!(
            vec![&Arc::new(SortOrder {
                order_id: 0,
                fields: vec![],
            })],
            table.metadata().sort_orders_iter().collect::<Vec<_>>()
        );

        config_mock.assert_async().await;
        create_table_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_create_table_409() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let create_table_mock = server
            .mock("POST", "/v1/namespaces/ns1/tables")
            .with_status(409)
            .with_body(r#"
{
    "error": {
        "message": "Table already exists: ns1.test1 in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
        "type": "AlreadyExistsException",
        "code": 409
    }
}
            "#)
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        let table_creation = TableCreation::builder()
            .name("test1".to_string())
            .schema(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                        NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean))
                            .into(),
                    ])
                    .with_schema_id(1)
                    .with_identifier_field_ids(vec![2])
                    .build()
                    .unwrap(),
            )
            .properties(HashMap::from([("owner".to_string(), "testx".to_string())]))
            .build();

        let table_result = catalog
            .create_table(&NamespaceIdent::from_strs(["ns1"]).unwrap(), table_creation)
            .await;

        assert!(table_result.is_err());
        assert!(
            table_result
                .err()
                .unwrap()
                .message()
                .contains("already exists")
        );

        config_mock.assert_async().await;
        create_table_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_update_table() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let load_table_mock = server
            .mock("GET", "/v1/namespaces/ns1/tables/test1")
            .with_status(200)
            .with_body_from_file(format!(
                "{}/testdata/{}",
                env!("CARGO_MANIFEST_DIR"),
                "load_table_response.json"
            ))
            .create_async()
            .await;

        let update_table_mock = server
            .mock("POST", "/v1/namespaces/ns1/tables/test1")
            .with_status(200)
            .with_body_from_file(format!(
                "{}/testdata/{}",
                env!("CARGO_MANIFEST_DIR"),
                "update_table_response.json"
            ))
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        let table1 = {
            let file = File::open(format!(
                "{}/testdata/{}",
                env!("CARGO_MANIFEST_DIR"),
                "create_table_response.json"
            ))
            .unwrap();
            let reader = BufReader::new(file);
            let resp = serde_json::from_reader::<_, LoadTableResult>(reader).unwrap();

            Table::builder()
                .metadata(resp.metadata)
                .metadata_location(resp.metadata_location.unwrap())
                .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
                .file_io(FileIO::from_path("/tmp").unwrap().build().unwrap())
                .build()
                .unwrap()
        };

        let tx = Transaction::new(&table1);
        let table = tx
            .upgrade_table_version()
            .set_format_version(FormatVersion::V2)
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        assert_eq!(
            &TableIdent::from_strs(vec!["ns1", "test1"]).unwrap(),
            table.identifier()
        );
        assert_eq!(
            "s3://warehouse/database/table/metadata.json",
            table.metadata_location().unwrap()
        );
        assert_eq!(FormatVersion::V2, table.metadata().format_version());
        assert_eq!("s3://warehouse/database/table", table.metadata().location());
        assert_eq!(
            uuid!("bf289591-dcc0-4234-ad4f-5c3eed811a29"),
            table.metadata().uuid()
        );
        assert_eq!(
            1657810967051,
            table
                .metadata()
                .last_updated_timestamp()
                .unwrap()
                .timestamp_millis()
        );
        assert_eq!(
            vec![&Arc::new(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                        NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean))
                            .into(),
                    ])
                    .with_schema_id(0)
                    .with_identifier_field_ids(vec![2])
                    .build()
                    .unwrap()
            )],
            table.metadata().schemas_iter().collect::<Vec<_>>()
        );
        assert_eq!(
            &HashMap::from([
                (
                    "write.delete.parquet.compression-codec".to_string(),
                    "zstd".to_string()
                ),
                (
                    "write.metadata.compression-codec".to_string(),
                    "gzip".to_string()
                ),
                (
                    "write.summary.partition-limit".to_string(),
                    "100".to_string()
                ),
                (
                    "write.parquet.compression-codec".to_string(),
                    "zstd".to_string()
                ),
            ]),
            table.metadata().properties()
        );
        assert!(table.metadata().current_snapshot().is_none());
        assert!(table.metadata().history().is_empty());
        assert_eq!(
            vec![&Arc::new(SortOrder {
                order_id: 0,
                fields: vec![],
            })],
            table.metadata().sort_orders_iter().collect::<Vec<_>>()
        );

        config_mock.assert_async().await;
        update_table_mock.assert_async().await;
        load_table_mock.assert_async().await
    }

    #[tokio::test]
    async fn test_update_table_404() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let load_table_mock = server
            .mock("GET", "/v1/namespaces/ns1/tables/test1")
            .with_status(200)
            .with_body_from_file(format!(
                "{}/testdata/{}",
                env!("CARGO_MANIFEST_DIR"),
                "load_table_response.json"
            ))
            .create_async()
            .await;

        let update_table_mock = server
            .mock("POST", "/v1/namespaces/ns1/tables/test1")
            .with_status(404)
            .with_body(
                r#"
{
    "error": {
        "message": "The given table does not exist",
        "type": "NoSuchTableException",
        "code": 404
    }
}
            "#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        let table1 = {
            let file = File::open(format!(
                "{}/testdata/{}",
                env!("CARGO_MANIFEST_DIR"),
                "create_table_response.json"
            ))
            .unwrap();
            let reader = BufReader::new(file);
            let resp = serde_json::from_reader::<_, LoadTableResult>(reader).unwrap();

            Table::builder()
                .metadata(resp.metadata)
                .metadata_location(resp.metadata_location.unwrap())
                .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
                .file_io(FileIO::from_path("/tmp").unwrap().build().unwrap())
                .build()
                .unwrap()
        };

        let tx = Transaction::new(&table1);
        let table_result = tx
            .upgrade_table_version()
            .set_format_version(FormatVersion::V2)
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await;

        assert!(table_result.is_err());
        assert!(
            table_result
                .err()
                .unwrap()
                .message()
                .contains("does not exist")
        );

        config_mock.assert_async().await;
        update_table_mock.assert_async().await;
        load_table_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_register_table() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let register_table_mock = server
            .mock("POST", "/v1/namespaces/ns1/register")
            .with_status(200)
            .with_body_from_file(format!(
                "{}/testdata/{}",
                env!("CARGO_MANIFEST_DIR"),
                "load_table_response.json"
            ))
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());
        let table_ident =
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "test1".to_string());
        let metadata_location = String::from(
            "s3://warehouse/database/table/metadata/00001-5f2f8166-244c-4eae-ac36-384ecdec81fc.gz.metadata.json",
        );

        let table = catalog
            .register_table(&table_ident, metadata_location)
            .await
            .unwrap();

        assert_eq!(
            &TableIdent::from_strs(vec!["ns1", "test1"]).unwrap(),
            table.identifier()
        );
        assert_eq!(
            "s3://warehouse/database/table/metadata/00001-5f2f8166-244c-4eae-ac36-384ecdec81fc.gz.metadata.json",
            table.metadata_location().unwrap()
        );

        config_mock.assert_async().await;
        register_table_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_register_table_404() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let register_table_mock = server
            .mock("POST", "/v1/namespaces/ns1/register")
            .with_status(404)
            .with_body(
                r#"
{
    "error": {
        "message": "The namespace specified does not exist",
        "type": "NoSuchNamespaceErrorException",
        "code": 404
    }
}
            "#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        let table_ident =
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "test1".to_string());
        let metadata_location = String::from(
            "s3://warehouse/database/table/metadata/00001-5f2f8166-244c-4eae-ac36-384ecdec81fc.gz.metadata.json",
        );
        let table = catalog
            .register_table(&table_ident, metadata_location)
            .await;

        assert!(table.is_err());
        assert!(table.err().unwrap().message().contains("does not exist"));

        config_mock.assert_async().await;
        register_table_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_create_rest_catalog() {
        let builder = RestCatalogBuilder::default().with_client(Client::new());

        let catalog = builder
            .load(
                "test",
                HashMap::from([
                    (
                        REST_CATALOG_PROP_URI.to_string(),
                        "http://localhost:8080".to_string(),
                    ),
                    ("a".to_string(), "b".to_string()),
                ]),
            )
            .await;

        assert!(catalog.is_ok());

        let catalog_config = catalog.unwrap().user_config;
        assert_eq!(catalog_config.name.as_deref(), Some("test"));
        assert_eq!(catalog_config.uri, "http://localhost:8080");
        assert_eq!(catalog_config.warehouse, None);
        assert!(catalog_config.client.is_some());

        assert_eq!(catalog_config.props.get("a"), Some(&"b".to_string()));
        assert!(!catalog_config.props.contains_key(REST_CATALOG_PROP_URI));
    }

    #[tokio::test]
    async fn test_create_rest_catalog_no_uri() {
        let builder = RestCatalogBuilder::default();

        let catalog = builder
            .load(
                "test",
                HashMap::from([(
                    REST_CATALOG_PROP_WAREHOUSE.to_string(),
                    "s3://warehouse".to_string(),
                )]),
            )
            .await;

        assert!(catalog.is_err());
        if let Err(err) = catalog {
            assert_eq!(err.kind(), ErrorKind::DataInvalid);
            assert_eq!(err.message(), "Catalog uri is required");
        }
    }
}
