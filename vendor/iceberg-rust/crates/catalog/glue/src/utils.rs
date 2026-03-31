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

use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_glue::config::Credentials;
use aws_sdk_glue::types::{Database, DatabaseInput, StorageDescriptor, TableInput};
use iceberg::spec::TableMetadata;
use iceberg::{Error, ErrorKind, Namespace, NamespaceIdent, Result};

use crate::error::from_aws_build_error;
use crate::schema::GlueSchemaBuilder;

/// Property aws profile name
pub const AWS_PROFILE_NAME: &str = "profile_name";
/// Property aws region
pub const AWS_REGION_NAME: &str = "region_name";
/// Property aws access key
pub const AWS_ACCESS_KEY_ID: &str = "aws_access_key_id";
/// Property aws secret access key
pub const AWS_SECRET_ACCESS_KEY: &str = "aws_secret_access_key";
/// Property aws session token
pub const AWS_SESSION_TOKEN: &str = "aws_session_token";
/// Parameter namespace description
const DESCRIPTION: &str = "description";
/// Parameter namespace location uri
const LOCATION: &str = "location_uri";
/// Property `metadata_location` for `TableInput`
const METADATA_LOCATION: &str = "metadata_location";
/// Property `previous_metadata_location` for `TableInput`
const PREV_METADATA_LOCATION: &str = "previous_metadata_location";
/// Property external table for `TableInput`
const EXTERNAL_TABLE: &str = "EXTERNAL_TABLE";
/// Parameter key `table_type` for `TableInput`
const TABLE_TYPE: &str = "table_type";
/// Parameter value `table_type` for `TableInput`
const ICEBERG: &str = "ICEBERG";

/// Creates an aws sdk configuration based on
/// provided properties and an optional endpoint URL.
pub(crate) async fn create_sdk_config(
    properties: &HashMap<String, String>,
    endpoint_uri: Option<&String>,
) -> SdkConfig {
    let mut config = aws_config::defaults(BehaviorVersion::latest());

    if let Some(endpoint) = endpoint_uri {
        config = config.endpoint_url(endpoint)
    };

    if properties.is_empty() {
        return config.load().await;
    }

    if let (Some(access_key), Some(secret_key)) = (
        properties.get(AWS_ACCESS_KEY_ID),
        properties.get(AWS_SECRET_ACCESS_KEY),
    ) {
        let session_token = properties.get(AWS_SESSION_TOKEN).cloned();
        let credentials_provider =
            Credentials::new(access_key, secret_key, session_token, None, "properties");

        config = config.credentials_provider(credentials_provider)
    };

    if let Some(profile_name) = properties.get(AWS_PROFILE_NAME) {
        config = config.profile_name(profile_name);
    }

    if let Some(region_name) = properties.get(AWS_REGION_NAME) {
        let region = Region::new(region_name.clone());
        config = config.region(region);
    }

    config.load().await
}

/// Create `DatabaseInput` from `NamespaceIdent` and properties
pub(crate) fn convert_to_database(
    namespace: &NamespaceIdent,
    properties: &HashMap<String, String>,
) -> Result<DatabaseInput> {
    let db_name = validate_namespace(namespace)?;
    let mut builder = DatabaseInput::builder().name(db_name);

    for (k, v) in properties.iter() {
        match k.as_ref() {
            DESCRIPTION => {
                builder = builder.description(v);
            }
            LOCATION => {
                builder = builder.location_uri(v);
            }
            _ => {
                builder = builder.parameters(k, v);
            }
        }
    }

    builder.build().map_err(from_aws_build_error)
}

/// Create `Namespace` from aws sdk glue `Database`
pub(crate) fn convert_to_namespace(database: &Database) -> Namespace {
    let db_name = database.name().to_string();
    let mut properties = database
        .parameters()
        .map_or_else(HashMap::new, |p| p.clone());

    if let Some(location_uri) = database.location_uri() {
        properties.insert(LOCATION.to_string(), location_uri.to_string());
    };

    if let Some(description) = database.description() {
        properties.insert(DESCRIPTION.to_string(), description.to_string());
    }

    Namespace::with_properties(NamespaceIdent::new(db_name), properties)
}

/// Converts Iceberg table metadata into an
/// AWS Glue `TableInput` representation.
///
/// This function facilitates the integration of Iceberg tables with AWS Glue
/// by converting Iceberg table metadata into a Glue-compatible `TableInput`
/// structure.
pub(crate) fn convert_to_glue_table(
    table_name: impl Into<String>,
    metadata_location: String,
    metadata: &TableMetadata,
    properties: &HashMap<String, String>,
    prev_metadata_location: Option<String>,
) -> Result<TableInput> {
    let glue_schema = GlueSchemaBuilder::from_iceberg(metadata)?.build();

    let storage_descriptor = StorageDescriptor::builder()
        .set_columns(Some(glue_schema))
        .location(metadata.location().to_string())
        .build();

    let mut parameters = HashMap::from([
        (TABLE_TYPE.to_string(), ICEBERG.to_string()),
        (METADATA_LOCATION.to_string(), metadata_location),
    ]);

    if let Some(prev) = prev_metadata_location {
        parameters.insert(PREV_METADATA_LOCATION.to_string(), prev);
    }

    let mut table_input_builder = TableInput::builder()
        .name(table_name)
        .set_parameters(Some(parameters))
        .storage_descriptor(storage_descriptor)
        .table_type(EXTERNAL_TABLE);

    if let Some(description) = properties.get(DESCRIPTION) {
        table_input_builder = table_input_builder.description(description);
    }

    let table_input = table_input_builder.build().map_err(from_aws_build_error)?;

    Ok(table_input)
}

/// Checks if provided `NamespaceIdent` is valid
pub(crate) fn validate_namespace(namespace: &NamespaceIdent) -> Result<String> {
    let name = namespace.as_ref();

    if name.len() != 1 {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Invalid database name: {namespace:?}, hierarchical namespaces are not supported"
            ),
        ));
    }

    let name = name[0].clone();

    if name.is_empty() {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "Invalid database, provided namespace is empty.",
        ));
    }

    Ok(name)
}

/// Get default table location from `Namespace` properties
pub(crate) fn get_default_table_location(
    namespace: &Namespace,
    db_name: impl AsRef<str>,
    table_name: impl AsRef<str>,
    warehouse: impl AsRef<str>,
) -> String {
    let properties = namespace.properties();

    match properties.get(LOCATION) {
        Some(location) => format!("{}/{}", location, table_name.as_ref()),
        None => {
            let warehouse_location = warehouse.as_ref().trim_end_matches('/');

            format!(
                "{}/{}.db/{}",
                warehouse_location,
                db_name.as_ref(),
                table_name.as_ref()
            )
        }
    }
}

/// Get metadata location from `GlueTable` parameters
pub(crate) fn get_metadata_location(
    parameters: &Option<HashMap<String, String>>,
) -> Result<String> {
    match parameters {
        Some(properties) => match properties.get(METADATA_LOCATION) {
            Some(location) => Ok(location.to_string()),
            None => Err(Error::new(
                ErrorKind::DataInvalid,
                format!("No '{METADATA_LOCATION}' set on table"),
            )),
        },
        None => Err(Error::new(
            ErrorKind::DataInvalid,
            "No 'parameters' set on table. Location of metadata is undefined",
        )),
    }
}

#[macro_export]
/// Extends aws sdk builder with `catalog_id` if present
macro_rules! with_catalog_id {
    ($builder:expr, $config:expr) => {{
        if let Some(catalog_id) = &$config.catalog_id {
            $builder.catalog_id(catalog_id)
        } else {
            $builder
        }
    }};
}

#[cfg(test)]
mod tests {
    use aws_sdk_glue::config::ProvideCredentials;
    use aws_sdk_glue::types::Column;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, TableMetadataBuilder, Type};
    use iceberg::{MetadataLocation, Namespace, Result, TableCreation};

    use super::*;
    use crate::schema::{ICEBERG_FIELD_CURRENT, ICEBERG_FIELD_ID, ICEBERG_FIELD_OPTIONAL};

    fn create_metadata(schema: Schema) -> Result<TableMetadata> {
        let table_creation = TableCreation::builder()
            .name("my_table".to_string())
            .location("my_location".to_string())
            .schema(schema)
            .build();
        let metadata = TableMetadataBuilder::from_table_creation(table_creation)?
            .build()?
            .metadata;

        Ok(metadata)
    }

    #[test]
    fn test_get_metadata_location() -> Result<()> {
        let params_valid = Some(HashMap::from([(
            METADATA_LOCATION.to_string(),
            "my_location".to_string(),
        )]));
        let params_missing_key = Some(HashMap::from([(
            "not_here".to_string(),
            "my_location".to_string(),
        )]));

        let result_valid = get_metadata_location(&params_valid)?;
        let result_missing_key = get_metadata_location(&params_missing_key);
        let result_no_params = get_metadata_location(&None);

        assert_eq!(result_valid, "my_location");
        assert!(result_missing_key.is_err());
        assert!(result_no_params.is_err());

        Ok(())
    }

    #[test]
    fn test_convert_to_glue_table() -> Result<()> {
        let table_name = "my_table".to_string();
        let location = "s3a://warehouse/hive".to_string();
        let metadata_location = MetadataLocation::new_with_table_location(location).to_string();
        let properties = HashMap::new();
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "foo", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()?;

        let metadata = create_metadata(schema)?;

        let parameters = HashMap::from([
            (ICEBERG_FIELD_ID.to_string(), "1".to_string()),
            (ICEBERG_FIELD_OPTIONAL.to_string(), "false".to_string()),
            (ICEBERG_FIELD_CURRENT.to_string(), "true".to_string()),
        ]);

        let column = Column::builder()
            .name("foo")
            .r#type("int")
            .set_parameters(Some(parameters))
            .set_comment(None)
            .build()
            .map_err(from_aws_build_error)?;

        let storage_descriptor = StorageDescriptor::builder()
            .set_columns(Some(vec![column]))
            .location(metadata.location())
            .build();

        let result =
            convert_to_glue_table(&table_name, metadata_location, &metadata, &properties, None)?;

        assert_eq!(result.name(), &table_name);
        assert_eq!(result.description(), None);
        assert_eq!(result.storage_descriptor, Some(storage_descriptor));

        Ok(())
    }

    #[test]
    fn test_get_default_table_location() -> Result<()> {
        let properties = HashMap::from([(LOCATION.to_string(), "db_location".to_string())]);

        let namespace =
            Namespace::with_properties(NamespaceIdent::new("default".into()), properties);
        let db_name = validate_namespace(namespace.name())?;
        let table_name = "my_table";

        let expected = "db_location/my_table";
        let result =
            get_default_table_location(&namespace, db_name, table_name, "warehouse_location");

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn test_get_default_table_location_warehouse() -> Result<()> {
        let namespace = Namespace::new(NamespaceIdent::new("default".into()));
        let db_name = validate_namespace(namespace.name())?;
        let table_name = "my_table";

        let expected = "warehouse_location/default.db/my_table";
        let result =
            get_default_table_location(&namespace, db_name, table_name, "warehouse_location");

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn test_convert_to_namespace() -> Result<()> {
        let db = Database::builder()
            .name("my_db")
            .location_uri("my_location")
            .description("my_description")
            .build()
            .map_err(from_aws_build_error)?;

        let properties = HashMap::from([
            (DESCRIPTION.to_string(), "my_description".to_string()),
            (LOCATION.to_string(), "my_location".to_string()),
        ]);

        let expected =
            Namespace::with_properties(NamespaceIdent::new("my_db".to_string()), properties);
        let result = convert_to_namespace(&db);

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn test_convert_to_database() -> Result<()> {
        let namespace = NamespaceIdent::new("my_database".to_string());
        let properties = HashMap::from([(LOCATION.to_string(), "my_location".to_string())]);

        let result = convert_to_database(&namespace, &properties)?;

        assert_eq!("my_database", result.name());
        assert_eq!(Some("my_location".to_string()), result.location_uri);

        Ok(())
    }

    #[test]
    fn test_validate_namespace() {
        let valid_ns = Namespace::new(NamespaceIdent::new("ns".to_string()));
        let empty_ns = Namespace::new(NamespaceIdent::new("".to_string()));
        let hierarchical_ns = Namespace::new(
            NamespaceIdent::from_vec(vec!["level1".to_string(), "level2".to_string()]).unwrap(),
        );

        let valid = validate_namespace(valid_ns.name());
        let empty = validate_namespace(empty_ns.name());
        let hierarchical = validate_namespace(hierarchical_ns.name());

        assert!(valid.is_ok());
        assert!(empty.is_err());
        assert!(hierarchical.is_err());
    }

    #[tokio::test]
    async fn test_config_with_custom_endpoint() {
        let properties = HashMap::new();
        let endpoint_url = "http://custom_url:5000";

        let sdk_config = create_sdk_config(&properties, Some(&endpoint_url.to_string())).await;

        let result = sdk_config.endpoint_url().unwrap();

        assert_eq!(result, endpoint_url);
    }

    #[tokio::test]
    async fn test_config_with_properties() {
        let properties = HashMap::from([
            (AWS_PROFILE_NAME.to_string(), "my_profile".to_string()),
            (AWS_REGION_NAME.to_string(), "us-east-1".to_string()),
            (AWS_ACCESS_KEY_ID.to_string(), "my-access-id".to_string()),
            (
                AWS_SECRET_ACCESS_KEY.to_string(),
                "my-secret-key".to_string(),
            ),
            (AWS_SESSION_TOKEN.to_string(), "my-token".to_string()),
        ]);

        let sdk_config = create_sdk_config(&properties, None).await;

        let region = sdk_config.region().unwrap().as_ref();
        let credentials = sdk_config
            .credentials_provider()
            .unwrap()
            .provide_credentials()
            .await
            .unwrap();

        assert_eq!("us-east-1", region);
        assert_eq!("my-access-id", credentials.access_key_id());
        assert_eq!("my-secret-key", credentials.secret_access_key());
        assert_eq!("my-token", credentials.session_token().unwrap());
    }
}
