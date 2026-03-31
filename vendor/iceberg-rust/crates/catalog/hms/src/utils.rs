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

use chrono::Utc;
use hive_metastore::{Database, PrincipalType, SerDeInfo, StorageDescriptor};
use iceberg::spec::Schema;
use iceberg::{Error, ErrorKind, Namespace, NamespaceIdent, Result};
use pilota::{AHashMap, FastStr};

use crate::schema::HiveSchemaBuilder;

/// hive.metastore.database.owner setting
const HMS_DB_OWNER: &str = "hive.metastore.database.owner";
/// hive.metastore.database.owner default setting
const HMS_DEFAULT_DB_OWNER: &str = "user.name";
/// hive.metastore.database.owner-type setting
const HMS_DB_OWNER_TYPE: &str = "hive.metastore.database.owner-type";
/// hive metatore `owner` property
const OWNER: &str = "owner";
/// hive metatore `description` property
const COMMENT: &str = "comment";
/// hive metatore `location` property
const LOCATION: &str = "location";
/// hive metatore `metadata_location` property
const METADATA_LOCATION: &str = "metadata_location";
/// hive metatore `external` property
const EXTERNAL: &str = "EXTERNAL";
/// hive metatore `external_table` property
const EXTERNAL_TABLE: &str = "EXTERNAL_TABLE";
/// hive metatore `table_type` property
const TABLE_TYPE: &str = "table_type";
/// hive metatore `SerDeInfo` serialization_lib parameter
const SERIALIZATION_LIB: &str = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
/// hive metatore input format
const INPUT_FORMAT: &str = "org.apache.hadoop.mapred.FileInputFormat";
/// hive metatore output format
const OUTPUT_FORMAT: &str = "org.apache.hadoop.mapred.FileOutputFormat";

/// Returns a `Namespace` by extracting database name and properties
/// from `hive_metastore::hms::Database`
pub(crate) fn convert_to_namespace(database: &Database) -> Result<Namespace> {
    let mut properties = HashMap::new();

    let name = database
        .name
        .as_ref()
        .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "Database name must be specified"))?
        .to_string();

    if let Some(description) = &database.description {
        properties.insert(COMMENT.to_string(), description.to_string());
    };

    if let Some(location) = &database.location_uri {
        properties.insert(LOCATION.to_string(), location.to_string());
    };

    if let Some(owner) = &database.owner_name {
        properties.insert(HMS_DB_OWNER.to_string(), owner.to_string());
    };

    if let Some(owner_type) = database.owner_type {
        let value = if owner_type == PrincipalType::USER {
            "User"
        } else if owner_type == PrincipalType::GROUP {
            "Group"
        } else if owner_type == PrincipalType::ROLE {
            "Role"
        } else {
            unreachable!("Invalid owner type")
        };

        properties.insert(HMS_DB_OWNER_TYPE.to_string(), value.to_string());
    };

    if let Some(params) = &database.parameters {
        params.iter().for_each(|(k, v)| {
            properties.insert(k.clone().into(), v.clone().into());
        });
    };

    Ok(Namespace::with_properties(
        NamespaceIdent::new(name),
        properties,
    ))
}

/// Converts name and properties into `hive_metastore::hms::Database`
/// after validating the `namespace` and `owner-settings`.
pub(crate) fn convert_to_database(
    namespace: &NamespaceIdent,
    properties: &HashMap<String, String>,
) -> Result<Database> {
    let name = validate_namespace(namespace)?;
    validate_owner_settings(properties)?;

    let mut db = Database::default();
    let mut parameters = AHashMap::new();

    db.name = Some(name.into());

    for (k, v) in properties {
        match k.as_str() {
            COMMENT => db.description = Some(v.clone().into()),
            LOCATION => db.location_uri = Some(format_location_uri(v.clone()).into()),
            HMS_DB_OWNER => db.owner_name = Some(v.clone().into()),
            HMS_DB_OWNER_TYPE => {
                let owner_type = match v.to_lowercase().as_str() {
                    "user" => PrincipalType::USER,
                    "group" => PrincipalType::GROUP,
                    "role" => PrincipalType::ROLE,
                    _ => {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!("Invalid value for setting 'owner_type': {v}"),
                        ));
                    }
                };
                db.owner_type = Some(owner_type);
            }
            _ => {
                parameters.insert(
                    FastStr::from_string(k.clone()),
                    FastStr::from_string(v.clone()),
                );
            }
        }
    }

    db.parameters = Some(parameters);

    // Set default owner, if none provided
    // https://github.com/apache/iceberg/blob/main/hive-metastore/src/main/java/org/apache/iceberg/hive/HiveHadoopUtil.java#L44
    if db.owner_name.is_none() {
        db.owner_name = Some(HMS_DEFAULT_DB_OWNER.into());
        db.owner_type = Some(PrincipalType::USER);
    }

    Ok(db)
}

pub(crate) fn convert_to_hive_table(
    db_name: String,
    schema: &Schema,
    table_name: String,
    location: String,
    metadata_location: String,
    properties: &HashMap<String, String>,
) -> Result<hive_metastore::Table> {
    let serde_info = SerDeInfo {
        serialization_lib: Some(SERIALIZATION_LIB.into()),
        ..Default::default()
    };

    let hive_schema = HiveSchemaBuilder::from_iceberg(schema)?.build();

    let storage_descriptor = StorageDescriptor {
        location: Some(location.into()),
        cols: Some(hive_schema),
        input_format: Some(INPUT_FORMAT.into()),
        output_format: Some(OUTPUT_FORMAT.into()),
        serde_info: Some(serde_info),
        ..Default::default()
    };

    let parameters = AHashMap::from([
        (FastStr::from(EXTERNAL), FastStr::from("TRUE")),
        (FastStr::from(TABLE_TYPE), FastStr::from("ICEBERG")),
        (
            FastStr::from(METADATA_LOCATION),
            FastStr::from(metadata_location),
        ),
    ]);

    let current_time_ms = get_current_time()?;
    let owner = properties
        .get(OWNER)
        .map_or(HMS_DEFAULT_DB_OWNER.to_string(), |v| v.into());

    Ok(hive_metastore::Table {
        table_name: Some(table_name.into()),
        db_name: Some(db_name.into()),
        table_type: Some(EXTERNAL_TABLE.into()),
        owner: Some(owner.into()),
        create_time: Some(current_time_ms),
        last_access_time: Some(current_time_ms),
        sd: Some(storage_descriptor),
        parameters: Some(parameters),
        ..Default::default()
    })
}

/// Checks if provided `NamespaceIdent` is valid.
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
    table_name: impl AsRef<str>,
    warehouse: impl AsRef<str>,
) -> String {
    let properties = namespace.properties();

    let location = match properties.get(LOCATION) {
        Some(location) => location,
        None => warehouse.as_ref(),
    };

    format!("{}/{}", location, table_name.as_ref())
}

/// Get metadata location from `HiveTable` parameters
pub(crate) fn get_metadata_location(
    parameters: &Option<AHashMap<FastStr, FastStr>>,
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

/// Formats location_uri by e.g. removing trailing slashes.
fn format_location_uri(location: String) -> String {
    let mut location = location;

    if !location.starts_with('/') {
        location = format!("/{location}");
    }

    if location.ends_with('/') && location.len() > 1 {
        location.pop();
    }

    location
}

/// Checks if `owner-settings` are valid.
/// If `owner_type` is set, then `owner` must also be set.
fn validate_owner_settings(properties: &HashMap<String, String>) -> Result<()> {
    let owner_is_set = properties.get(HMS_DB_OWNER).is_some();
    let owner_type_is_set = properties.get(HMS_DB_OWNER_TYPE).is_some();

    if owner_type_is_set && !owner_is_set {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Setting '{HMS_DB_OWNER_TYPE}' without setting '{HMS_DB_OWNER}' is not allowed"
            ),
        ));
    }

    Ok(())
}

fn get_current_time() -> Result<i32> {
    let now = Utc::now();
    now.timestamp().try_into().map_err(|_| {
        Error::new(
            ErrorKind::Unexpected,
            "Current time is out of range for i32",
        )
    })
}

#[cfg(test)]
mod tests {
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    use iceberg::{MetadataLocation, Namespace, NamespaceIdent};

    use super::*;

    #[test]
    fn test_get_metadata_location() -> Result<()> {
        let params_valid = Some(AHashMap::from([(
            FastStr::new(METADATA_LOCATION),
            FastStr::new("my_location"),
        )]));
        let params_missing_key = Some(AHashMap::from([(
            FastStr::new("not_here"),
            FastStr::new("my_location"),
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
    fn test_convert_to_hive_table() -> Result<()> {
        let db_name = "my_db".to_string();
        let table_name = "my_table".to_string();
        let location = "s3a://warehouse/hms".to_string();
        let metadata_location =
            MetadataLocation::new_with_table_location(location.clone()).to_string();
        let properties = HashMap::new();
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "foo", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()?;

        let result = convert_to_hive_table(
            db_name.clone(),
            &schema,
            table_name.clone(),
            location.clone(),
            metadata_location,
            &properties,
        )?;

        let serde_info = SerDeInfo {
            serialization_lib: Some(SERIALIZATION_LIB.into()),
            ..Default::default()
        };

        let hive_schema = HiveSchemaBuilder::from_iceberg(&schema)?.build();

        let sd = StorageDescriptor {
            location: Some(location.into()),
            cols: Some(hive_schema),
            input_format: Some(INPUT_FORMAT.into()),
            output_format: Some(OUTPUT_FORMAT.into()),
            serde_info: Some(serde_info),
            ..Default::default()
        };

        assert_eq!(result.db_name, Some(db_name.into()));
        assert_eq!(result.table_name, Some(table_name.into()));
        assert_eq!(result.table_type, Some(EXTERNAL_TABLE.into()));
        assert_eq!(result.owner, Some(HMS_DEFAULT_DB_OWNER.into()));
        assert_eq!(result.sd, Some(sd));

        Ok(())
    }

    #[test]
    fn test_get_default_table_location() -> Result<()> {
        let properties = HashMap::from([(LOCATION.to_string(), "db_location".to_string())]);

        let namespace =
            Namespace::with_properties(NamespaceIdent::new("default".into()), properties);
        let table_name = "my_table";

        let expected = "db_location/my_table";
        let result = get_default_table_location(&namespace, table_name, "warehouse_location");

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn test_get_default_table_location_warehouse() -> Result<()> {
        let namespace = Namespace::new(NamespaceIdent::new("default".into()));
        let table_name = "my_table";

        let expected = "warehouse_location/my_table";
        let result = get_default_table_location(&namespace, table_name, "warehouse_location");

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn test_convert_to_namespace() -> Result<()> {
        let properties = HashMap::from([
            (COMMENT.to_string(), "my_description".to_string()),
            (LOCATION.to_string(), "/my_location".to_string()),
            (HMS_DB_OWNER.to_string(), "apache".to_string()),
            (HMS_DB_OWNER_TYPE.to_string(), "User".to_string()),
            ("key1".to_string(), "value1".to_string()),
        ]);

        let ident = NamespaceIdent::new("my_namespace".into());
        let db = convert_to_database(&ident, &properties)?;

        let expected_ns = Namespace::with_properties(ident, properties);
        let result_ns = convert_to_namespace(&db)?;

        assert_eq!(expected_ns, result_ns);

        Ok(())
    }

    #[test]
    fn test_validate_owner_settings() {
        let valid = HashMap::from([
            (HMS_DB_OWNER.to_string(), "apache".to_string()),
            (HMS_DB_OWNER_TYPE.to_string(), "user".to_string()),
        ]);
        let invalid = HashMap::from([(HMS_DB_OWNER_TYPE.to_string(), "user".to_string())]);

        assert!(validate_owner_settings(&valid).is_ok());
        assert!(validate_owner_settings(&invalid).is_err());
    }

    #[test]
    fn test_convert_to_database() -> Result<()> {
        let ns = NamespaceIdent::new("my_namespace".into());
        let properties = HashMap::from([
            (COMMENT.to_string(), "my_description".to_string()),
            (LOCATION.to_string(), "my_location".to_string()),
            (HMS_DB_OWNER.to_string(), "apache".to_string()),
            (HMS_DB_OWNER_TYPE.to_string(), "user".to_string()),
            ("key1".to_string(), "value1".to_string()),
        ]);

        let db = convert_to_database(&ns, &properties)?;

        assert_eq!(db.name, Some(FastStr::from("my_namespace")));
        assert_eq!(db.description, Some(FastStr::from("my_description")));
        assert_eq!(db.owner_name, Some(FastStr::from("apache")));
        assert_eq!(db.owner_type, Some(PrincipalType::USER));

        if let Some(params) = db.parameters {
            assert_eq!(params.get("key1"), Some(&FastStr::from("value1")));
        }

        Ok(())
    }

    #[test]
    fn test_convert_to_database_with_default_user() -> Result<()> {
        let ns = NamespaceIdent::new("my_namespace".into());
        let properties = HashMap::new();

        let db = convert_to_database(&ns, &properties)?;

        assert_eq!(db.name, Some(FastStr::from("my_namespace")));
        assert_eq!(db.owner_name, Some(FastStr::from(HMS_DEFAULT_DB_OWNER)));
        assert_eq!(db.owner_type, Some(PrincipalType::USER));

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

    #[test]
    fn test_format_location_uri() {
        let inputs = vec!["iceberg", "is/", "/nice/", "really/nice/", "/"];
        let outputs = vec!["/iceberg", "/is", "/nice", "/really/nice", "/"];

        inputs.into_iter().zip(outputs).for_each(|(inp, out)| {
            let location = format_location_uri(inp.to_string());
            assert_eq!(location, out);
        })
    }
}
