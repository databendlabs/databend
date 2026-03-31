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

//! This module contains the location generator and file name generator for generating path of data file.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use crate::Result;
use crate::spec::{DataFileFormat, PartitionKey, TableMetadata};

/// `LocationGenerator` used to generate the location of data file.
pub trait LocationGenerator: Clone + Send + Sync + 'static {
    /// Generate an absolute path for the given file name that includes the partition path.
    ///
    /// # Arguments
    ///
    /// * `partition_key` - The partition key of the file. If None, generate a non-partitioned path.
    /// * `file_name` - The name of the file
    ///
    /// # Returns
    ///
    /// An absolute path that includes the partition path, e.g.,
    /// "/table/data/id=1/name=alice/part-00000.parquet"
    /// or non-partitioned path:
    /// "/table/data/part-00000.parquet"
    fn generate_location(&self, partition_key: Option<&PartitionKey>, file_name: &str) -> String;
}

const WRITE_DATA_LOCATION: &str = "write.data.path";
const WRITE_FOLDER_STORAGE_LOCATION: &str = "write.folder-storage.path";
const DEFAULT_DATA_DIR: &str = "/data";

#[derive(Clone, Debug)]
/// `DefaultLocationGenerator` used to generate the data dir location of data file.
/// The location is generated based on the table location and the data location in table properties.
pub struct DefaultLocationGenerator {
    data_location: String,
}

impl DefaultLocationGenerator {
    /// Create a new `DefaultLocationGenerator`.
    pub fn new(table_metadata: TableMetadata) -> Result<Self> {
        let table_location = table_metadata.location();
        let prop = table_metadata.properties();
        let configured_data_location = prop
            .get(WRITE_DATA_LOCATION)
            .or(prop.get(WRITE_FOLDER_STORAGE_LOCATION));
        let data_location = if let Some(data_location) = configured_data_location {
            data_location.clone()
        } else {
            format!("{table_location}{DEFAULT_DATA_DIR}")
        };
        Ok(Self { data_location })
    }

    /// Create a new `DefaultLocationGenerator` with a specified data location.
    ///
    /// # Arguments
    ///
    /// * `data_location` - The data location to use for generating file locations.
    pub fn with_data_location(data_location: String) -> Self {
        Self { data_location }
    }
}

impl LocationGenerator for DefaultLocationGenerator {
    fn generate_location(&self, partition_key: Option<&PartitionKey>, file_name: &str) -> String {
        if PartitionKey::is_effectively_none(partition_key) {
            format!("{}/{}", self.data_location, file_name)
        } else {
            format!(
                "{}/{}/{}",
                self.data_location,
                partition_key.unwrap().to_path(),
                file_name
            )
        }
    }
}

/// `FileNameGeneratorTrait` used to generate file name for data file. The file name can be passed to `LocationGenerator` to generate the location of the file.
pub trait FileNameGenerator: Clone + Send + Sync + 'static {
    /// Generate a file name.
    fn generate_file_name(&self) -> String;
}

/// `DefaultFileNameGenerator` used to generate file name for data file. The file name can be
/// passed to `LocationGenerator` to generate the location of the file.
/// The file name format is "{prefix}-{file_count}[-{suffix}].{file_format}".
#[derive(Clone, Debug)]
pub struct DefaultFileNameGenerator {
    prefix: String,
    suffix: String,
    format: String,
    file_count: Arc<AtomicU64>,
}

impl DefaultFileNameGenerator {
    /// Create a new `FileNameGenerator`.
    pub fn new(prefix: String, suffix: Option<String>, format: DataFileFormat) -> Self {
        let suffix = if let Some(suffix) = suffix {
            format!("-{suffix}")
        } else {
            "".to_string()
        };

        Self {
            prefix,
            suffix,
            format: format.to_string(),
            file_count: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl FileNameGenerator for DefaultFileNameGenerator {
    fn generate_file_name(&self) -> String {
        let file_id = self
            .file_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!(
            "{}-{:05}{}.{}",
            self.prefix, file_id, self.suffix, self.format
        )
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use uuid::Uuid;

    use super::LocationGenerator;
    use crate::spec::{
        FormatVersion, Literal, NestedField, PartitionKey, PartitionSpec, PrimitiveType, Schema,
        Struct, StructType, TableMetadata, Transform, Type,
    };
    use crate::writer::file_writer::location_generator::{
        DefaultLocationGenerator, FileNameGenerator, WRITE_DATA_LOCATION,
        WRITE_FOLDER_STORAGE_LOCATION,
    };

    #[test]
    fn test_default_location_generate() {
        let mut table_metadata = TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: Uuid::parse_str("fb072c92-a02b-11e9-ae9c-1bb7bc9eca94").unwrap(),
            location: "s3://data.db/table".to_string(),
            last_updated_ms: 1515100955770,
            last_column_id: 1,
            schemas: HashMap::new(),
            current_schema_id: 1,
            partition_specs: HashMap::new(),
            default_spec: PartitionSpec::unpartition_spec().into(),
            default_partition_type: StructType::new(vec![]),
            last_partition_id: 1000,
            default_sort_order_id: 0,
            sort_orders: HashMap::from_iter(vec![]),
            snapshots: HashMap::default(),
            current_snapshot_id: None,
            last_sequence_number: 1,
            properties: HashMap::new(),
            snapshot_log: Vec::new(),
            metadata_log: vec![],
            refs: HashMap::new(),
            statistics: HashMap::new(),
            partition_statistics: HashMap::new(),
            encryption_keys: HashMap::new(),
            next_row_id: 0,
        };

        let file_name_generator = super::DefaultFileNameGenerator::new(
            "part".to_string(),
            Some("test".to_string()),
            crate::spec::DataFileFormat::Parquet,
        );

        // test default data location
        let location_generator =
            super::DefaultLocationGenerator::new(table_metadata.clone()).unwrap();
        let location =
            location_generator.generate_location(None, &file_name_generator.generate_file_name());
        assert_eq!(location, "s3://data.db/table/data/part-00000-test.parquet");

        // test custom data location
        table_metadata.properties.insert(
            WRITE_FOLDER_STORAGE_LOCATION.to_string(),
            "s3://data.db/table/data_1".to_string(),
        );
        let location_generator =
            super::DefaultLocationGenerator::new(table_metadata.clone()).unwrap();
        let location =
            location_generator.generate_location(None, &file_name_generator.generate_file_name());
        assert_eq!(
            location,
            "s3://data.db/table/data_1/part-00001-test.parquet"
        );

        table_metadata.properties.insert(
            WRITE_DATA_LOCATION.to_string(),
            "s3://data.db/table/data_2".to_string(),
        );
        let location_generator =
            super::DefaultLocationGenerator::new(table_metadata.clone()).unwrap();
        let location =
            location_generator.generate_location(None, &file_name_generator.generate_file_name());
        assert_eq!(
            location,
            "s3://data.db/table/data_2/part-00002-test.parquet"
        );

        table_metadata.properties.insert(
            WRITE_DATA_LOCATION.to_string(),
            // invalid table location
            "s3://data.db/data_3".to_string(),
        );
        let location_generator =
            super::DefaultLocationGenerator::new(table_metadata.clone()).unwrap();
        let location =
            location_generator.generate_location(None, &file_name_generator.generate_file_name());
        assert_eq!(location, "s3://data.db/data_3/part-00003-test.parquet");
    }

    #[test]
    fn test_location_generate_with_partition() {
        // Create a schema with two fields: id (int) and name (string)
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );

        // Create a partition spec with both fields
        let partition_spec = PartitionSpec::builder(schema.clone())
            .add_partition_field("id", "id", Transform::Identity)
            .unwrap()
            .add_partition_field("name", "name", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        // Create partition data with values
        let partition_data =
            Struct::from_iter([Some(Literal::int(42)), Some(Literal::string("alice"))]);

        // Create a partition key
        let partition_key = PartitionKey::new(partition_spec, schema, partition_data);

        let location_gen = DefaultLocationGenerator::with_data_location("/base/path".to_string());
        let file_name = "data-00000.parquet";
        let location = location_gen.generate_location(Some(&partition_key), file_name);
        assert_eq!(location, "/base/path/id=42/name=alice/data-00000.parquet");

        // Create a table metadata for DefaultLocationGenerator
        let table_metadata = TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: Uuid::parse_str("fb072c92-a02b-11e9-ae9c-1bb7bc9eca94").unwrap(),
            location: "s3://data.db/table".to_string(),
            last_updated_ms: 1515100955770,
            last_column_id: 2,
            schemas: HashMap::new(),
            current_schema_id: 1,
            partition_specs: HashMap::new(),
            default_spec: PartitionSpec::unpartition_spec().into(),
            default_partition_type: StructType::new(vec![]),
            last_partition_id: 1000,
            default_sort_order_id: 0,
            sort_orders: HashMap::from_iter(vec![]),
            snapshots: HashMap::default(),
            current_snapshot_id: None,
            last_sequence_number: 1,
            properties: HashMap::new(),
            snapshot_log: Vec::new(),
            metadata_log: vec![],
            refs: HashMap::new(),
            statistics: HashMap::new(),
            partition_statistics: HashMap::new(),
            encryption_keys: HashMap::new(),
            next_row_id: 0,
        };

        // Test with DefaultLocationGenerator
        let default_location_gen = super::DefaultLocationGenerator::new(table_metadata).unwrap();
        let location = default_location_gen.generate_location(Some(&partition_key), file_name);
        assert_eq!(
            location,
            "s3://data.db/table/data/id=42/name=alice/data-00000.parquet"
        );
    }
}
