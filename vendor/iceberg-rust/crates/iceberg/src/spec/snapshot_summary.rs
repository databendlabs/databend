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

use itertools::Itertools;

use super::{DataContentType, DataFile, PartitionSpecRef};
use crate::spec::{ManifestContentType, ManifestFile, Operation, SchemaRef, Summary};
use crate::{Error, ErrorKind, Result};

const ADDED_DATA_FILES: &str = "added-data-files";
const ADDED_DELETE_FILES: &str = "added-delete-files";
const ADDED_EQUALITY_DELETES: &str = "added-equality-deletes";
const ADDED_FILE_SIZE: &str = "added-files-size";
const ADDED_POSITION_DELETES: &str = "added-position-deletes";
const ADDED_POSITION_DELETE_FILES: &str = "added-position-delete-files";
const ADDED_RECORDS: &str = "added-records";
const DELETED_DATA_FILES: &str = "deleted-data-files";
const DELETED_RECORDS: &str = "deleted-records";
const ADDED_EQUALITY_DELETE_FILES: &str = "added-equality-delete-files";
const REMOVED_DELETE_FILES: &str = "removed-delete-files";
const REMOVED_EQUALITY_DELETES: &str = "removed-equality-deletes";
const REMOVED_EQUALITY_DELETE_FILES: &str = "removed-equality-delete-files";
const REMOVED_FILE_SIZE: &str = "removed-files-size";
const REMOVED_POSITION_DELETES: &str = "removed-position-deletes";
const REMOVED_POSITION_DELETE_FILES: &str = "removed-position-delete-files";
const TOTAL_EQUALITY_DELETES: &str = "total-equality-deletes";
const TOTAL_POSITION_DELETES: &str = "total-position-deletes";
const TOTAL_DATA_FILES: &str = "total-data-files";
const TOTAL_DELETE_FILES: &str = "total-delete-files";
const TOTAL_RECORDS: &str = "total-records";
const TOTAL_FILE_SIZE: &str = "total-files-size";
const CHANGED_PARTITION_COUNT_PROP: &str = "changed-partition-count";
const CHANGED_PARTITION_PREFIX: &str = "partitions.";

/// `SnapshotSummaryCollector` collects and aggregates snapshot update metrics.
/// It gathers metrics about added or removed data files and manifests, and tracks
/// partition-specific updates.
#[derive(Default)]
pub struct SnapshotSummaryCollector {
    metrics: UpdateMetrics,
    partition_metrics: HashMap<String, UpdateMetrics>,
    max_changed_partitions_for_summaries: u64,
    properties: HashMap<String, String>,
    trust_partition_metrics: bool,
}

impl SnapshotSummaryCollector {
    /// Set properties for snapshot summary
    pub fn set(&mut self, key: &str, value: &str) {
        self.properties.insert(key.to_string(), value.to_string());
    }

    /// Sets the limit for including partition summaries. Summaries are not
    /// included if the number of partitions is exceeded.
    pub fn set_partition_summary_limit(&mut self, limit: u64) {
        self.max_changed_partitions_for_summaries = limit;
    }

    /// Adds a data file to the summary collector
    pub fn add_file(
        &mut self,
        data_file: &DataFile,
        schema: SchemaRef,
        partition_spec: PartitionSpecRef,
    ) {
        self.metrics.add_file(data_file);
        if !data_file.partition.fields().is_empty() {
            self.update_partition_metrics(schema, partition_spec, data_file, true);
        }
    }

    /// Removes a data file from the summary collector
    pub fn remove_file(
        &mut self,
        data_file: &DataFile,
        schema: SchemaRef,
        partition_spec: PartitionSpecRef,
    ) {
        self.metrics.remove_file(data_file);
        if !data_file.partition.fields().is_empty() {
            self.update_partition_metrics(schema, partition_spec, data_file, false);
        }
    }

    /// Adds a manifest to the summary collector
    pub fn add_manifest(&mut self, manifest: &ManifestFile) {
        self.trust_partition_metrics = false;
        self.partition_metrics.clear();
        self.metrics.add_manifest(manifest);
    }

    /// Updates partition-specific metrics for a data file.
    pub fn update_partition_metrics(
        &mut self,
        schema: SchemaRef,
        partition_spec: PartitionSpecRef,
        data_file: &DataFile,
        is_add_file: bool,
    ) {
        let partition_path = partition_spec.partition_to_path(&data_file.partition, schema);
        let metrics = self.partition_metrics.entry(partition_path).or_default();

        if is_add_file {
            metrics.add_file(data_file);
        } else {
            metrics.remove_file(data_file);
        }
    }

    /// Merges another `SnapshotSummaryCollector` into the current one
    pub fn merge(&mut self, summary: SnapshotSummaryCollector) {
        self.metrics.merge(&summary.metrics);
        self.properties.extend(summary.properties);

        if self.trust_partition_metrics && summary.trust_partition_metrics {
            for (partition, partition_metric) in summary.partition_metrics.iter() {
                self.partition_metrics
                    .entry(partition.to_string())
                    .or_default()
                    .merge(partition_metric);
            }
        } else {
            self.partition_metrics.clear();
            self.trust_partition_metrics = false;
        }
    }

    /// Builds final map of summaries
    pub fn build(&self) -> HashMap<String, String> {
        let mut properties = self.metrics.to_map();
        let changed_partitions_count = self.partition_metrics.len() as u64;
        set_if_positive(
            &mut properties,
            changed_partitions_count,
            CHANGED_PARTITION_COUNT_PROP,
        );

        if changed_partitions_count <= self.max_changed_partitions_for_summaries {
            for (partition_path, update_metrics_partition) in &self.partition_metrics {
                let property_key = format!("{CHANGED_PARTITION_PREFIX}{partition_path}");
                let partition_summary = update_metrics_partition
                    .to_map()
                    .into_iter()
                    .map(|(property, value)| format!("{property}={value}"))
                    .join(",");

                if !partition_summary.is_empty() {
                    properties.insert(property_key, partition_summary);
                }
            }
        }
        properties
    }
}

#[derive(Debug, Default)]
struct UpdateMetrics {
    added_file_size: u64,
    removed_file_size: u64,
    added_data_files: u32,
    removed_data_files: u32,
    added_eq_delete_files: u64,
    removed_eq_delete_files: u64,
    added_pos_delete_files: u64,
    removed_pos_delete_files: u64,
    added_delete_files: u32,
    removed_delete_files: u32,
    added_records: u64,
    deleted_records: u64,
    added_pos_deletes: u64,
    removed_pos_deletes: u64,
    added_eq_deletes: u64,
    removed_eq_deletes: u64,
}

impl UpdateMetrics {
    fn add_file(&mut self, data_file: &DataFile) {
        self.added_file_size += data_file.file_size_in_bytes;
        match data_file.content_type() {
            DataContentType::Data => {
                self.added_data_files += 1;
                self.added_records += data_file.record_count;
            }
            DataContentType::PositionDeletes => {
                self.added_delete_files += 1;
                self.added_pos_delete_files += 1;
                self.added_pos_deletes += data_file.record_count;
            }
            DataContentType::EqualityDeletes => {
                self.added_delete_files += 1;
                self.added_eq_delete_files += 1;
                self.added_eq_deletes += data_file.record_count;
            }
        }
    }

    fn remove_file(&mut self, data_file: &DataFile) {
        self.removed_file_size += data_file.file_size_in_bytes;
        match data_file.content_type() {
            DataContentType::Data => {
                self.removed_data_files += 1;
                self.deleted_records += data_file.record_count;
            }
            DataContentType::PositionDeletes => {
                self.removed_delete_files += 1;
                self.removed_pos_delete_files += 1;
                self.removed_pos_deletes += data_file.record_count;
            }
            DataContentType::EqualityDeletes => {
                self.removed_delete_files += 1;
                self.removed_eq_delete_files += 1;
                self.removed_eq_deletes += data_file.record_count;
            }
        }
    }

    fn add_manifest(&mut self, manifest: &ManifestFile) {
        match manifest.content {
            ManifestContentType::Data => {
                self.added_data_files += manifest.added_files_count.unwrap_or(0);
                self.added_records += manifest.added_rows_count.unwrap_or(0);
                self.removed_data_files += manifest.deleted_files_count.unwrap_or(0);
                self.deleted_records += manifest.deleted_rows_count.unwrap_or(0);
            }
            ManifestContentType::Deletes => {
                self.added_delete_files += manifest.added_files_count.unwrap_or(0);
                self.removed_delete_files += manifest.deleted_files_count.unwrap_or(0);
            }
        }
    }

    fn to_map(&self) -> HashMap<String, String> {
        let mut properties = HashMap::new();
        set_if_positive(&mut properties, self.added_file_size, ADDED_FILE_SIZE);
        set_if_positive(&mut properties, self.removed_file_size, REMOVED_FILE_SIZE);
        set_if_positive(&mut properties, self.added_data_files, ADDED_DATA_FILES);
        set_if_positive(&mut properties, self.removed_data_files, DELETED_DATA_FILES);
        set_if_positive(
            &mut properties,
            self.added_eq_delete_files,
            ADDED_EQUALITY_DELETE_FILES,
        );
        set_if_positive(
            &mut properties,
            self.removed_eq_delete_files,
            REMOVED_EQUALITY_DELETE_FILES,
        );
        set_if_positive(
            &mut properties,
            self.added_pos_delete_files,
            ADDED_POSITION_DELETE_FILES,
        );
        set_if_positive(
            &mut properties,
            self.removed_pos_delete_files,
            REMOVED_POSITION_DELETE_FILES,
        );
        set_if_positive(&mut properties, self.added_delete_files, ADDED_DELETE_FILES);
        set_if_positive(
            &mut properties,
            self.removed_delete_files,
            REMOVED_DELETE_FILES,
        );
        set_if_positive(&mut properties, self.added_records, ADDED_RECORDS);
        set_if_positive(&mut properties, self.deleted_records, DELETED_RECORDS);
        set_if_positive(
            &mut properties,
            self.added_pos_deletes,
            ADDED_POSITION_DELETES,
        );
        set_if_positive(
            &mut properties,
            self.removed_pos_deletes,
            REMOVED_POSITION_DELETES,
        );
        set_if_positive(
            &mut properties,
            self.added_eq_deletes,
            ADDED_EQUALITY_DELETES,
        );
        set_if_positive(
            &mut properties,
            self.removed_eq_deletes,
            REMOVED_EQUALITY_DELETES,
        );
        properties
    }

    fn merge(&mut self, other: &UpdateMetrics) {
        self.added_file_size += other.added_file_size;
        self.removed_file_size += other.removed_file_size;
        self.added_data_files += other.added_data_files;
        self.removed_data_files += other.removed_data_files;
        self.added_eq_delete_files += other.added_eq_delete_files;
        self.removed_eq_delete_files += other.removed_eq_delete_files;
        self.added_pos_delete_files += other.added_pos_delete_files;
        self.removed_pos_delete_files += other.removed_pos_delete_files;
        self.added_delete_files += other.added_delete_files;
        self.removed_delete_files += other.removed_delete_files;
        self.added_records += other.added_records;
        self.deleted_records += other.deleted_records;
        self.added_pos_deletes += other.added_pos_deletes;
        self.removed_pos_deletes += other.removed_pos_deletes;
        self.added_eq_deletes += other.added_eq_deletes;
        self.removed_eq_deletes += other.removed_eq_deletes;
    }
}

fn set_if_positive<T>(properties: &mut HashMap<String, String>, value: T, property_name: &str)
where T: PartialOrd + Default + ToString {
    if value > T::default() {
        properties.insert(property_name.to_string(), value.to_string());
    }
}

#[allow(dead_code)]
pub(crate) fn update_snapshot_summaries(
    summary: Summary,
    previous_summary: Option<&Summary>,
    truncate_full_table: bool,
) -> Result<Summary> {
    // Validate that the operation is supported
    if summary.operation != Operation::Append
        && summary.operation != Operation::Overwrite
        && summary.operation != Operation::Delete
    {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "Operation is not supported.",
        ));
    }

    let mut summary = match previous_summary {
        Some(prev_summary) if truncate_full_table && summary.operation == Operation::Overwrite => {
            truncate_table_summary(summary, prev_summary)
                .map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "Failed to truncate table summary.")
                        .with_source(err)
                })
                .unwrap()
        }
        _ => summary,
    };

    update_totals(
        &mut summary,
        previous_summary,
        TOTAL_DATA_FILES,
        ADDED_DATA_FILES,
        DELETED_DATA_FILES,
    );

    update_totals(
        &mut summary,
        previous_summary,
        TOTAL_DELETE_FILES,
        ADDED_DELETE_FILES,
        REMOVED_DELETE_FILES,
    );

    update_totals(
        &mut summary,
        previous_summary,
        TOTAL_RECORDS,
        ADDED_RECORDS,
        DELETED_RECORDS,
    );

    update_totals(
        &mut summary,
        previous_summary,
        TOTAL_FILE_SIZE,
        ADDED_FILE_SIZE,
        REMOVED_FILE_SIZE,
    );

    update_totals(
        &mut summary,
        previous_summary,
        TOTAL_POSITION_DELETES,
        ADDED_POSITION_DELETES,
        REMOVED_POSITION_DELETES,
    );

    update_totals(
        &mut summary,
        previous_summary,
        TOTAL_EQUALITY_DELETES,
        ADDED_EQUALITY_DELETES,
        REMOVED_EQUALITY_DELETES,
    );
    Ok(summary)
}

#[allow(dead_code)]
fn get_prop(previous_summary: &Summary, prop: &str) -> Result<i32> {
    let value_str = previous_summary
        .additional_properties
        .get(prop)
        .map(String::as_str)
        .unwrap_or("0");
    value_str.parse::<i32>().map_err(|err| {
        Error::new(
            ErrorKind::Unexpected,
            "Failed to parse value from previous summary property.",
        )
        .with_source(err)
    })
}

#[allow(dead_code)]
fn truncate_table_summary(mut summary: Summary, previous_summary: &Summary) -> Result<Summary> {
    for prop in [
        TOTAL_DATA_FILES,
        TOTAL_DELETE_FILES,
        TOTAL_RECORDS,
        TOTAL_FILE_SIZE,
        TOTAL_POSITION_DELETES,
        TOTAL_EQUALITY_DELETES,
    ] {
        summary
            .additional_properties
            .insert(prop.to_string(), "0".to_string());
    }

    let value = get_prop(previous_summary, TOTAL_DATA_FILES)?;
    if value != 0 {
        summary
            .additional_properties
            .insert(DELETED_DATA_FILES.to_string(), value.to_string());
    }
    let value = get_prop(previous_summary, TOTAL_DELETE_FILES)?;
    if value != 0 {
        summary
            .additional_properties
            .insert(REMOVED_DELETE_FILES.to_string(), value.to_string());
    }
    let value = get_prop(previous_summary, TOTAL_RECORDS)?;
    if value != 0 {
        summary
            .additional_properties
            .insert(DELETED_RECORDS.to_string(), value.to_string());
    }
    let value = get_prop(previous_summary, TOTAL_FILE_SIZE)?;
    if value != 0 {
        summary
            .additional_properties
            .insert(REMOVED_FILE_SIZE.to_string(), value.to_string());
    }

    let value = get_prop(previous_summary, TOTAL_POSITION_DELETES)?;
    if value != 0 {
        summary
            .additional_properties
            .insert(REMOVED_POSITION_DELETES.to_string(), value.to_string());
    }

    let value = get_prop(previous_summary, TOTAL_EQUALITY_DELETES)?;
    if value != 0 {
        summary
            .additional_properties
            .insert(REMOVED_EQUALITY_DELETES.to_string(), value.to_string());
    }

    Ok(summary)
}

#[allow(dead_code)]
fn update_totals(
    summary: &mut Summary,
    previous_summary: Option<&Summary>,
    total_property: &str,
    added_property: &str,
    removed_property: &str,
) {
    let previous_total = previous_summary.map_or(0, |previous_summary| {
        previous_summary
            .additional_properties
            .get(total_property)
            .map_or(0, |value| value.parse::<u64>().unwrap())
    });

    let mut new_total = previous_total;
    if let Some(value) = summary
        .additional_properties
        .get(added_property)
        .map(|value| value.parse::<u64>().unwrap())
    {
        new_total += value;
    }
    if let Some(value) = summary
        .additional_properties
        .get(removed_property)
        .map(|value| value.parse::<u64>().unwrap())
    {
        new_total -= value;
    }
    summary
        .additional_properties
        .insert(total_property.to_string(), new_total.to_string());
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::spec::{
        DataFileFormat, Datum, Literal, NestedField, PartitionSpec, PrimitiveType, Schema, Struct,
        Transform, Type, UnboundPartitionField,
    };

    #[test]
    fn test_update_snapshot_summaries_append() {
        let prev_props: HashMap<String, String> = [
            (TOTAL_DATA_FILES.to_string(), "10".to_string()),
            (TOTAL_DELETE_FILES.to_string(), "5".to_string()),
            (TOTAL_RECORDS.to_string(), "100".to_string()),
            (TOTAL_FILE_SIZE.to_string(), "1000".to_string()),
            (TOTAL_POSITION_DELETES.to_string(), "3".to_string()),
            (TOTAL_EQUALITY_DELETES.to_string(), "2".to_string()),
        ]
        .into_iter()
        .collect();

        let previous_summary = Summary {
            operation: Operation::Append,
            additional_properties: prev_props,
        };

        let new_props: HashMap<String, String> = [
            (ADDED_DATA_FILES.to_string(), "4".to_string()),
            (DELETED_DATA_FILES.to_string(), "1".to_string()),
            (ADDED_DELETE_FILES.to_string(), "2".to_string()),
            (REMOVED_DELETE_FILES.to_string(), "1".to_string()),
            (ADDED_RECORDS.to_string(), "40".to_string()),
            (DELETED_RECORDS.to_string(), "10".to_string()),
            (ADDED_FILE_SIZE.to_string(), "400".to_string()),
            (REMOVED_FILE_SIZE.to_string(), "100".to_string()),
            (ADDED_POSITION_DELETES.to_string(), "5".to_string()),
            (REMOVED_POSITION_DELETES.to_string(), "2".to_string()),
            (ADDED_EQUALITY_DELETES.to_string(), "3".to_string()),
            (REMOVED_EQUALITY_DELETES.to_string(), "1".to_string()),
        ]
        .into_iter()
        .collect();

        let summary = Summary {
            operation: Operation::Append,
            additional_properties: new_props,
        };

        let updated = update_snapshot_summaries(summary, Some(&previous_summary), false).unwrap();

        assert_eq!(
            updated.additional_properties.get(TOTAL_DATA_FILES).unwrap(),
            "13"
        );
        assert_eq!(
            updated
                .additional_properties
                .get(TOTAL_DELETE_FILES)
                .unwrap(),
            "6"
        );
        assert_eq!(
            updated.additional_properties.get(TOTAL_RECORDS).unwrap(),
            "130"
        );
        assert_eq!(
            updated.additional_properties.get(TOTAL_FILE_SIZE).unwrap(),
            "1300"
        );
        assert_eq!(
            updated
                .additional_properties
                .get(TOTAL_POSITION_DELETES)
                .unwrap(),
            "6"
        );
        assert_eq!(
            updated
                .additional_properties
                .get(TOTAL_EQUALITY_DELETES)
                .unwrap(),
            "4"
        );
    }

    #[test]
    fn test_truncate_table_summary() {
        let prev_props: HashMap<String, String> = [
            (TOTAL_DATA_FILES.to_string(), "10".to_string()),
            (TOTAL_DELETE_FILES.to_string(), "5".to_string()),
            (TOTAL_RECORDS.to_string(), "100".to_string()),
            (TOTAL_FILE_SIZE.to_string(), "1000".to_string()),
            (TOTAL_POSITION_DELETES.to_string(), "3".to_string()),
            (TOTAL_EQUALITY_DELETES.to_string(), "2".to_string()),
        ]
        .into_iter()
        .collect();

        let previous_summary = Summary {
            operation: Operation::Overwrite,
            additional_properties: prev_props,
        };

        let mut new_props = HashMap::new();
        new_props.insert("dummy".to_string(), "value".to_string());
        let summary = Summary {
            operation: Operation::Overwrite,
            additional_properties: new_props,
        };

        let truncated = truncate_table_summary(summary, &previous_summary).unwrap();

        assert_eq!(
            truncated
                .additional_properties
                .get(TOTAL_DATA_FILES)
                .unwrap(),
            "0"
        );
        assert_eq!(
            truncated
                .additional_properties
                .get(TOTAL_DELETE_FILES)
                .unwrap(),
            "0"
        );
        assert_eq!(
            truncated.additional_properties.get(TOTAL_RECORDS).unwrap(),
            "0"
        );
        assert_eq!(
            truncated
                .additional_properties
                .get(TOTAL_FILE_SIZE)
                .unwrap(),
            "0"
        );
        assert_eq!(
            truncated
                .additional_properties
                .get(TOTAL_POSITION_DELETES)
                .unwrap(),
            "0"
        );
        assert_eq!(
            truncated
                .additional_properties
                .get(TOTAL_EQUALITY_DELETES)
                .unwrap(),
            "0"
        );

        assert_eq!(
            truncated
                .additional_properties
                .get(DELETED_DATA_FILES)
                .unwrap(),
            "10"
        );
        assert_eq!(
            truncated
                .additional_properties
                .get(REMOVED_DELETE_FILES)
                .unwrap(),
            "5"
        );
        assert_eq!(
            truncated
                .additional_properties
                .get(DELETED_RECORDS)
                .unwrap(),
            "100"
        );
        assert_eq!(
            truncated
                .additional_properties
                .get(REMOVED_FILE_SIZE)
                .unwrap(),
            "1000"
        );
        assert_eq!(
            truncated
                .additional_properties
                .get(REMOVED_POSITION_DELETES)
                .unwrap(),
            "3"
        );
        assert_eq!(
            truncated
                .additional_properties
                .get(REMOVED_EQUALITY_DELETES)
                .unwrap(),
            "2"
        );
    }

    #[test]
    fn test_snapshot_summary_collector_build() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );

        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .add_unbound_fields(vec![
                    UnboundPartitionField::builder()
                        .source_id(2)
                        .name("year".to_string())
                        .transform(Transform::Identity)
                        .build(),
                ])
                .unwrap()
                .with_spec_id(1)
                .build()
                .unwrap(),
        );

        let mut collector = SnapshotSummaryCollector::default();
        collector.set_partition_summary_limit(10);

        let file1 = DataFile {
            content: DataContentType::Data,
            file_path: "s3://testbucket/path/to/file1.parquet".to_string(),
            file_format: DataFileFormat::Parquet,
            partition: Struct::from_iter(vec![]),
            record_count: 10,
            file_size_in_bytes: 100,
            column_sizes: HashMap::from([(1, 46), (2, 48), (3, 48)]),
            value_counts: HashMap::from([(1, 10), (2, 10), (3, 10)]),
            null_value_counts: HashMap::from([(1, 0), (2, 0), (3, 0)]),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::from([
                (1, Datum::long(1)),
                (2, Datum::string("a")),
                (3, Datum::string("x")),
            ]),
            upper_bounds: HashMap::from([
                (1, Datum::long(1)),
                (2, Datum::string("a")),
                (3, Datum::string("x")),
            ]),
            key_metadata: None,
            split_offsets: Some(vec![4]),
            equality_ids: None,
            sort_order_id: Some(0),
            partition_spec_id: 0,
            first_row_id: None,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        };

        let file2 = DataFile {
            content: DataContentType::Data,
            file_path: "s3://testbucket/path/to/file2.parquet".to_string(),
            file_format: DataFileFormat::Parquet,
            partition: Struct::from_iter(vec![Some(Literal::string("2025"))]),
            record_count: 20,
            file_size_in_bytes: 200,
            column_sizes: HashMap::from([(1, 46), (2, 48), (3, 48)]),
            value_counts: HashMap::from([(1, 20), (2, 20), (3, 20)]),
            null_value_counts: HashMap::from([(1, 0), (2, 0), (3, 0)]),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::from([
                (1, Datum::long(1)),
                (2, Datum::string("a")),
                (3, Datum::string("x")),
            ]),
            upper_bounds: HashMap::from([
                (1, Datum::long(1)),
                (2, Datum::string("a")),
                (3, Datum::string("x")),
            ]),
            key_metadata: None,
            split_offsets: Some(vec![4]),
            equality_ids: None,
            sort_order_id: Some(0),
            partition_spec_id: 0,
            first_row_id: None,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        };

        collector.add_file(&file1, schema.clone(), partition_spec.clone());
        collector.add_file(&file2, schema.clone(), partition_spec.clone());

        collector.remove_file(&file1, schema.clone(), partition_spec.clone());

        let props = collector.build();

        assert_eq!(props.get(ADDED_FILE_SIZE).unwrap(), "300");
        assert_eq!(props.get(REMOVED_FILE_SIZE).unwrap(), "100");

        let partition_key = format!("{}{}", CHANGED_PARTITION_PREFIX, "year=2025");

        assert!(props.contains_key(&partition_key));

        let partition_summary = props.get(&partition_key).unwrap();
        assert!(partition_summary.contains(&format!("{ADDED_FILE_SIZE}=200")));
        assert!(partition_summary.contains(&format!("{ADDED_DATA_FILES}=1")));
        assert!(partition_summary.contains(&format!("{ADDED_RECORDS}=20")));
    }

    #[test]
    fn test_snapshot_summary_collector_add_manifest() {
        let mut collector = SnapshotSummaryCollector::default();
        collector.set_partition_summary_limit(10);

        let manifest = ManifestFile {
            manifest_path: "file://dummy.manifest".to_string(),
            manifest_length: 0,
            partition_spec_id: 0,
            content: ManifestContentType::Data,
            sequence_number: 0,
            min_sequence_number: 0,
            added_snapshot_id: 0,
            added_files_count: Some(3),
            existing_files_count: Some(0),
            deleted_files_count: Some(1),
            added_rows_count: Some(100),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(50),
            partitions: Some(Vec::new()),
            key_metadata: None,
            first_row_id: None,
        };

        collector
            .partition_metrics
            .insert("dummy".to_string(), UpdateMetrics::default());
        collector.add_manifest(&manifest);

        let props = collector.build();
        assert_eq!(props.get(ADDED_DATA_FILES).unwrap(), "3");
        assert_eq!(props.get(DELETED_DATA_FILES).unwrap(), "1");
        assert_eq!(props.get(ADDED_RECORDS).unwrap(), "100");
        assert_eq!(props.get(DELETED_RECORDS).unwrap(), "50");
    }

    #[test]
    fn test_snapshot_summary_collector_merge() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );

        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .add_unbound_fields(vec![
                    UnboundPartitionField::builder()
                        .source_id(2)
                        .name("year".to_string())
                        .transform(Transform::Identity)
                        .build(),
                ])
                .unwrap()
                .with_spec_id(1)
                .build()
                .unwrap(),
        );

        let mut summary_one = SnapshotSummaryCollector::default();
        let mut summary_two = SnapshotSummaryCollector::default();

        summary_one.add_file(
            &DataFile {
                content: DataContentType::Data,
                file_path: "test.parquet".into(),
                file_format: DataFileFormat::Parquet,
                partition: Struct::from_iter(vec![]),
                record_count: 10,
                file_size_in_bytes: 100,
                column_sizes: HashMap::new(),
                value_counts: HashMap::new(),
                null_value_counts: HashMap::new(),
                nan_value_counts: HashMap::new(),
                lower_bounds: HashMap::new(),
                upper_bounds: HashMap::new(),
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
                sort_order_id: None,
                partition_spec_id: 0,
                first_row_id: None,
                referenced_data_file: None,
                content_offset: None,
                content_size_in_bytes: None,
            },
            schema.clone(),
            partition_spec.clone(),
        );

        summary_two.add_file(
            &DataFile {
                content: DataContentType::Data,
                file_path: "test.parquet".into(),
                file_format: DataFileFormat::Parquet,
                partition: Struct::from_iter(vec![]),
                record_count: 20,
                file_size_in_bytes: 200,
                column_sizes: HashMap::new(),
                value_counts: HashMap::new(),
                null_value_counts: HashMap::new(),
                nan_value_counts: HashMap::new(),
                lower_bounds: HashMap::new(),
                upper_bounds: HashMap::new(),
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
                sort_order_id: None,
                partition_spec_id: 0,
                first_row_id: None,
                referenced_data_file: None,
                content_offset: None,
                content_size_in_bytes: None,
            },
            schema.clone(),
            partition_spec.clone(),
        );

        summary_one.merge(summary_two);
        let props = summary_one.build();
        assert_eq!(props.get(ADDED_DATA_FILES).unwrap(), "2");
        assert_eq!(props.get(ADDED_RECORDS).unwrap(), "30");

        let mut summary_three = SnapshotSummaryCollector::default();
        let mut summary_four = SnapshotSummaryCollector::default();

        summary_three.add_manifest(&ManifestFile {
            manifest_path: "test.manifest".to_string(),
            manifest_length: 0,
            partition_spec_id: 0,
            content: ManifestContentType::Data,
            sequence_number: 0,
            min_sequence_number: 0,
            added_snapshot_id: 0,
            added_files_count: Some(1),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(5),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: Some(Vec::new()),
            key_metadata: None,
            first_row_id: None,
        });

        summary_four.add_file(
            &DataFile {
                content: DataContentType::Data,
                file_path: "test.parquet".into(),
                file_format: DataFileFormat::Parquet,
                partition: Struct::from_iter(vec![]),
                record_count: 1,
                file_size_in_bytes: 10,
                column_sizes: HashMap::new(),
                value_counts: HashMap::new(),
                null_value_counts: HashMap::new(),
                nan_value_counts: HashMap::new(),
                lower_bounds: HashMap::new(),
                upper_bounds: HashMap::new(),
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
                sort_order_id: None,
                partition_spec_id: 0,
                first_row_id: None,
                referenced_data_file: None,
                content_offset: None,
                content_size_in_bytes: None,
            },
            schema.clone(),
            partition_spec.clone(),
        );

        summary_three.merge(summary_four);
        let props = summary_three.build();

        assert_eq!(props.get(ADDED_DATA_FILES).unwrap(), "2");
        assert_eq!(props.get(ADDED_RECORDS).unwrap(), "6");
        assert!(
            props
                .iter()
                .all(|(k, _)| !k.starts_with(CHANGED_PARTITION_PREFIX))
        );
    }
}
