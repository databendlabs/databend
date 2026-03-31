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

/*!
 * Partitioning
 */
use std::sync::Arc;

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use super::transform::Transform;
use super::{NestedField, Schema, SchemaRef, StructType};
use crate::spec::Struct;
use crate::{Error, ErrorKind, Result};

pub(crate) const UNPARTITIONED_LAST_ASSIGNED_ID: i32 = 999;
pub(crate) const DEFAULT_PARTITION_SPEC_ID: i32 = 0;

/// Partition fields capture the transform from table data to partition values.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, TypedBuilder)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionField {
    /// A source column id from the table’s schema
    pub source_id: i32,
    /// A partition field id that is used to identify a partition field and is unique within a partition spec.
    /// In v2 table metadata, it is unique across all partition specs.
    pub field_id: i32,
    /// A partition name.
    pub name: String,
    /// A transform that is applied to the source column to produce a partition value.
    pub transform: Transform,
}

impl PartitionField {
    /// To unbound partition field
    pub fn into_unbound(self) -> UnboundPartitionField {
        self.into()
    }
}

/// Reference to [`PartitionSpec`].
pub type PartitionSpecRef = Arc<PartitionSpec>;
/// Partition spec that defines how to produce a tuple of partition values from a record.
///
/// A [`PartitionSpec`] is originally obtained by binding an [`UnboundPartitionSpec`] to a schema and is
/// only guaranteed to be valid for that schema. The main difference between [`PartitionSpec`] and
/// [`UnboundPartitionSpec`] is that the former has field ids assigned,
/// while field ids are optional for [`UnboundPartitionSpec`].
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionSpec {
    /// Identifier for PartitionSpec
    spec_id: i32,
    /// Details of the partition spec
    fields: Vec<PartitionField>,
}

impl PartitionSpec {
    /// Create a new partition spec builder with the given schema.
    pub fn builder(schema: impl Into<SchemaRef>) -> PartitionSpecBuilder {
        PartitionSpecBuilder::new(schema)
    }

    /// Fields of the partition spec
    pub fn fields(&self) -> &[PartitionField] {
        &self.fields
    }

    /// Spec id of the partition spec
    pub fn spec_id(&self) -> i32 {
        self.spec_id
    }

    /// Get a new unpartitioned partition spec
    pub fn unpartition_spec() -> Self {
        Self {
            spec_id: DEFAULT_PARTITION_SPEC_ID,
            fields: vec![],
        }
    }

    /// Returns if the partition spec is unpartitioned.
    ///
    /// A [`PartitionSpec`] is unpartitioned if it has no fields or all fields are [`Transform::Void`] transform.
    pub fn is_unpartitioned(&self) -> bool {
        self.fields.is_empty() || self.fields.iter().all(|f| f.transform == Transform::Void)
    }

    /// Returns the partition type of this partition spec.
    pub fn partition_type(&self, schema: &Schema) -> Result<StructType> {
        PartitionSpecBuilder::partition_type(&self.fields, schema)
    }

    /// Convert to unbound partition spec
    pub fn into_unbound(self) -> UnboundPartitionSpec {
        self.into()
    }

    /// Change the spec id of the partition spec
    pub fn with_spec_id(self, spec_id: i32) -> Self {
        Self { spec_id, ..self }
    }

    /// Check if this partition spec has sequential partition ids.
    /// Sequential ids start from 1000 and increment by 1 for each field.
    /// This is required for spec version 1
    pub fn has_sequential_ids(&self) -> bool {
        has_sequential_ids(self.fields.iter().map(|f| f.field_id))
    }

    /// Get the highest field id in the partition spec.
    pub fn highest_field_id(&self) -> Option<i32> {
        self.fields.iter().map(|f| f.field_id).max()
    }

    /// Check if this partition spec is compatible with another partition spec.
    ///
    /// Returns true if the partition spec is equal to the other spec with partition field ids ignored and
    /// spec_id ignored. The following must be identical:
    /// * The number of fields
    /// * Field order
    /// * Field names
    /// * Source column ids
    /// * Transforms
    pub fn is_compatible_with(&self, other: &PartitionSpec) -> bool {
        if self.fields.len() != other.fields.len() {
            return false;
        }

        for (this_field, other_field) in self.fields.iter().zip(other.fields.iter()) {
            if this_field.source_id != other_field.source_id
                || this_field.name != other_field.name
                || this_field.transform != other_field.transform
            {
                return false;
            }
        }

        true
    }

    /// Returns partition path string containing partition type and partition
    /// value as key-value pairs.
    pub fn partition_to_path(&self, data: &Struct, schema: SchemaRef) -> String {
        let partition_type = self.partition_type(&schema).unwrap();
        let field_types = partition_type.fields();

        self.fields
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let value = data[i].as_ref();
                format!(
                    "{}={}",
                    field.name,
                    field
                        .transform
                        .to_human_string(&field_types[i].field_type, value)
                )
            })
            .join("/")
    }
}

/// A partition key represents a specific partition in a table, containing the partition spec,
/// schema, and the actual partition values.
#[derive(Clone, Debug)]
pub struct PartitionKey {
    /// The partition spec that contains the partition fields.
    spec: PartitionSpec,
    /// The schema to which the partition spec is bound.
    schema: SchemaRef,
    /// Partition fields' values in struct.
    data: Struct,
}

impl PartitionKey {
    /// Creates a new partition key with the given spec, schema, and data.
    pub fn new(spec: PartitionSpec, schema: SchemaRef, data: Struct) -> Self {
        Self { spec, schema, data }
    }

    /// Creates a new partition key from another partition key, with a new data field.
    pub fn copy_with_data(&self, data: Struct) -> Self {
        Self {
            spec: self.spec.clone(),
            schema: self.schema.clone(),
            data,
        }
    }

    /// Generates a partition path based on the partition values.
    pub fn to_path(&self) -> String {
        self.spec.partition_to_path(&self.data, self.schema.clone())
    }

    /// Returns `true` if the partition key is absent (`None`)
    /// or represents an unpartitioned spec.
    pub fn is_effectively_none(partition_key: Option<&PartitionKey>) -> bool {
        match partition_key {
            None => true,
            Some(pk) => pk.spec.is_unpartitioned(),
        }
    }

    /// Returns the associated [`PartitionSpec`].
    pub fn spec(&self) -> &PartitionSpec {
        &self.spec
    }

    /// Returns the associated [`SchemaRef`].
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns the associated [`Struct`].
    pub fn data(&self) -> &Struct {
        &self.data
    }
}

/// Reference to [`UnboundPartitionSpec`].
pub type UnboundPartitionSpecRef = Arc<UnboundPartitionSpec>;
/// Unbound partition field can be built without a schema and later bound to a schema.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, TypedBuilder)]
#[serde(rename_all = "kebab-case")]
pub struct UnboundPartitionField {
    /// A source column id from the table’s schema
    pub source_id: i32,
    /// A partition field id that is used to identify a partition field and is unique within a partition spec.
    /// In v2 table metadata, it is unique across all partition specs.
    #[builder(default, setter(strip_option(fallback = field_id_opt)))]
    pub field_id: Option<i32>,
    /// A partition name.
    pub name: String,
    /// A transform that is applied to the source column to produce a partition value.
    pub transform: Transform,
}

/// Unbound partition spec can be built without a schema and later bound to a schema.
/// They are used to transport schema information as part of the REST specification.
/// The main difference to [`PartitionSpec`] is that the field ids are optional.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default)]
#[serde(rename_all = "kebab-case")]
pub struct UnboundPartitionSpec {
    /// Identifier for PartitionSpec
    pub(crate) spec_id: Option<i32>,
    /// Details of the partition spec
    pub(crate) fields: Vec<UnboundPartitionField>,
}

impl UnboundPartitionSpec {
    /// Create unbound partition spec builder
    pub fn builder() -> UnboundPartitionSpecBuilder {
        UnboundPartitionSpecBuilder::default()
    }

    /// Bind this unbound partition spec to a schema.
    pub fn bind(self, schema: impl Into<SchemaRef>) -> Result<PartitionSpec> {
        PartitionSpecBuilder::new_from_unbound(self, schema)?.build()
    }

    /// Spec id of the partition spec
    pub fn spec_id(&self) -> Option<i32> {
        self.spec_id
    }

    /// Fields of the partition spec
    pub fn fields(&self) -> &[UnboundPartitionField] {
        &self.fields
    }

    /// Change the spec id of the partition spec
    pub fn with_spec_id(self, spec_id: i32) -> Self {
        Self {
            spec_id: Some(spec_id),
            ..self
        }
    }
}

fn has_sequential_ids(field_ids: impl Iterator<Item = i32>) -> bool {
    for (index, field_id) in field_ids.enumerate() {
        let expected_id = (UNPARTITIONED_LAST_ASSIGNED_ID as i64)
            .checked_add(1)
            .and_then(|id| id.checked_add(index as i64))
            .unwrap_or(i64::MAX);

        if field_id as i64 != expected_id {
            return false;
        }
    }

    true
}

impl From<PartitionField> for UnboundPartitionField {
    fn from(field: PartitionField) -> Self {
        UnboundPartitionField {
            source_id: field.source_id,
            field_id: Some(field.field_id),
            name: field.name,
            transform: field.transform,
        }
    }
}

impl From<PartitionSpec> for UnboundPartitionSpec {
    fn from(spec: PartitionSpec) -> Self {
        UnboundPartitionSpec {
            spec_id: Some(spec.spec_id),
            fields: spec.fields.into_iter().map(Into::into).collect(),
        }
    }
}

/// Create a new UnboundPartitionSpec
#[derive(Debug, Default)]
pub struct UnboundPartitionSpecBuilder {
    spec_id: Option<i32>,
    fields: Vec<UnboundPartitionField>,
}

impl UnboundPartitionSpecBuilder {
    /// Create a new partition spec builder with the given schema.
    pub fn new() -> Self {
        Self {
            spec_id: None,
            fields: vec![],
        }
    }

    /// Set the spec id for the partition spec.
    pub fn with_spec_id(mut self, spec_id: i32) -> Self {
        self.spec_id = Some(spec_id);
        self
    }

    /// Add a new partition field to the partition spec from an unbound partition field.
    pub fn add_partition_field(
        self,
        source_id: i32,
        target_name: impl ToString,
        transformation: Transform,
    ) -> Result<Self> {
        let field = UnboundPartitionField {
            source_id,
            field_id: None,
            name: target_name.to_string(),
            transform: transformation,
        };
        self.add_partition_field_internal(field)
    }

    /// Add multiple partition fields to the partition spec.
    pub fn add_partition_fields(
        self,
        fields: impl IntoIterator<Item = UnboundPartitionField>,
    ) -> Result<Self> {
        let mut builder = self;
        for field in fields {
            builder = builder.add_partition_field_internal(field)?;
        }
        Ok(builder)
    }

    fn add_partition_field_internal(mut self, field: UnboundPartitionField) -> Result<Self> {
        self.check_name_set_and_unique(&field.name)?;
        self.check_for_redundant_partitions(field.source_id, &field.transform)?;
        if let Some(partition_field_id) = field.field_id {
            self.check_partition_id_unique(partition_field_id)?;
        }
        self.fields.push(field);
        Ok(self)
    }

    /// Build the unbound partition spec.
    pub fn build(self) -> UnboundPartitionSpec {
        UnboundPartitionSpec {
            spec_id: self.spec_id,
            fields: self.fields,
        }
    }
}

/// Create valid partition specs for a given schema.
#[derive(Debug)]
pub struct PartitionSpecBuilder {
    spec_id: Option<i32>,
    last_assigned_field_id: i32,
    fields: Vec<UnboundPartitionField>,
    schema: SchemaRef,
}

impl PartitionSpecBuilder {
    /// Create a new partition spec builder with the given schema.
    pub fn new(schema: impl Into<SchemaRef>) -> Self {
        Self {
            spec_id: None,
            fields: vec![],
            last_assigned_field_id: UNPARTITIONED_LAST_ASSIGNED_ID,
            schema: schema.into(),
        }
    }

    /// Create a new partition spec builder from an existing unbound partition spec.
    pub fn new_from_unbound(
        unbound: UnboundPartitionSpec,
        schema: impl Into<SchemaRef>,
    ) -> Result<Self> {
        let mut builder =
            Self::new(schema).with_spec_id(unbound.spec_id.unwrap_or(DEFAULT_PARTITION_SPEC_ID));

        for field in unbound.fields {
            builder = builder.add_unbound_field(field)?;
        }
        Ok(builder)
    }

    /// Set the last assigned field id for the partition spec.
    ///
    /// Set this field when a new partition spec is created for an existing TableMetaData.
    /// As `field_id` must be unique in V2 metadata, this should be set to
    /// the highest field id used previously.
    pub fn with_last_assigned_field_id(mut self, last_assigned_field_id: i32) -> Self {
        self.last_assigned_field_id = last_assigned_field_id;
        self
    }

    /// Set the spec id for the partition spec.
    pub fn with_spec_id(mut self, spec_id: i32) -> Self {
        self.spec_id = Some(spec_id);
        self
    }

    /// Add a new partition field to the partition spec.
    pub fn add_partition_field(
        self,
        source_name: impl AsRef<str>,
        target_name: impl Into<String>,
        transform: Transform,
    ) -> Result<Self> {
        let source_id = self
            .schema
            .field_by_name(source_name.as_ref())
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot find source column with name: {} in schema",
                        source_name.as_ref()
                    ),
                )
            })?
            .id;
        let field = UnboundPartitionField {
            source_id,
            field_id: None,
            name: target_name.into(),
            transform,
        };

        self.add_unbound_field(field)
    }

    /// Add a new partition field to the partition spec.
    ///
    /// If partition field id is set, it is used as the field id.
    /// Otherwise, a new `field_id` is assigned.
    pub fn add_unbound_field(mut self, field: UnboundPartitionField) -> Result<Self> {
        self.check_name_set_and_unique(&field.name)?;
        self.check_for_redundant_partitions(field.source_id, &field.transform)?;
        Self::check_name_does_not_collide_with_schema(&field, &self.schema)?;
        Self::check_transform_compatibility(&field, &self.schema)?;
        if let Some(partition_field_id) = field.field_id {
            self.check_partition_id_unique(partition_field_id)?;
        }

        // Non-fallible from here
        self.fields.push(field);
        Ok(self)
    }

    /// Wrapper around `with_unbound_fields` to add multiple partition fields.
    pub fn add_unbound_fields(
        self,
        fields: impl IntoIterator<Item = UnboundPartitionField>,
    ) -> Result<Self> {
        let mut builder = self;
        for field in fields {
            builder = builder.add_unbound_field(field)?;
        }
        Ok(builder)
    }

    /// Build a bound partition spec with the given schema.
    pub fn build(self) -> Result<PartitionSpec> {
        let fields = Self::set_field_ids(self.fields, self.last_assigned_field_id)?;
        Ok(PartitionSpec {
            spec_id: self.spec_id.unwrap_or(DEFAULT_PARTITION_SPEC_ID),
            fields,
        })
    }

    fn set_field_ids(
        fields: Vec<UnboundPartitionField>,
        last_assigned_field_id: i32,
    ) -> Result<Vec<PartitionField>> {
        let mut last_assigned_field_id = last_assigned_field_id;
        // Already assigned partition ids. If we see one of these during iteration,
        // we skip it.
        let assigned_ids = fields
            .iter()
            .filter_map(|f| f.field_id)
            .collect::<std::collections::HashSet<_>>();

        fn _check_add_1(prev: i32) -> Result<i32> {
            prev.checked_add(1).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Cannot assign more partition ids. Overflow.",
                )
            })
        }

        let mut bound_fields = Vec::with_capacity(fields.len());
        for field in fields.into_iter() {
            let partition_field_id = if let Some(partition_field_id) = field.field_id {
                last_assigned_field_id = std::cmp::max(last_assigned_field_id, partition_field_id);
                partition_field_id
            } else {
                last_assigned_field_id = _check_add_1(last_assigned_field_id)?;
                while assigned_ids.contains(&last_assigned_field_id) {
                    last_assigned_field_id = _check_add_1(last_assigned_field_id)?;
                }
                last_assigned_field_id
            };

            bound_fields.push(PartitionField {
                source_id: field.source_id,
                field_id: partition_field_id,
                name: field.name,
                transform: field.transform,
            })
        }

        Ok(bound_fields)
    }

    /// Returns the partition type of this partition spec.
    fn partition_type(fields: &Vec<PartitionField>, schema: &Schema) -> Result<StructType> {
        let mut struct_fields = Vec::with_capacity(fields.len());
        for partition_field in fields {
            let field = schema
                .field_by_id(partition_field.source_id)
                .ok_or_else(|| {
                    Error::new(
                        // This should never occur as check_transform_compatibility
                        // already ensures that the source field exists in the schema
                        ErrorKind::Unexpected,
                        format!(
                            "No column with source column id {} in schema {:?}",
                            partition_field.source_id, schema
                        ),
                    )
                })?;
            let res_type = partition_field.transform.result_type(&field.field_type)?;
            let field =
                NestedField::optional(partition_field.field_id, &partition_field.name, res_type)
                    .into();
            struct_fields.push(field);
        }
        Ok(StructType::new(struct_fields))
    }

    /// Ensure that the partition name is unique among columns in the schema.
    /// Duplicate names are allowed if:
    /// 1. The column is sourced from the column with the same name.
    /// 2. AND the transformation is identity
    fn check_name_does_not_collide_with_schema(
        field: &UnboundPartitionField,
        schema: &Schema,
    ) -> Result<()> {
        match schema.field_by_name(field.name.as_str()) {
            Some(schema_collision) => {
                if field.transform == Transform::Identity {
                    if schema_collision.id == field.source_id {
                        Ok(())
                    } else {
                        Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "Cannot create identity partition sourced from different field in schema. Field name '{}' has id `{}` in schema but partition source id is `{}`",
                                field.name, schema_collision.id, field.source_id
                            ),
                        ))
                    }
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Cannot create partition with name: '{}' that conflicts with schema field and is not an identity transform.",
                            field.name
                        ),
                    ))
                }
            }
            None => Ok(()),
        }
    }

    /// Ensure that the transformation of the field is compatible with type of the field
    /// in the schema. Implicitly also checks if the source field exists in the schema.
    fn check_transform_compatibility(field: &UnboundPartitionField, schema: &Schema) -> Result<()> {
        let schema_field = schema.field_by_id(field.source_id).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot find partition source field with id `{}` in schema",
                    field.source_id
                ),
            )
        })?;

        if field.transform != Transform::Void {
            if !schema_field.field_type.is_primitive() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot partition by non-primitive source field: '{}'.",
                        schema_field.field_type
                    ),
                ));
            }

            if field
                .transform
                .result_type(&schema_field.field_type)
                .is_err()
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Invalid source type: '{}' for transform: '{}'.",
                        schema_field.field_type,
                        field.transform.dedup_name()
                    ),
                ));
            }
        }

        Ok(())
    }
}

/// Contains checks that are common to both PartitionSpecBuilder and UnboundPartitionSpecBuilder
trait CorePartitionSpecValidator {
    /// Ensure that the partition name is unique among the partition fields and is not empty.
    fn check_name_set_and_unique(&self, name: &str) -> Result<()> {
        if name.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot use empty partition name",
            ));
        }

        if self.fields().iter().any(|f| f.name == name) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Cannot use partition name more than once: {name}"),
            ));
        }
        Ok(())
    }

    /// For a single source-column transformations must be unique.
    fn check_for_redundant_partitions(&self, source_id: i32, transform: &Transform) -> Result<()> {
        let collision = self.fields().iter().find(|f| {
            f.source_id == source_id && f.transform.dedup_name() == transform.dedup_name()
        });

        if let Some(collision) = collision {
            Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot add redundant partition with source id `{}` and transform `{}`. A partition with the same source id and transform already exists with name `{}`",
                    source_id,
                    transform.dedup_name(),
                    collision.name
                ),
            ))
        } else {
            Ok(())
        }
    }

    /// Check field / partition_id unique within the partition spec if set
    fn check_partition_id_unique(&self, field_id: i32) -> Result<()> {
        if self.fields().iter().any(|f| f.field_id == Some(field_id)) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Cannot use field id more than once in one PartitionSpec: {field_id}"),
            ));
        }

        Ok(())
    }

    fn fields(&self) -> &Vec<UnboundPartitionField>;
}

impl CorePartitionSpecValidator for PartitionSpecBuilder {
    fn fields(&self) -> &Vec<UnboundPartitionField> {
        &self.fields
    }
}

impl CorePartitionSpecValidator for UnboundPartitionSpecBuilder {
    fn fields(&self) -> &Vec<UnboundPartitionField> {
        &self.fields
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{Literal, PrimitiveType, Type};

    #[test]
    fn test_partition_spec() {
        let spec = r#"
        {
        "spec-id": 1,
        "fields": [ {
            "source-id": 4,
            "field-id": 1000,
            "name": "ts_day",
            "transform": "day"
            }, {
            "source-id": 1,
            "field-id": 1001,
            "name": "id_bucket",
            "transform": "bucket[16]"
            }, {
            "source-id": 2,
            "field-id": 1002,
            "name": "id_truncate",
            "transform": "truncate[4]"
            } ]
        }
        "#;

        let partition_spec: PartitionSpec = serde_json::from_str(spec).unwrap();
        assert_eq!(4, partition_spec.fields[0].source_id);
        assert_eq!(1000, partition_spec.fields[0].field_id);
        assert_eq!("ts_day", partition_spec.fields[0].name);
        assert_eq!(Transform::Day, partition_spec.fields[0].transform);

        assert_eq!(1, partition_spec.fields[1].source_id);
        assert_eq!(1001, partition_spec.fields[1].field_id);
        assert_eq!("id_bucket", partition_spec.fields[1].name);
        assert_eq!(Transform::Bucket(16), partition_spec.fields[1].transform);

        assert_eq!(2, partition_spec.fields[2].source_id);
        assert_eq!(1002, partition_spec.fields[2].field_id);
        assert_eq!("id_truncate", partition_spec.fields[2].name);
        assert_eq!(Transform::Truncate(4), partition_spec.fields[2].transform);
    }

    #[test]
    fn test_is_unpartitioned() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(crate::spec::PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "name",
                    Type::Primitive(crate::spec::PrimitiveType::String),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(1)
            .build()
            .unwrap();
        assert!(
            partition_spec.is_unpartitioned(),
            "Empty partition spec should be unpartitioned"
        );

        let partition_spec = PartitionSpec::builder(schema.clone())
            .add_unbound_fields(vec![
                UnboundPartitionField::builder()
                    .source_id(1)
                    .name("id".to_string())
                    .transform(Transform::Identity)
                    .build(),
                UnboundPartitionField::builder()
                    .source_id(2)
                    .name("name_string".to_string())
                    .transform(Transform::Void)
                    .build(),
            ])
            .unwrap()
            .with_spec_id(1)
            .build()
            .unwrap();
        assert!(
            !partition_spec.is_unpartitioned(),
            "Partition spec with one non void transform should not be unpartitioned"
        );

        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(1)
            .add_unbound_fields(vec![
                UnboundPartitionField::builder()
                    .source_id(1)
                    .name("id_void".to_string())
                    .transform(Transform::Void)
                    .build(),
                UnboundPartitionField::builder()
                    .source_id(2)
                    .name("name_void".to_string())
                    .transform(Transform::Void)
                    .build(),
            ])
            .unwrap()
            .build()
            .unwrap();
        assert!(
            partition_spec.is_unpartitioned(),
            "Partition spec with all void field should be unpartitioned"
        );
    }

    #[test]
    fn test_unbound_partition_spec() {
        let spec = r#"
		{
		"spec-id": 1,
		"fields": [ {
			"source-id": 4,
			"field-id": 1000,
			"name": "ts_day",
			"transform": "day"
			}, {
			"source-id": 1,
			"field-id": 1001,
			"name": "id_bucket",
			"transform": "bucket[16]"
			}, {
			"source-id": 2,
			"field-id": 1002,
			"name": "id_truncate",
			"transform": "truncate[4]"
			} ]
		}
		"#;

        let partition_spec: UnboundPartitionSpec = serde_json::from_str(spec).unwrap();
        assert_eq!(Some(1), partition_spec.spec_id);

        assert_eq!(4, partition_spec.fields[0].source_id);
        assert_eq!(Some(1000), partition_spec.fields[0].field_id);
        assert_eq!("ts_day", partition_spec.fields[0].name);
        assert_eq!(Transform::Day, partition_spec.fields[0].transform);

        assert_eq!(1, partition_spec.fields[1].source_id);
        assert_eq!(Some(1001), partition_spec.fields[1].field_id);
        assert_eq!("id_bucket", partition_spec.fields[1].name);
        assert_eq!(Transform::Bucket(16), partition_spec.fields[1].transform);

        assert_eq!(2, partition_spec.fields[2].source_id);
        assert_eq!(Some(1002), partition_spec.fields[2].field_id);
        assert_eq!("id_truncate", partition_spec.fields[2].name);
        assert_eq!(Transform::Truncate(4), partition_spec.fields[2].transform);

        let spec = r#"
		{
		"fields": [ {
			"source-id": 4,
			"name": "ts_day",
			"transform": "day"
			} ]
		}
		"#;
        let partition_spec: UnboundPartitionSpec = serde_json::from_str(spec).unwrap();
        assert_eq!(None, partition_spec.spec_id);

        assert_eq!(4, partition_spec.fields[0].source_id);
        assert_eq!(None, partition_spec.fields[0].field_id);
        assert_eq!("ts_day", partition_spec.fields[0].name);
        assert_eq!(Transform::Day, partition_spec.fields[0].transform);
    }

    #[test]
    fn test_new_unpartition() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(crate::spec::PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "name",
                    Type::Primitive(crate::spec::PrimitiveType::String),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .build()
            .unwrap();
        let partition_type = partition_spec.partition_type(&schema).unwrap();
        assert_eq!(0, partition_type.fields().len());

        let unpartition_spec = PartitionSpec::unpartition_spec();
        assert_eq!(partition_spec, unpartition_spec);
    }

    #[test]
    fn test_partition_type() {
        let spec = r#"
            {
            "spec-id": 1,
            "fields": [ {
                "source-id": 4,
                "field-id": 1000,
                "name": "ts_day",
                "transform": "day"
                }, {
                "source-id": 1,
                "field-id": 1001,
                "name": "id_bucket",
                "transform": "bucket[16]"
                }, {
                "source-id": 2,
                "field-id": 1002,
                "name": "id_truncate",
                "transform": "truncate[4]"
                } ]
            }
            "#;

        let partition_spec: PartitionSpec = serde_json::from_str(spec).unwrap();
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(crate::spec::PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "name",
                    Type::Primitive(crate::spec::PrimitiveType::String),
                )
                .into(),
                NestedField::required(
                    3,
                    "ts",
                    Type::Primitive(crate::spec::PrimitiveType::Timestamp),
                )
                .into(),
                NestedField::required(
                    4,
                    "ts_day",
                    Type::Primitive(crate::spec::PrimitiveType::Timestamp),
                )
                .into(),
                NestedField::required(
                    5,
                    "id_bucket",
                    Type::Primitive(crate::spec::PrimitiveType::Int),
                )
                .into(),
                NestedField::required(
                    6,
                    "id_truncate",
                    Type::Primitive(crate::spec::PrimitiveType::Int),
                )
                .into(),
            ])
            .build()
            .unwrap();

        let partition_type = partition_spec.partition_type(&schema).unwrap();
        assert_eq!(3, partition_type.fields().len());
        assert_eq!(
            *partition_type.fields()[0],
            NestedField::optional(
                partition_spec.fields[0].field_id,
                &partition_spec.fields[0].name,
                Type::Primitive(crate::spec::PrimitiveType::Date)
            )
        );
        assert_eq!(
            *partition_type.fields()[1],
            NestedField::optional(
                partition_spec.fields[1].field_id,
                &partition_spec.fields[1].name,
                Type::Primitive(crate::spec::PrimitiveType::Int)
            )
        );
        assert_eq!(
            *partition_type.fields()[2],
            NestedField::optional(
                partition_spec.fields[2].field_id,
                &partition_spec.fields[2].name,
                Type::Primitive(crate::spec::PrimitiveType::String)
            )
        );
    }

    #[test]
    fn test_partition_empty() {
        let spec = r#"
            {
            "spec-id": 1,
            "fields": []
            }
            "#;

        let partition_spec: PartitionSpec = serde_json::from_str(spec).unwrap();
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(crate::spec::PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "name",
                    Type::Primitive(crate::spec::PrimitiveType::String),
                )
                .into(),
                NestedField::required(
                    3,
                    "ts",
                    Type::Primitive(crate::spec::PrimitiveType::Timestamp),
                )
                .into(),
                NestedField::required(
                    4,
                    "ts_day",
                    Type::Primitive(crate::spec::PrimitiveType::Timestamp),
                )
                .into(),
                NestedField::required(
                    5,
                    "id_bucket",
                    Type::Primitive(crate::spec::PrimitiveType::Int),
                )
                .into(),
                NestedField::required(
                    6,
                    "id_truncate",
                    Type::Primitive(crate::spec::PrimitiveType::Int),
                )
                .into(),
            ])
            .build()
            .unwrap();

        let partition_type = partition_spec.partition_type(&schema).unwrap();
        assert_eq!(0, partition_type.fields().len());
    }

    #[test]
    fn test_partition_error() {
        let spec = r#"
        {
        "spec-id": 1,
        "fields": [ {
            "source-id": 4,
            "field-id": 1000,
            "name": "ts_day",
            "transform": "day"
            }, {
            "source-id": 1,
            "field-id": 1001,
            "name": "id_bucket",
            "transform": "bucket[16]"
            }, {
            "source-id": 2,
            "field-id": 1002,
            "name": "id_truncate",
            "transform": "truncate[4]"
            } ]
        }
        "#;

        let partition_spec: PartitionSpec = serde_json::from_str(spec).unwrap();
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(crate::spec::PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "name",
                    Type::Primitive(crate::spec::PrimitiveType::String),
                )
                .into(),
            ])
            .build()
            .unwrap();

        assert!(partition_spec.partition_type(&schema).is_err());
    }

    #[test]
    fn test_builder_disallow_duplicate_names() {
        UnboundPartitionSpec::builder()
            .add_partition_field(1, "ts_day".to_string(), Transform::Day)
            .unwrap()
            .add_partition_field(2, "ts_day".to_string(), Transform::Day)
            .unwrap_err();
    }

    #[test]
    fn test_builder_disallow_duplicate_field_ids() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(crate::spec::PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "name",
                    Type::Primitive(crate::spec::PrimitiveType::String),
                )
                .into(),
            ])
            .build()
            .unwrap();
        PartitionSpec::builder(schema.clone())
            .add_unbound_field(UnboundPartitionField {
                source_id: 1,
                field_id: Some(1000),
                name: "id".to_string(),
                transform: Transform::Identity,
            })
            .unwrap()
            .add_unbound_field(UnboundPartitionField {
                source_id: 2,
                field_id: Some(1000),
                name: "id_bucket".to_string(),
                transform: Transform::Bucket(16),
            })
            .unwrap_err();
    }

    #[test]
    fn test_builder_auto_assign_field_ids() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(crate::spec::PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "name",
                    Type::Primitive(crate::spec::PrimitiveType::String),
                )
                .into(),
                NestedField::required(
                    3,
                    "ts",
                    Type::Primitive(crate::spec::PrimitiveType::Timestamp),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(1)
            .add_unbound_field(UnboundPartitionField {
                source_id: 1,
                name: "id".to_string(),
                transform: Transform::Identity,
                field_id: Some(1012),
            })
            .unwrap()
            .add_unbound_field(UnboundPartitionField {
                source_id: 2,
                name: "name_void".to_string(),
                transform: Transform::Void,
                field_id: None,
            })
            .unwrap()
            // Should keep its ID even if its lower
            .add_unbound_field(UnboundPartitionField {
                source_id: 3,
                name: "year".to_string(),
                transform: Transform::Year,
                field_id: Some(1),
            })
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(1012, spec.fields[0].field_id);
        assert_eq!(1013, spec.fields[1].field_id);
        assert_eq!(1, spec.fields[2].field_id);
    }

    #[test]
    fn test_builder_valid_schema() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(crate::spec::PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "name",
                    Type::Primitive(crate::spec::PrimitiveType::String),
                )
                .into(),
            ])
            .build()
            .unwrap();

        PartitionSpec::builder(schema.clone())
            .with_spec_id(1)
            .build()
            .unwrap();

        let spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(1)
            .add_partition_field("id", "id_bucket[16]", Transform::Bucket(16))
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(spec, PartitionSpec {
            spec_id: 1,
            fields: vec![PartitionField {
                source_id: 1,
                field_id: 1000,
                name: "id_bucket[16]".to_string(),
                transform: Transform::Bucket(16),
            }],
        });
        assert_eq!(
            spec.partition_type(&schema).unwrap(),
            StructType::new(vec![
                NestedField::optional(1000, "id_bucket[16]", Type::Primitive(PrimitiveType::Int))
                    .into()
            ])
        )
    }

    #[test]
    fn test_collision_with_schema_name() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(crate::spec::PrimitiveType::Int))
                    .into(),
            ])
            .build()
            .unwrap();

        PartitionSpec::builder(schema.clone())
            .with_spec_id(1)
            .build()
            .unwrap();

        let err = PartitionSpec::builder(schema)
            .with_spec_id(1)
            .add_unbound_field(UnboundPartitionField {
                source_id: 1,
                field_id: None,
                name: "id".to_string(),
                transform: Transform::Bucket(16),
            })
            .unwrap_err();
        assert!(err.message().contains("conflicts with schema"))
    }

    #[test]
    fn test_builder_collision_is_ok_for_identity_transforms() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(crate::spec::PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "number",
                    Type::Primitive(crate::spec::PrimitiveType::Int),
                )
                .into(),
            ])
            .build()
            .unwrap();

        PartitionSpec::builder(schema.clone())
            .with_spec_id(1)
            .build()
            .unwrap();

        PartitionSpec::builder(schema.clone())
            .with_spec_id(1)
            .add_unbound_field(UnboundPartitionField {
                source_id: 1,
                field_id: None,
                name: "id".to_string(),
                transform: Transform::Identity,
            })
            .unwrap()
            .build()
            .unwrap();

        // Not OK for different source id
        PartitionSpec::builder(schema)
            .with_spec_id(1)
            .add_unbound_field(UnboundPartitionField {
                source_id: 2,
                field_id: None,
                name: "id".to_string(),
                transform: Transform::Identity,
            })
            .unwrap_err();
    }

    #[test]
    fn test_builder_all_source_ids_must_exist() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(crate::spec::PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "name",
                    Type::Primitive(crate::spec::PrimitiveType::String),
                )
                .into(),
                NestedField::required(
                    3,
                    "ts",
                    Type::Primitive(crate::spec::PrimitiveType::Timestamp),
                )
                .into(),
            ])
            .build()
            .unwrap();

        // Valid
        PartitionSpec::builder(schema.clone())
            .with_spec_id(1)
            .add_unbound_fields(vec![
                UnboundPartitionField {
                    source_id: 1,
                    field_id: None,
                    name: "id_bucket".to_string(),
                    transform: Transform::Bucket(16),
                },
                UnboundPartitionField {
                    source_id: 2,
                    field_id: None,
                    name: "name".to_string(),
                    transform: Transform::Identity,
                },
            ])
            .unwrap()
            .build()
            .unwrap();

        // Invalid
        PartitionSpec::builder(schema)
            .with_spec_id(1)
            .add_unbound_fields(vec![
                UnboundPartitionField {
                    source_id: 1,
                    field_id: None,
                    name: "id_bucket".to_string(),
                    transform: Transform::Bucket(16),
                },
                UnboundPartitionField {
                    source_id: 4,
                    field_id: None,
                    name: "name".to_string(),
                    transform: Transform::Identity,
                },
            ])
            .unwrap_err();
    }

    #[test]
    fn test_builder_disallows_redundant() {
        let err = UnboundPartitionSpec::builder()
            .with_spec_id(1)
            .add_partition_field(1, "id_bucket[16]".to_string(), Transform::Bucket(16))
            .unwrap()
            .add_partition_field(
                1,
                "id_bucket_with_other_name".to_string(),
                Transform::Bucket(16),
            )
            .unwrap_err();
        assert!(err.message().contains("redundant partition"));
    }

    #[test]
    fn test_builder_incompatible_transforms_disallowed() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(crate::spec::PrimitiveType::Int))
                    .into(),
            ])
            .build()
            .unwrap();

        PartitionSpec::builder(schema)
            .with_spec_id(1)
            .add_unbound_field(UnboundPartitionField {
                source_id: 1,
                field_id: None,
                name: "id_year".to_string(),
                transform: Transform::Year,
            })
            .unwrap_err();
    }

    #[test]
    fn test_build_unbound_specs_without_partition_id() {
        let spec = UnboundPartitionSpec::builder()
            .with_spec_id(1)
            .add_partition_fields(vec![UnboundPartitionField {
                source_id: 1,
                field_id: None,
                name: "id_bucket[16]".to_string(),
                transform: Transform::Bucket(16),
            }])
            .unwrap()
            .build();

        assert_eq!(spec, UnboundPartitionSpec {
            spec_id: Some(1),
            fields: vec![UnboundPartitionField {
                source_id: 1,
                field_id: None,
                name: "id_bucket[16]".to_string(),
                transform: Transform::Bucket(16),
            }]
        });
    }

    #[test]
    fn test_is_compatible_with() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(crate::spec::PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "name",
                    Type::Primitive(crate::spec::PrimitiveType::String),
                )
                .into(),
            ])
            .build()
            .unwrap();

        let partition_spec_1 = PartitionSpec::builder(schema.clone())
            .with_spec_id(1)
            .add_unbound_field(UnboundPartitionField {
                source_id: 1,
                field_id: None,
                name: "id_bucket".to_string(),
                transform: Transform::Bucket(16),
            })
            .unwrap()
            .build()
            .unwrap();

        let partition_spec_2 = PartitionSpec::builder(schema)
            .with_spec_id(1)
            .add_unbound_field(UnboundPartitionField {
                source_id: 1,
                field_id: None,
                name: "id_bucket".to_string(),
                transform: Transform::Bucket(16),
            })
            .unwrap()
            .build()
            .unwrap();

        assert!(partition_spec_1.is_compatible_with(&partition_spec_2));
    }

    #[test]
    fn test_not_compatible_with_transform_different() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(crate::spec::PrimitiveType::Int))
                    .into(),
            ])
            .build()
            .unwrap();

        let partition_spec_1 = PartitionSpec::builder(schema.clone())
            .with_spec_id(1)
            .add_unbound_field(UnboundPartitionField {
                source_id: 1,
                field_id: None,
                name: "id_bucket".to_string(),
                transform: Transform::Bucket(16),
            })
            .unwrap()
            .build()
            .unwrap();

        let partition_spec_2 = PartitionSpec::builder(schema)
            .with_spec_id(1)
            .add_unbound_field(UnboundPartitionField {
                source_id: 1,
                field_id: None,
                name: "id_bucket".to_string(),
                transform: Transform::Bucket(32),
            })
            .unwrap()
            .build()
            .unwrap();

        assert!(!partition_spec_1.is_compatible_with(&partition_spec_2));
    }

    #[test]
    fn test_not_compatible_with_source_id_different() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(crate::spec::PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "name",
                    Type::Primitive(crate::spec::PrimitiveType::String),
                )
                .into(),
            ])
            .build()
            .unwrap();

        let partition_spec_1 = PartitionSpec::builder(schema.clone())
            .with_spec_id(1)
            .add_unbound_field(UnboundPartitionField {
                source_id: 1,
                field_id: None,
                name: "id_bucket".to_string(),
                transform: Transform::Bucket(16),
            })
            .unwrap()
            .build()
            .unwrap();

        let partition_spec_2 = PartitionSpec::builder(schema)
            .with_spec_id(1)
            .add_unbound_field(UnboundPartitionField {
                source_id: 2,
                field_id: None,
                name: "id_bucket".to_string(),
                transform: Transform::Bucket(16),
            })
            .unwrap()
            .build()
            .unwrap();

        assert!(!partition_spec_1.is_compatible_with(&partition_spec_2));
    }

    #[test]
    fn test_not_compatible_with_order_different() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(crate::spec::PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "name",
                    Type::Primitive(crate::spec::PrimitiveType::String),
                )
                .into(),
            ])
            .build()
            .unwrap();

        let partition_spec_1 = PartitionSpec::builder(schema.clone())
            .with_spec_id(1)
            .add_unbound_field(UnboundPartitionField {
                source_id: 1,
                field_id: None,
                name: "id_bucket".to_string(),
                transform: Transform::Bucket(16),
            })
            .unwrap()
            .add_unbound_field(UnboundPartitionField {
                source_id: 2,
                field_id: None,
                name: "name".to_string(),
                transform: Transform::Identity,
            })
            .unwrap()
            .build()
            .unwrap();

        let partition_spec_2 = PartitionSpec::builder(schema)
            .with_spec_id(1)
            .add_unbound_field(UnboundPartitionField {
                source_id: 2,
                field_id: None,
                name: "name".to_string(),
                transform: Transform::Identity,
            })
            .unwrap()
            .add_unbound_field(UnboundPartitionField {
                source_id: 1,
                field_id: None,
                name: "id_bucket".to_string(),
                transform: Transform::Bucket(16),
            })
            .unwrap()
            .build()
            .unwrap();

        assert!(!partition_spec_1.is_compatible_with(&partition_spec_2));
    }

    #[test]
    fn test_highest_field_id_unpartitioned() {
        let spec = PartitionSpec::builder(Schema::builder().with_fields(vec![]).build().unwrap())
            .with_spec_id(1)
            .build()
            .unwrap();

        assert!(spec.highest_field_id().is_none());
    }

    #[test]
    fn test_highest_field_id() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(crate::spec::PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "name",
                    Type::Primitive(crate::spec::PrimitiveType::String),
                )
                .into(),
            ])
            .build()
            .unwrap();

        let spec = PartitionSpec::builder(schema)
            .with_spec_id(1)
            .add_unbound_field(UnboundPartitionField {
                source_id: 1,
                field_id: Some(1001),
                name: "id".to_string(),
                transform: Transform::Identity,
            })
            .unwrap()
            .add_unbound_field(UnboundPartitionField {
                source_id: 2,
                field_id: Some(1000),
                name: "name".to_string(),
                transform: Transform::Identity,
            })
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(Some(1001), spec.highest_field_id());
    }

    #[test]
    fn test_has_sequential_ids() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(crate::spec::PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "name",
                    Type::Primitive(crate::spec::PrimitiveType::String),
                )
                .into(),
            ])
            .build()
            .unwrap();

        let spec = PartitionSpec::builder(schema)
            .with_spec_id(1)
            .add_unbound_field(UnboundPartitionField {
                source_id: 1,
                field_id: Some(1000),
                name: "id".to_string(),
                transform: Transform::Identity,
            })
            .unwrap()
            .add_unbound_field(UnboundPartitionField {
                source_id: 2,
                field_id: Some(1001),
                name: "name".to_string(),
                transform: Transform::Identity,
            })
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(1000, spec.fields[0].field_id);
        assert_eq!(1001, spec.fields[1].field_id);
        assert!(spec.has_sequential_ids());
    }

    #[test]
    fn test_sequential_ids_must_start_at_1000() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(crate::spec::PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "name",
                    Type::Primitive(crate::spec::PrimitiveType::String),
                )
                .into(),
            ])
            .build()
            .unwrap();

        let spec = PartitionSpec::builder(schema)
            .with_spec_id(1)
            .add_unbound_field(UnboundPartitionField {
                source_id: 1,
                field_id: Some(999),
                name: "id".to_string(),
                transform: Transform::Identity,
            })
            .unwrap()
            .add_unbound_field(UnboundPartitionField {
                source_id: 2,
                field_id: Some(1000),
                name: "name".to_string(),
                transform: Transform::Identity,
            })
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(999, spec.fields[0].field_id);
        assert_eq!(1000, spec.fields[1].field_id);
        assert!(!spec.has_sequential_ids());
    }

    #[test]
    fn test_sequential_ids_must_have_no_gaps() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(crate::spec::PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "name",
                    Type::Primitive(crate::spec::PrimitiveType::String),
                )
                .into(),
            ])
            .build()
            .unwrap();

        let spec = PartitionSpec::builder(schema)
            .with_spec_id(1)
            .add_unbound_field(UnboundPartitionField {
                source_id: 1,
                field_id: Some(1000),
                name: "id".to_string(),
                transform: Transform::Identity,
            })
            .unwrap()
            .add_unbound_field(UnboundPartitionField {
                source_id: 2,
                field_id: Some(1002),
                name: "name".to_string(),
                transform: Transform::Identity,
            })
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(1000, spec.fields[0].field_id);
        assert_eq!(1002, spec.fields[1].field_id);
        assert!(!spec.has_sequential_ids());
    }

    #[test]
    fn test_partition_to_path() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(3, "timestamp", Type::Primitive(PrimitiveType::Timestamp))
                    .into(),
                NestedField::required(4, "empty", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let spec = PartitionSpec::builder(schema.clone())
            .add_partition_field("id", "id", Transform::Identity)
            .unwrap()
            .add_partition_field("name", "name", Transform::Identity)
            .unwrap()
            .add_partition_field("timestamp", "ts_hour", Transform::Hour)
            .unwrap()
            .add_partition_field("empty", "empty_void", Transform::Void)
            .unwrap()
            .build()
            .unwrap();

        let data = Struct::from_iter([
            Some(Literal::int(42)),
            Some(Literal::string("alice")),
            Some(Literal::int(1000)),
            Some(Literal::string("empty")),
        ]);

        assert_eq!(
            spec.partition_to_path(&data, schema.into()),
            "id=42/name=alice/ts_hour=1000/empty_void=null"
        );
    }
}
