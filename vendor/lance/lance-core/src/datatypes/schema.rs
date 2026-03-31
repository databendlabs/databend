// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Schema

use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::{self, Debug, Formatter},
    sync::Arc,
};

use arrow_array::RecordBatch;
use arrow_schema::{Field as ArrowField, Schema as ArrowSchema};
use deepsize::DeepSizeOf;
use lance_arrow::*;
use snafu::location;

use super::field::{BlobVersion, Field, OnTypeMismatch, SchemaCompareOptions};
use crate::{Error, Result, ROW_ADDR, ROW_ADDR_FIELD, ROW_ID, ROW_ID_FIELD, WILDCARD};

/// Lance Schema.
#[derive(Default, Debug, Clone, DeepSizeOf)]
pub struct Schema {
    /// Top-level fields in the dataset.
    pub fields: Vec<Field>,
    /// Metadata of the schema
    pub metadata: HashMap<String, String>,
}

/// Reference to a field in a schema, either by ID or by path.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FieldRef<'a> {
    /// Reference by field ID
    ById(i32),
    /// Reference by field path (e.g., "struct_field.sub_field")
    ByPath(&'a str),
}

impl FieldRef<'_> {
    /// Convert this field reference to a field ID by looking it up in the schema.
    pub fn into_id(self, schema: &Schema) -> Result<i32> {
        match self {
            FieldRef::ById(id) => {
                if schema.field_by_id(id).is_none() {
                    return Err(Error::InvalidInput {
                        source: format!("Field ID {} not found in schema", id).into(),
                        location: location!(),
                    });
                }
                Ok(id)
            }
            FieldRef::ByPath(path) => {
                let field = schema.field(path).ok_or_else(|| Error::InvalidInput {
                    source: format!("Field '{}' not found in schema", path).into(),
                    location: location!(),
                })?;
                Ok(field.id)
            }
        }
    }
}

impl From<i32> for FieldRef<'_> {
    fn from(id: i32) -> Self {
        FieldRef::ById(id)
    }
}

impl<'a> From<&'a str> for FieldRef<'a> {
    fn from(path: &'a str) -> Self {
        FieldRef::ByPath(path)
    }
}

impl<'a> From<&'a String> for FieldRef<'a> {
    fn from(path: &'a String) -> Self {
        FieldRef::ByPath(path.as_str())
    }
}

/// State for a pre-order DFS iterator over the fields of a schema.
struct SchemaFieldIterPreOrder<'a> {
    field_stack: Vec<&'a Field>,
}

impl<'a> SchemaFieldIterPreOrder<'a> {
    #[allow(dead_code)]
    fn new(schema: &'a Schema) -> Self {
        let mut field_stack = Vec::with_capacity(schema.fields.len() * 2);
        for field in schema.fields.iter().rev() {
            field_stack.push(field);
        }
        Self { field_stack }
    }
}

/// Iterator implementation for a pre-order traversal of fields
impl<'a> Iterator for SchemaFieldIterPreOrder<'a> {
    type Item = &'a Field;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(next_field) = self.field_stack.pop() {
            for child in next_field.children.iter().rev() {
                self.field_stack.push(child);
            }
            Some(next_field)
        } else {
            None
        }
    }
}

impl Schema {
    /// The unenforced primary key fields in the schema
    pub fn unenforced_primary_key(&self) -> Vec<&Field> {
        self.fields_pre_order()
            .filter(|f| f.unenforced_primary_key)
            .collect::<Vec<_>>()
    }

    pub fn compare_with_options(&self, expected: &Self, options: &SchemaCompareOptions) -> bool {
        compare_fields(&self.fields, &expected.fields, options)
            && (!options.compare_metadata || self.metadata == expected.metadata)
    }

    pub fn explain_difference(
        &self,
        expected: &Self,
        options: &SchemaCompareOptions,
    ) -> Option<String> {
        let mut differences =
            explain_fields_difference(&self.fields, &expected.fields, options, None);

        if options.compare_metadata {
            if let Some(difference) =
                explain_metadata_difference(&self.metadata, &expected.metadata)
            {
                differences.push(difference);
            }
        }

        if differences.is_empty() {
            None
        } else {
            Some(differences.join(", "))
        }
    }

    pub fn has_dictionary_types(&self) -> bool {
        self.fields.iter().any(|f| f.has_dictionary_types())
    }

    pub fn check_compatible(&self, expected: &Self, options: &SchemaCompareOptions) -> Result<()> {
        if !self.compare_with_options(expected, options) {
            let difference = self.explain_difference(expected, options);
            Err(Error::SchemaMismatch {
                // unknown reason is messy but this shouldn't happen.
                difference: difference.unwrap_or("unknown reason".to_string()),
                location: location!(),
            })
        } else {
            Ok(())
        }
    }

    /// Convert to a compact string representation.
    ///
    /// This is intended for display purposes and not for serialization.
    pub fn to_compact_string(&self, indent: Indentation) -> String {
        ArrowSchema::from(self).to_compact_string(indent)
    }

    /// Given a string column reference, resolve the path of fields
    ///
    /// For example, given a.b.c we will return the fields [a, b, c]
    /// Field names containing dots must be quoted: parent."child.with.dot"
    ///
    /// Returns None if we can't find a segment at any point
    pub fn resolve(&self, column: impl AsRef<str>) -> Option<Vec<&Field>> {
        let split = parse_field_path(column.as_ref()).ok()?;
        if split.is_empty() {
            return None;
        }

        if split.len() == 1 {
            let field_name = &split[0];
            if let Some(field) = self.fields.iter().find(|f| &f.name == field_name) {
                return Some(vec![field]);
            }
            return None;
        }

        // Multiple segments - resolve as a nested field path
        let mut fields = Vec::with_capacity(split.len());
        let first = &split[0];

        // Find the first field
        let field = self.fields.iter().find(|f| &f.name == first)?;

        let mut split_refs: VecDeque<&str> = split[1..].iter().map(|s| s.as_str()).collect();
        if field.resolve(&mut split_refs, &mut fields) {
            Some(fields)
        } else {
            None
        }
    }

    fn do_project<T: AsRef<str>>(&self, columns: &[T], err_on_missing: bool) -> Result<Self> {
        let mut candidates: Vec<Field> = vec![];
        for col in columns {
            let split = parse_field_path(col.as_ref())?;
            let first = split[0].as_str();
            if let Some(field) = self.field(first) {
                let split_refs: Vec<&str> = split[1..].iter().map(|s| s.as_str()).collect();
                let projected_field = field.project(&split_refs)?;
                if let Some(candidate_field) = candidates.iter_mut().find(|f| f.name == first) {
                    candidate_field.merge(&projected_field)?;
                } else {
                    candidates.push(projected_field)
                }
            } else if err_on_missing && first != ROW_ID && first != ROW_ADDR {
                return Err(Error::Schema {
                    message: format!("Column {} does not exist", col.as_ref()),
                    location: location!(),
                });
            }
        }

        Ok(Self {
            fields: candidates,
            metadata: self.metadata.clone(),
        })
    }

    /// Project the columns over the schema.
    ///
    /// ```ignore
    /// let schema = Schema::from(...);
    /// let projected = schema.project(&["col1", "col2.sub_col3.field4"])?;
    /// ```
    pub fn project<T: AsRef<str>>(&self, columns: &[T]) -> Result<Self> {
        self.do_project(columns, true)
    }

    /// Project the columns over the schema, dropping unrecognized columns
    pub fn project_or_drop<T: AsRef<str>>(&self, columns: &[T]) -> Result<Self> {
        self.do_project(columns, false)
    }

    /// Check that the top level fields don't contain `.` in their names
    /// to distinguish from nested fields.
    // TODO: pub(crate)
    pub fn validate(&self) -> Result<()> {
        let mut seen_names = HashSet::new();

        for field in self.fields.iter() {
            if field.name.contains('.') {
                return Err(Error::Schema{message:format!(
                    "Top level field {} cannot contain `.`. Maybe you meant to create a struct field?",
                    field.name.clone()
                ), location: location!(),});
            }

            let column_path = self
                .field_ancestry_by_id(field.id)
                .unwrap()
                .iter()
                .map(|f| f.name.as_str())
                .collect::<Vec<_>>()
                .join(".");
            if !seen_names.insert(column_path.clone()) {
                return Err(Error::Schema {
                    message: format!(
                        "Duplicate field name \"{}\" in schema:\n {:#?}",
                        column_path, self
                    ),
                    location: location!(),
                });
            }
        }

        // Check for duplicate field ids
        let mut seen_ids = HashSet::new();
        for field in self.fields_pre_order() {
            if field.id < 0 {
                return Err(Error::Schema {
                    message: format!("Field {} has a negative id {}", field.name, field.id),
                    location: location!(),
                });
            }
            if !seen_ids.insert(field.id) {
                return Err(Error::Schema {
                    message: format!("Duplicate field id {} in schema {:?}", field.id, self),
                    location: location!(),
                });
            }
        }

        Ok(())
    }

    /// Intersection between two [`Schema`].
    pub fn intersection(&self, other: &Self) -> Result<Self> {
        self.do_intersection(other, false)
    }

    /// Intersection between two [`Schema`], ignoring data types.
    pub fn intersection_ignore_types(&self, other: &Self) -> Result<Self> {
        self.do_intersection(other, true)
    }

    fn do_intersection(&self, other: &Self, ignore_types: bool) -> Result<Self> {
        let mut candidates: Vec<Field> = vec![];
        for field in other.fields.iter() {
            if let Some(candidate_field) = self.field(&field.name) {
                candidates.push(candidate_field.do_intersection(field, ignore_types)?);
            }
        }

        Ok(Self {
            fields: candidates,
            metadata: self.metadata.clone(),
        })
    }

    /// Iterates over the fields using a pre-order traversal
    ///
    /// This is a DFS traversal where the parent is visited
    /// before its children
    pub fn fields_pre_order(&self) -> impl Iterator<Item = &Field> {
        SchemaFieldIterPreOrder::new(self)
    }

    /// Returns a new schema that only contains the fields in `column_ids`.
    ///
    /// This projection can filter out both top-level and nested fields
    ///
    /// If `include_all_children` is true, then if a parent field id is passed,
    /// then all children of that field will be included in the projection
    /// regardless of whether their ids were passed. If this is false, then
    /// only the child fields with the passed ids will be included.
    pub fn project_by_ids(&self, column_ids: &[i32], include_all_children: bool) -> Self {
        let filtered_fields = self
            .fields
            .iter()
            .filter_map(|f| f.project_by_ids(column_ids, include_all_children))
            .collect();
        Self {
            fields: filtered_fields,
            metadata: self.metadata.clone(),
        }
    }

    fn apply_projection(&self, projection: &Projection) -> Self {
        let filtered_fields = self
            .fields
            .iter()
            .filter_map(|f| f.apply_projection(projection))
            .collect();
        Self {
            fields: filtered_fields,
            metadata: self.metadata.clone(),
        }
    }

    /// Project the schema by another schema, and preserves field metadata, i.e., Field IDs.
    ///
    /// Parameters
    /// - `projection`: The schema to project by. Can be [`arrow_schema::Schema`] or [`Schema`].
    pub fn project_by_schema<S: TryInto<Self, Error = Error>>(
        &self,
        projection: S,
        on_missing: OnMissing,
        on_type_mismatch: OnTypeMismatch,
    ) -> Result<Self> {
        let projection = projection.try_into()?;
        let mut new_fields = vec![];
        for field in projection.fields.iter() {
            // Ensure the field is a top-level field (no dots in the name)
            if field.name.contains('.') {
                return Err(Error::Schema {
                    message: format!(
                        "Field '{}' contains dots. project_by_schema only accepts top-level fields. \
                         Use project() method for nested field paths.",
                        field.name
                    ),
                    location: location!(),
                });
            }

            if let Some(self_field) = self.field(&field.name) {
                new_fields.push(self_field.project_by_field(field, on_type_mismatch)?);
            } else if matches!(on_missing, OnMissing::Error) {
                return Err(Error::Schema {
                    message: format!("Field {} not found", field.name),
                    location: location!(),
                });
            }
        }
        Ok(Self {
            fields: new_fields,
            metadata: self.metadata.clone(),
        })
    }

    /// Exclude the fields from `other` Schema, and returns a new Schema.
    pub fn exclude<T: TryInto<Self> + Debug>(&self, schema: T) -> Result<Self> {
        let other = schema.try_into().map_err(|_| Error::Schema {
            message: "The other schema is not compatible with this schema".to_string(),
            location: location!(),
        })?;
        let mut fields = vec![];
        for field in self.fields.iter() {
            if let Some(other_field) = other.field(&field.name) {
                if field.data_type().is_struct() {
                    if let Some(f) = field.exclude(other_field) {
                        fields.push(f)
                    }
                }
            } else {
                fields.push(field.clone());
            }
        }
        Ok(Self {
            fields,
            metadata: self.metadata.clone(),
        })
    }

    /// Get a field by its path. Return `None` if the field does not exist.
    /// Field names containing dots must be quoted: parent."child.with.dot"
    pub fn field(&self, name: &str) -> Option<&Field> {
        self.resolve(name).and_then(|fields| fields.last().copied())
    }

    /// Get a field by its path, with case-insensitive matching.
    ///
    /// This first tries an exact match, then falls back to case-insensitive matching.
    /// Returns the actual field from the schema (preserving original case).
    /// Field names containing dots must be quoted: parent."child.with.dot"
    pub fn field_case_insensitive(&self, name: &str) -> Option<&Field> {
        self.resolve_case_insensitive(name)
            .and_then(|fields| fields.last().copied())
    }

    /// Given a string column reference, resolve the path of fields with case-insensitive matching.
    ///
    /// This first tries an exact match, then falls back to case-insensitive matching.
    /// Returns the actual fields from the schema (preserving original case).
    pub fn resolve_case_insensitive(&self, column: impl AsRef<str>) -> Option<Vec<&Field>> {
        let split = parse_field_path(column.as_ref()).ok()?;
        if split.is_empty() {
            return None;
        }

        if split.len() == 1 {
            let field_name = &split[0];
            // Try exact match first
            if let Some(field) = self.fields.iter().find(|f| &f.name == field_name) {
                return Some(vec![field]);
            }
            // Fall back to case-insensitive match
            if let Some(field) = self
                .fields
                .iter()
                .find(|f| f.name.eq_ignore_ascii_case(field_name))
            {
                return Some(vec![field]);
            }
            return None;
        }

        // Multiple segments - resolve as a nested field path
        let mut fields = Vec::with_capacity(split.len());
        let first = &split[0];

        // Find the first field (try exact match, then case-insensitive)
        let field = self.fields.iter().find(|f| &f.name == first).or_else(|| {
            self.fields
                .iter()
                .find(|f| f.name.eq_ignore_ascii_case(first))
        })?;

        let mut split_refs: VecDeque<&str> = split[1..].iter().map(|s| s.as_str()).collect();
        if field.resolve_case_insensitive(&mut split_refs, &mut fields) {
            Some(fields)
        } else {
            None
        }
    }

    // TODO: This is not a public API, change to pub(crate) after refactor is done.
    pub fn field_id(&self, column: &str) -> Result<i32> {
        self.field(column)
            .map(|f| f.id)
            .ok_or_else(|| Error::Schema {
                message: "Vector column not in schema".to_string(),
                location: location!(),
            })
    }

    pub fn top_level_field_ids(&self) -> Vec<i32> {
        self.fields.iter().map(|f| f.id).collect()
    }

    // Recursively collect all the field IDs, in pre-order traversal order.
    // TODO: pub(crate)
    pub fn field_ids(&self) -> Vec<i32> {
        self.fields_pre_order().map(|f| f.id).collect()
    }

    /// Get field by its id.
    pub fn field_by_id_mut(&mut self, id: impl Into<i32>) -> Option<&mut Field> {
        let id = id.into();
        for field in self.fields.iter_mut() {
            if field.id == id {
                return Some(field);
            }
            if let Some(grandchild) = field.field_by_id_mut(id) {
                return Some(grandchild);
            }
        }
        None
    }

    pub fn field_by_id(&self, id: impl Into<i32>) -> Option<&Field> {
        let id = id.into();
        for field in self.fields.iter() {
            if field.id == id {
                return Some(field);
            }
            if let Some(grandchild) = field.field_by_id(id) {
                return Some(grandchild);
            }
        }
        None
    }

    /// Get the sequence of fields from the root to the field with the given id.
    pub fn field_ancestry_by_id(&self, id: i32) -> Option<Vec<&Field>> {
        let mut to_visit = self.fields.iter().map(|f| vec![f]).collect::<Vec<_>>();
        while let Some(path) = to_visit.pop() {
            let field = path.last().unwrap();
            if field.id == id {
                return Some(path);
            }
            for child in field.children.iter() {
                let mut new_path = path.clone();
                new_path.push(child);
                to_visit.push(new_path);
            }
        }
        None
    }

    pub fn mut_field_by_id(&mut self, id: impl Into<i32>) -> Option<&mut Field> {
        let id = id.into();
        for field in self.fields.as_mut_slice() {
            if field.id == id {
                return Some(field);
            }
            if let Some(grandchild) = field.mut_field_by_id(id) {
                return Some(grandchild);
            }
        }
        None
    }

    // TODO: pub(crate)
    /// Get the maximum field id in the schema.
    ///
    /// Note: When working with Datasets, you should prefer [Manifest::max_field_id()]
    /// over this method. This method does not take into account the field IDs
    /// of dropped fields.
    pub fn max_field_id(&self) -> Option<i32> {
        self.fields.iter().map(|f| f.max_id()).max()
    }

    /// Recursively attach set up dictionary values to the dictionary fields.
    // TODO: pub(crate)
    pub fn set_dictionary(&mut self, batch: &RecordBatch) -> Result<()> {
        for field in self.fields.as_mut_slice() {
            let column = batch
                .column_by_name(&field.name)
                .ok_or_else(|| Error::Schema {
                    message: format!("column '{}' does not exist in the record batch", field.name),
                    location: location!(),
                })?;
            field.set_dictionary(column);
        }
        Ok(())
    }

    /// Walk through the fields and assign a new field id to each field that does
    /// not have one (e.g. is set to -1)
    ///
    /// If this schema is on an existing dataset, pass the result of
    /// `Manifest::max_field_id` to `max_existing_id`. If for some reason that
    /// id is lower than the maximum field id in this schema, the field IDs will
    /// be reassigned starting from the maximum field id in this schema.
    ///
    /// If this schema is not associated with a dataset, pass `None` to
    /// `max_existing_id`. This is the same as passing [Self::max_field_id()].
    pub fn set_field_id(&mut self, max_existing_id: Option<i32>) {
        let schema_max_id = self.max_field_id().unwrap_or(-1);
        let max_existing_id = max_existing_id.unwrap_or(-1);
        let mut current_id = schema_max_id.max(max_existing_id) + 1;
        self.fields
            .iter_mut()
            .for_each(|f| f.set_id(-1, &mut current_id));
    }

    fn reset_id(&mut self) {
        self.fields.iter_mut().for_each(|f| f.reset_id());
    }

    /// Create a new schema by adding fields to the end of this schema
    pub fn extend(&mut self, fields: &[ArrowField]) -> Result<()> {
        let new_fields = fields
            .iter()
            .map(Field::try_from)
            .collect::<Result<Vec<_>>>()?;
        self.fields.extend(new_fields);
        // Validate this addition does not create any duplicate field names
        let field_names = self.fields.iter().map(|f| &f.name).collect::<HashSet<_>>();
        if field_names.len() != self.fields.len() {
            Err(Error::Internal {
                message: format!(
                    "Attempt to add fields [{:?}] would lead to duplicate field names",
                    fields.iter().map(|f| f.name()).collect::<Vec<_>>()
                ),
                location: location!(),
            })
        } else {
            Ok(())
        }
    }

    /// Merge this schema from the other schema.
    ///
    /// After merging, the field IDs from `other` schema will be reassigned,
    /// following the fields in `self`.
    pub fn merge<S: TryInto<Self, Error = Error>>(&self, other: S) -> Result<Self> {
        let mut other: Self = other.try_into()?;
        other.reset_id();

        let mut merged_fields: Vec<Field> = vec![];
        for mut field in self.fields.iter().cloned() {
            if let Some(other_field) = other.field(&field.name) {
                // if both are struct types, then merge the fields
                field.merge(other_field)?;
            }
            merged_fields.push(field);
        }

        // we already checked for overlap so just need to add new top-level fields
        // in the incoming schema
        for field in other.fields.as_slice() {
            if !merged_fields.iter().any(|f| f.name == field.name) {
                merged_fields.push(field.clone());
            }
        }
        let metadata = self
            .metadata
            .iter()
            .chain(other.metadata.iter())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let schema = Self {
            fields: merged_fields,
            metadata,
        };
        Ok(schema)
    }

    pub fn all_fields_nullable(&self) -> bool {
        SchemaFieldIterPreOrder::new(self).all(|f| f.nullable)
    }

    /// Returns the properly formatted path from root to the field.
    /// Field names containing dots are quoted (e.g., struct.`field.with.dot`)
    pub fn field_path(&self, field_id: i32) -> Result<String> {
        self.field_ancestry_by_id(field_id)
            .map(|ancestry| {
                let field_refs: Vec<&str> = ancestry.iter().map(|f| f.name.as_str()).collect();
                format_field_path(&field_refs)
            })
            .ok_or_else(|| Error::Index {
                message: format!("Could not find field ancestry for id {}", field_id),
                location: location!(),
            })
    }
}

impl PartialEq for Schema {
    fn eq(&self, other: &Self) -> bool {
        self.fields == other.fields
    }
}

impl fmt::Display for Schema {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for field in self.fields.iter() {
            writeln!(f, "{field}")?
        }
        Ok(())
    }
}

/// Convert `arrow2::datatype::Schema` to Lance
impl TryFrom<&ArrowSchema> for Schema {
    type Error = Error;

    fn try_from(schema: &ArrowSchema) -> Result<Self> {
        let mut schema = Self {
            fields: schema
                .fields
                .iter()
                .map(|f| Field::try_from(f.as_ref()))
                .collect::<Result<_>>()?,
            metadata: schema.metadata.clone(),
        };
        schema.set_field_id(None);
        schema.validate()?;

        let pk = schema.unenforced_primary_key();
        for pk_col in pk.into_iter() {
            if !pk_col.is_leaf() {
                return Err(Error::Schema {
                    message: format!("Primary key column must be a leaf: {}", pk_col),
                    location: location!(),
                });
            }

            if let Some(ancestors) = schema.field_ancestry_by_id(pk_col.id) {
                for ancestor in ancestors {
                    if ancestor.nullable {
                        return Err(Error::Schema {
                            message: format!(
                                "Primary key column and all its ancestors must not be nullable: {}",
                                ancestor
                            ),
                            location: location!(),
                        });
                    }

                    if ancestor.logical_type.is_list() || ancestor.logical_type.is_large_list() {
                        return Err(Error::Schema {
                            message: format!(
                                "Primary key column must not be in a list type: {}",
                                ancestor
                            ),
                            location: location!(),
                        });
                    }
                }
            }
        }

        Ok(schema)
    }
}

/// Convert Lance Schema to Arrow Schema
impl From<&Schema> for ArrowSchema {
    fn from(schema: &Schema) -> Self {
        Self {
            fields: schema.fields.iter().map(ArrowField::from).collect(),
            metadata: schema.metadata.clone(),
        }
    }
}

/// Make API cleaner to accept both [`Schema`] and Arrow Schema.
impl TryFrom<&Self> for Schema {
    type Error = Error;

    fn try_from(schema: &Self) -> Result<Self> {
        Ok(schema.clone())
    }
}

pub fn compare_fields(
    fields: &[Field],
    expected: &[Field],
    options: &SchemaCompareOptions,
) -> bool {
    if options.allow_missing_if_nullable || options.ignore_field_order || options.allow_subschema {
        let expected_names = expected
            .iter()
            .map(|f| f.name.as_str())
            .collect::<HashSet<_>>();
        for field in fields {
            if !expected_names.contains(field.name.as_str()) {
                // Extra field
                return false;
            }
        }

        let field_mapping = fields
            .iter()
            .enumerate()
            .map(|(pos, f)| (f.name.as_str(), (f, pos)))
            .collect::<HashMap<_, _>>();
        let mut cumulative_position = 0;
        for expected_field in expected {
            if let Some((field, pos)) = field_mapping.get(expected_field.name.as_str()) {
                if !field.compare_with_options(expected_field, options) {
                    return false;
                }
                if !options.ignore_field_order && *pos < cumulative_position {
                    return false;
                }
                cumulative_position = *pos;
            } else if options.allow_subschema {
                // allow_subschema: allow missing any field
                continue;
            } else if options.allow_missing_if_nullable && expected_field.nullable {
                continue;
            } else {
                return false;
            }
        }
        true
    } else {
        // Fast path: we can just zip
        fields.len() == expected.len()
            && fields
                .iter()
                .zip(expected.iter())
                .all(|(lhs, rhs)| lhs.compare_with_options(rhs, options))
    }
}

pub fn explain_fields_difference(
    fields: &[Field],
    expected: &[Field],
    options: &SchemaCompareOptions,
    path: Option<&str>,
) -> Vec<String> {
    let field_names = fields
        .iter()
        .map(|f| f.name.as_str())
        .collect::<HashSet<_>>();
    let expected_names = expected
        .iter()
        .map(|f| f.name.as_str())
        .collect::<HashSet<_>>();

    let prepend_path = |f: &str| {
        if let Some(path) = path {
            format!("{}.{}", path, f)
        } else {
            f.to_string()
        }
    };

    // Check there are no extra fields or missing fields
    let unexpected_fields = field_names
        .difference(&expected_names)
        .cloned()
        .map(prepend_path)
        .collect::<Vec<_>>();
    let missing_fields = expected_names.difference(&field_names);
    let missing_fields = if options.allow_subschema {
        // allow_subschema: don't report any missing fields
        Vec::new()
    } else if options.allow_missing_if_nullable {
        missing_fields
            .filter(|f| {
                let expected_field = expected.iter().find(|ef| ef.name == **f).unwrap();
                !expected_field.nullable
            })
            .cloned()
            .map(prepend_path)
            .collect::<Vec<_>>()
    } else {
        missing_fields
            .cloned()
            .map(prepend_path)
            .collect::<Vec<_>>()
    };

    let mut differences = vec![];
    if !missing_fields.is_empty() || !unexpected_fields.is_empty() {
        differences.push(format!(
            "fields did not match, missing=[{}], unexpected=[{}]",
            missing_fields.join(", "),
            unexpected_fields.join(", ")
        ));
    }

    // Map the expected fields to position of field
    let field_mapping = expected
        .iter()
        .filter_map(|ef| {
            fields
                .iter()
                .position(|f| ef.name == f.name)
                .map(|pos| (ef, pos))
        })
        .collect::<Vec<_>>();

    // Check the fields are in the same order
    if !options.ignore_field_order {
        let fields_out_of_order = field_mapping.windows(2).any(|w| w[0].1 > w[1].1);
        if fields_out_of_order {
            let expected_order = expected.iter().map(|f| f.name.as_str()).collect::<Vec<_>>();
            let actual_order = fields.iter().map(|f| f.name.as_str()).collect::<Vec<_>>();
            differences.push(format!(
                "fields in different order, expected: [{}], actual: [{}]",
                expected_order.join(", "),
                actual_order.join(", ")
            ));
        }
    }

    // Check for individual differences in the fields
    for (expected_field, field_pos) in field_mapping.iter() {
        let field = &fields[*field_pos];
        debug_assert_eq!(field.name, expected_field.name);
        let field_diffs = field.explain_differences(expected_field, options, path);
        if !field_diffs.is_empty() {
            differences.push(field_diffs.join(", "))
        }
    }

    differences
}

fn explain_metadata_difference(
    metadata: &HashMap<String, String>,
    expected: &HashMap<String, String>,
) -> Option<String> {
    if metadata != expected {
        Some(format!(
            "metadata did not match, expected: {:?}, actual: {:?}",
            expected, metadata
        ))
    } else {
        None
    }
}

/// What to do when a column is missing in the schema
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnMissing {
    Error,
    Ignore,
}

/// A trait for something that we can project fields from.
pub trait Projectable: Debug + Send + Sync {
    fn schema(&self) -> &Schema;
}

impl Projectable for Schema {
    fn schema(&self) -> &Schema {
        self
    }
}

/// Specifies how to handle blob columns when projecting
#[derive(Debug, Clone, Default)]
pub enum BlobHandling {
    /// Read all blobs as binary
    AllBinary,
    #[default]
    /// Read all blobs as descriptions and other binary columns as binary
    BlobsDescriptions,
    /// Read all binary columns as descriptions
    AllDescriptions,
    /// Read specific blobs as binary and the rest as descriptions
    ///
    /// Non-blob binary columns will be read as binary
    ///
    /// The set contains the field ids that should be read as binary
    SomeBlobsBinary(HashSet<u32>),
    /// Read specific columns as binary and all other binary columns as descriptions
    ///
    /// The set contains the field ids that should be read as binary
    SomeBinary(HashSet<u32>),
}

impl BlobHandling {
    fn should_unload(&self, field: &Field) -> bool {
        if !field.data_type().is_binary_like() {
            return false;
        }
        match self {
            Self::AllBinary => false,
            Self::BlobsDescriptions => field.is_blob(),
            Self::AllDescriptions => true,
            Self::SomeBlobsBinary(set) => field.is_blob() && !set.contains(&(field.id as u32)),
            Self::SomeBinary(set) => !set.contains(&(field.id as u32)),
        }
    }

    pub fn unload_if_needed(&self, field: Field, version: BlobVersion) -> Field {
        if self.should_unload(&field) {
            field.into_unloaded_with_version(version)
        } else {
            field
        }
    }
}

/// A projection is a selection of fields in a schema
///
/// In addition we record whether the row_id or row_addr are
/// selected (these fields have no field id)
#[derive(Clone)]
pub struct Projection {
    base: Arc<dyn Projectable>,
    pub field_ids: HashSet<i32>,
    pub with_row_id: bool,
    pub with_row_addr: bool,
    pub with_row_last_updated_at_version: bool,
    pub with_row_created_at_version: bool,
    pub blob_handling: BlobHandling,
    pub blob_version: BlobVersion,
}

impl Debug for Projection {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Projection")
            .field("field_ids", &self.field_ids)
            .field("with_row_id", &self.with_row_id)
            .field("with_row_addr", &self.with_row_addr)
            .field(
                "with_row_last_updated_at_version",
                &self.with_row_last_updated_at_version,
            )
            .field(
                "with_row_created_at_version",
                &self.with_row_created_at_version,
            )
            .field("blob_handling", &self.blob_handling)
            .field("blob_version", &self.blob_version)
            .finish()
    }
}

impl Projection {
    /// Create a new empty projection
    pub fn empty(base: Arc<dyn Projectable>) -> Self {
        Self {
            base,
            field_ids: HashSet::new(),
            with_row_id: false,
            with_row_addr: false,
            with_row_last_updated_at_version: false,
            with_row_created_at_version: false,
            blob_handling: BlobHandling::default(),
            blob_version: BlobVersion::V1,
        }
    }

    pub fn full(base: Arc<dyn Projectable>) -> Self {
        let schema = base.schema().clone();
        Self::empty(base).union_schema(&schema)
    }

    pub fn with_row_id(mut self) -> Self {
        self.with_row_id = true;
        self
    }

    pub fn with_row_addr(mut self) -> Self {
        self.with_row_addr = true;
        self
    }

    pub fn with_row_last_updated_at_version(mut self) -> Self {
        self.with_row_last_updated_at_version = true;
        self
    }

    pub fn with_row_created_at_version(mut self) -> Self {
        self.with_row_created_at_version = true;
        self
    }

    pub fn with_blob_handling(mut self, blob_handling: BlobHandling) -> Self {
        self.blob_handling = blob_handling;
        self
    }

    pub fn with_blob_version(mut self, blob_version: BlobVersion) -> Self {
        self.blob_version = blob_version;
        self
    }

    fn add_field_children(field_ids: &mut HashSet<i32>, field: &Field) {
        for child in &field.children {
            field_ids.insert(child.id);
            Self::add_field_children(field_ids, child);
        }
    }

    /// Add a column to the projection from a string reference
    ///
    /// The string reference can be a dotted field path (x.y.z) to reference inner struct fields
    ///
    /// Parent fields will automatically be added.  If the specified field has any children then
    /// those will be added to.  Siblings, aunts, etc. are not automatically added
    pub fn union_column(mut self, column: impl AsRef<str>, on_missing: OnMissing) -> Result<Self> {
        let column = column.as_ref();
        if column == ROW_ID {
            self.with_row_id = true;
            return Ok(self);
        } else if column == ROW_ADDR {
            self.with_row_addr = true;
            return Ok(self);
        } else if column == crate::ROW_LAST_UPDATED_AT_VERSION {
            self.with_row_last_updated_at_version = true;
            return Ok(self);
        } else if column == crate::ROW_CREATED_AT_VERSION {
            self.with_row_created_at_version = true;
            return Ok(self);
        }

        if let Some(fields) = self.base.schema().resolve(column) {
            self.field_ids.extend(fields.iter().map(|f| f.id));
            if let Some(last_field) = fields.last() {
                Self::add_field_children(&mut self.field_ids, last_field);
            }
        } else if matches!(on_missing, OnMissing::Error) {
            return Err(Error::InvalidInput {
                source: format!("Column {} does not exist", column).into(),
                location: location!(),
            });
        }
        Ok(self)
    }

    /// True if the projection selects the given field id
    pub fn contains_field_id(&self, id: i32) -> bool {
        self.field_ids.contains(&id)
    }

    /// True if the projection selects fields other than the row id / addr
    pub fn has_data_fields(&self) -> bool {
        !self.field_ids.is_empty()
    }

    /// Add multiple columns (and their parents) to the projection
    pub fn union_columns(
        mut self,
        columns: impl IntoIterator<Item = impl AsRef<str>>,
        on_missing: OnMissing,
    ) -> Result<Self> {
        for column in columns {
            self = self.union_column(column, on_missing)?;
        }
        Ok(self)
    }

    /// Adds all fields from the base schema satisfying a predicate
    pub fn union_predicate(mut self, predicate: impl Fn(&Field) -> bool) -> Self {
        for field in self.base.schema().fields_pre_order() {
            if predicate(field) {
                self.field_ids.insert(field.id);
            }
        }
        self
    }

    /// Removes all fields in the base schema satisfying a predicate
    pub fn subtract_predicate(mut self, predicate: impl Fn(&Field) -> bool) -> Self {
        for field in self.base.schema().fields_pre_order() {
            if predicate(field) {
                self.field_ids.remove(&field.id);
            }
        }
        self
    }

    /// Creates a new projection that is the intersection of this projection and another
    pub fn intersect(mut self, other: &Self) -> Self {
        self.field_ids = HashSet::from_iter(self.field_ids.intersection(&other.field_ids).copied());
        self.with_row_id = self.with_row_id && other.with_row_id;
        self.with_row_addr = self.with_row_addr && other.with_row_addr;
        self.with_row_last_updated_at_version =
            self.with_row_last_updated_at_version && other.with_row_last_updated_at_version;
        self.with_row_created_at_version =
            self.with_row_created_at_version && other.with_row_created_at_version;
        self
    }

    /// Adds all fields from the provided schema to the projection
    ///
    /// Fields are only added if they exist in the base schema, otherwise they
    /// are ignored.
    ///
    /// Will panic if a field in the given schema has a non-negative id and is not in the base schema.
    pub fn union_schema(mut self, other: &Schema) -> Self {
        for field in other.fields_pre_order() {
            if field.id >= 0 {
                self.field_ids.insert(field.id);
            } else if field.name == ROW_ID {
                self.with_row_id = true;
            } else if field.name == ROW_ADDR {
                self.with_row_addr = true;
            } else if field.name == crate::ROW_LAST_UPDATED_AT_VERSION {
                self.with_row_last_updated_at_version = true;
            } else if field.name == crate::ROW_CREATED_AT_VERSION {
                self.with_row_created_at_version = true;
            } else {
                // If a field is not in our schema then it should probably have an id of -1.  If it isn't -1
                // that probably implies some kind of weird schema mixing is going on and we should panic.
                debug_assert_eq!(field.id, -1);
            }
        }
        self
    }

    /// Creates a new projection that is the union of this projection and another
    pub fn union_projection(mut self, other: &Self) -> Self {
        self.field_ids.extend(&other.field_ids);
        self.with_row_id = self.with_row_id || other.with_row_id;
        self.with_row_addr = self.with_row_addr || other.with_row_addr;
        self.with_row_last_updated_at_version =
            self.with_row_last_updated_at_version || other.with_row_last_updated_at_version;
        self.with_row_created_at_version =
            self.with_row_created_at_version || other.with_row_created_at_version;
        self
    }

    /// Adds all fields from the given schema to the projection
    ///
    /// on_missing controls what happen to fields that are not in the base schema
    ///
    /// Name based matching is used to determine if a field is in the base schema.
    pub fn union_arrow_schema(
        mut self,
        other: &ArrowSchema,
        on_missing: OnMissing,
    ) -> Result<Self> {
        self.with_row_id |= other.fields().iter().any(|f| f.name() == ROW_ID);
        self.with_row_addr |= other.fields().iter().any(|f| f.name() == ROW_ADDR);
        self.with_row_last_updated_at_version |= other
            .fields()
            .iter()
            .any(|f| f.name() == crate::ROW_LAST_UPDATED_AT_VERSION);
        self.with_row_created_at_version |= other
            .fields()
            .iter()
            .any(|f| f.name() == crate::ROW_CREATED_AT_VERSION);
        let other =
            self.base
                .schema()
                .project_by_schema(other, on_missing, OnTypeMismatch::TakeSelf)?;
        Ok(self.union_schema(&other))
    }

    /// Removes all fields from the projection that are in the given schema
    ///
    /// on_missing controls what happen to fields that are not in the base schema
    ///
    /// Name based matching is used to determine if a field is in the base schema.
    pub fn subtract_arrow_schema(
        mut self,
        other: &ArrowSchema,
        on_missing: OnMissing,
    ) -> Result<Self> {
        self.with_row_id &= !other.fields().iter().any(|f| f.name() == ROW_ID);
        self.with_row_addr &= !other.fields().iter().any(|f| f.name() == ROW_ADDR);
        self.with_row_last_updated_at_version &= !other
            .fields()
            .iter()
            .any(|f| f.name() == crate::ROW_LAST_UPDATED_AT_VERSION);
        self.with_row_created_at_version &= !other
            .fields()
            .iter()
            .any(|f| f.name() == crate::ROW_CREATED_AT_VERSION);
        let other =
            self.base
                .schema()
                .project_by_schema(other, on_missing, OnTypeMismatch::TakeSelf)?;
        Ok(self.subtract_schema(&other))
    }

    /// Removes all fields from this projection that are present in the given projection
    pub fn subtract_projection(mut self, other: &Self) -> Self {
        self.field_ids = self
            .field_ids
            .difference(&other.field_ids)
            .copied()
            .collect();
        self.with_row_addr = self.with_row_addr && !other.with_row_addr;
        self.with_row_id = self.with_row_id && !other.with_row_id;
        self.with_row_last_updated_at_version =
            self.with_row_last_updated_at_version && !other.with_row_last_updated_at_version;
        self.with_row_created_at_version =
            self.with_row_created_at_version && !other.with_row_created_at_version;
        self
    }

    /// Removes all fields from the projection that are in the given schema
    ///
    /// Fields are only removed if they exist in the base schema, otherwise they
    /// are ignored.
    ///
    /// Will panic if a field in the given schema has a non-negative id and is not in the base schema.
    pub fn subtract_schema(mut self, other: &Schema) -> Self {
        for field in other.fields_pre_order() {
            if field.id >= 0 {
                self.field_ids.remove(&field.id);
            } else if field.name == ROW_ID {
                self.with_row_id = false;
            } else if field.name == ROW_ADDR {
                self.with_row_addr = false;
            } else if field.name == crate::ROW_LAST_UPDATED_AT_VERSION {
                self.with_row_last_updated_at_version = false;
            } else if field.name == crate::ROW_CREATED_AT_VERSION {
                self.with_row_created_at_version = false;
            } else {
                debug_assert_eq!(field.id, -1);
            }
        }
        self
    }

    /// True if the projection does not select any fields or take the row id / addr
    pub fn is_empty(&self) -> bool {
        self.field_ids.is_empty()
            && !self.with_row_addr
            && !self.with_row_id
            && !self.with_row_last_updated_at_version
            && !self.with_row_created_at_version
    }

    /// True if the projection is only the row_id or row_addr columns
    ///
    /// Note: this will return false for a completely empty projection
    pub fn is_metadata_only(&self) -> bool {
        self.field_ids.is_empty()
            && (self.with_row_addr
                || self.with_row_id
                || self.with_row_last_updated_at_version
                || self.with_row_created_at_version)
    }

    /// True if the projection has at least one non-metadata column
    pub fn has_non_meta_cols(&self) -> bool {
        !self.field_ids.is_empty()
    }

    /// Convert the projection to a schema that does not include metadata columns
    pub fn to_bare_schema(&self) -> Schema {
        self.base.schema().apply_projection(self)
    }

    /// Convert the projection to a schema
    ///
    /// Includes the _rowid and _rowaddr columns if requested
    pub fn to_schema(&self) -> Schema {
        let mut schema = self.to_bare_schema();
        let mut extra_fields = Vec::new();
        if self.with_row_id {
            extra_fields.push(ROW_ID_FIELD.clone());
        }
        if self.with_row_addr {
            extra_fields.push(ROW_ADDR_FIELD.clone());
        }
        if self.with_row_last_updated_at_version {
            extra_fields.push(crate::ROW_LAST_UPDATED_AT_VERSION_FIELD.clone());
        }
        if self.with_row_created_at_version {
            extra_fields.push(crate::ROW_CREATED_AT_VERSION_FIELD.clone());
        }
        schema.extend(&extra_fields).unwrap();
        schema
    }

    /// Convert the projection to a schema
    pub fn into_schema(self) -> Schema {
        self.to_schema()
    }

    /// Convert the projection to a schema reference
    pub fn into_schema_ref(self) -> Arc<Schema> {
        Arc::new(self.into_schema())
    }

    /// Convert the projection into an Arrow schema
    pub fn to_arrow_schema(&self) -> arrow_schema::Schema {
        (&self.to_schema()).into()
    }
}

/// Parse a field path that may contain quoted field names.
///
/// Field names containing dots must be quoted with backticks.
/// For example: "parent.`child.with.dot`" parses to ["parent", "child.with.dot"]
///
/// Backticks within quoted fields must be escaped by doubling them.
/// For example: "`field``with``backticks`" represents the field name "field`with`backticks"
///
/// Returns an error if:
/// - The input path is empty
/// - The path has malformed quotes (unclosed, misplaced, etc.)
/// - The path has empty segments (e.g., "parent..child" or "parent.")
///
/// The result is guaranteed to contain at least one element.
pub fn parse_field_path(path: &str) -> Result<Vec<String>> {
    if path.is_empty() {
        return Err(Error::Schema {
            message: "Field path cannot be empty".to_string(),
            location: location!(),
        });
    }

    let mut result = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = path.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '`' => {
                if in_quotes {
                    // Check if this is an escaped backtick (double backtick)
                    if chars.peek() == Some(&'`') {
                        // Consume the second backtick and add a single backtick to current
                        chars.next();
                        current.push('`');
                    } else {
                        // End of quoted field
                        in_quotes = false;
                        // After closing quote, we should either see a dot or end of string
                        if let Some(&next_ch) = chars.peek() {
                            if next_ch != '.' {
                                return Err(Error::Schema {
                                    message: format!("Invalid field path '{}': expected '.' or end of string after closing quote", path),
                                    location: location!(),
                                });
                            }
                        }
                    }
                } else if current.is_empty() {
                    // Start of quoted field
                    in_quotes = true;
                } else {
                    // Quote in the middle of unquoted field name
                    return Err(Error::Schema {
                        message: format!(
                            "Invalid field path '{}': unexpected quote in the middle of field name",
                            path
                        ),
                        location: location!(),
                    });
                }
            }
            '.' if !in_quotes => {
                if current.is_empty() {
                    return Err(Error::Schema {
                        message: format!("Invalid field path '{}': empty field name", path),
                        location: location!(),
                    });
                }
                result.push(current);
                current = String::new();
            }
            _ => {
                current.push(ch);
            }
        }
    }

    if in_quotes {
        return Err(Error::Schema {
            message: format!("Invalid field path '{}': unclosed quote", path),
            location: location!(),
        });
    }

    if !current.is_empty() {
        result.push(current);
    } else if !result.is_empty() {
        return Err(Error::Schema {
            message: format!("Invalid field path '{}': trailing dot", path),
            location: location!(),
        });
    }

    // This check is now redundant since we check for empty input at the beginning,
    // but keeping it for extra safety
    if result.is_empty() {
        return Err(Error::Schema {
            message: format!("Invalid field path '{}'", path),
            location: location!(),
        });
    }

    Ok(result)
}

/// Format a field path, quoting field names that require escaping.
///
/// Field names are quoted if they contain any character that is not alphanumeric
/// or underscore, to ensure safe SQL parsing.
///
/// For example: ["parent", "child.with.dot"] formats to "parent.`child.with.dot`"
/// For example: ["meta-data", "user-id"] formats to "`meta-data`.`user-id`"
/// Backticks in field names are escaped by doubling them.
/// For example: ["field`with`backticks"] formats to "`field``with``backticks`"
pub fn format_field_path(fields: &[&str]) -> String {
    fields
        .iter()
        .map(|field| {
            // Quote if the field contains any non-identifier character
            // (i.e., anything other than alphanumeric or underscore)
            let needs_quoting = field.chars().any(|c| !c.is_alphanumeric() && c != '_');
            if needs_quoting {
                // Escape backticks by doubling them (PostgreSQL style)
                let escaped = field.replace('`', "``");
                format!("`{}`", escaped)
            } else {
                field.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(".")
}

/// Escape a field path for project
///
/// Parses the field path and formats it for SQL usage.
/// Always quotes all segments with backticks to prevent special characters.
///
/// For example:
/// - "parent.child" -> “`parent`.`child`”
/// - "parent.`child.with.dot`" -> “`parent`.`child.with.dot`”
pub fn escape_field_path_for_project(name: &str) -> String {
    if name == WILDCARD {
        return name.to_string();
    }
    let segments = parse_field_path(name).unwrap_or_else(|_| vec![name.to_string()]);
    segments
        .iter()
        .map(|s| {
            let escaped = s.replace('`', "``");
            format!("`{}`", escaped)
        })
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType as ArrowDataType, Fields as ArrowFields};
    use std::{collections::HashMap, sync::Arc};

    use super::*;

    #[test]
    fn projection_from_schema_defaults_to_v1() {
        let field = Field::try_from(&ArrowField::new("a", ArrowDataType::Int32, true)).unwrap();
        let schema = Schema {
            fields: vec![field],
            metadata: HashMap::new(),
        };

        let projection = Projection::empty(Arc::new(schema));

        assert_eq!(projection.blob_version, BlobVersion::V1);
    }

    #[test]
    fn test_resolve_with_quoted_fields() {
        // Create a schema with fields containing dots
        let field_with_dots = Field::try_from(&ArrowField::new(
            "simple.name.with.dot",
            ArrowDataType::Int32,
            false,
        ))
        .unwrap();
        let normal_field =
            Field::try_from(&ArrowField::new("normal", ArrowDataType::Int32, false)).unwrap();
        let nested_field = Field::try_from(&ArrowField::new(
            "parent",
            ArrowDataType::Struct(ArrowFields::from(vec![
                ArrowField::new("child.with.dot", ArrowDataType::Int32, false),
                ArrowField::new("normal_child", ArrowDataType::Int32, false),
            ])),
            false,
        ))
        .unwrap();

        let schema = Schema {
            fields: vec![field_with_dots, normal_field, nested_field],
            metadata: HashMap::new(),
        };

        // Test 1: Resolving a field with dots using quotes
        let resolved = schema.resolve("`simple.name.with.dot`");
        assert!(resolved.is_some());
        let fields = resolved.unwrap();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name, "simple.name.with.dot");

        // Test 2: Resolving a normal field
        let resolved = schema.resolve("normal");
        assert!(resolved.is_some());
        let fields = resolved.unwrap();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name, "normal");

        // Test 3: Resolving a nested field with dots
        let resolved = schema.resolve("parent.`child.with.dot`");
        assert!(resolved.is_some());
        let fields = resolved.unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "parent");
        assert_eq!(fields[1].name, "child.with.dot");

        // Test 4: Resolving a normal nested field
        let resolved = schema.resolve("parent.normal_child");
        assert!(resolved.is_some());
        let fields = resolved.unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "parent");
        assert_eq!(fields[1].name, "normal_child");

        // Test 5: Non-existent field should return None
        let resolved = schema.resolve("\"non.existent\"");
        assert!(resolved.is_none());

        // Test 6: Schema::field should work the same way
        let field = schema.field("`simple.name.with.dot`");
        assert!(field.is_some());
        assert_eq!(field.unwrap().name, "simple.name.with.dot");

        let field = schema.field("parent.`child.with.dot`");
        assert!(field.is_some());
        assert_eq!(field.unwrap().name, "child.with.dot");

        let field = schema.field("parent.normal_child");
        assert!(field.is_some());
        assert_eq!(field.unwrap().name, "normal_child");
    }

    #[test]
    fn test_field_path_parsing() {
        // Simple paths without quotes
        assert_eq!(
            parse_field_path("a.b.c").unwrap(),
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );

        // Single quoted field with dots
        assert_eq!(
            parse_field_path("`simple.name.with.dot`").unwrap(),
            vec!["simple.name.with.dot".to_string()]
        );

        // Path with quoted field containing dots
        assert_eq!(
            parse_field_path("parent.`child.with.dot`.normal").unwrap(),
            vec![
                "parent".to_string(),
                "child.with.dot".to_string(),
                "normal".to_string()
            ]
        );

        // Quoted field at the beginning
        assert_eq!(
            parse_field_path("`field.with.dot`.child").unwrap(),
            vec!["field.with.dot".to_string(), "child".to_string()]
        );

        // Simple field
        assert_eq!(
            parse_field_path("simple").unwrap(),
            vec!["simple".to_string()]
        );

        // Quoted field at the end
        assert_eq!(
            parse_field_path("parent.`field.with.dot`").unwrap(),
            vec!["parent".to_string(), "field.with.dot".to_string()]
        );

        // Field with escaped backticks (PostgreSQL style - double backticks)
        assert_eq!(
            parse_field_path("parent.`field``with``backticks`").unwrap(),
            vec!["parent".to_string(), "field`with`backticks".to_string()]
        );

        // Invalid: unclosed quote
        assert!(parse_field_path("parent.`unclosed").is_err());

        // Invalid: quote in middle of unquoted field
        assert!(parse_field_path("par`ent.child").is_err());

        // Invalid: empty field name
        assert!(parse_field_path("parent..child").is_err());

        // Invalid: trailing dot
        assert!(parse_field_path("parent.").is_err());

        // Test formatting
        assert_eq!(
            format_field_path(&["parent", "child.with.dot", "normal"]),
            "parent.`child.with.dot`.normal"
        );

        assert_eq!(
            format_field_path(&["field`with`backticks"]),
            "`field``with``backticks`"
        );
    }

    #[test]
    fn test_resolve_quoted_fields() {
        // Test that top-level fields with dots are rejected during validation
        let arrow_schema_with_dots = ArrowSchema::new(vec![ArrowField::new(
            "field.with.dots",
            DataType::Int32,
            false,
        )]);

        // Schema creation should fail due to validation
        let schema_result = Schema::try_from(&arrow_schema_with_dots);
        assert!(schema_result.is_err());
        let err = schema_result.unwrap_err();
        assert!(err
            .to_string()
            .contains("Top level field field.with.dots cannot contain `.`"));

        // Test that nested fields with dots are allowed
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("regular_field", DataType::Int32, false),
            ArrowField::new(
                "parent",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("child.with.dot", DataType::Utf8, true),
                    ArrowField::new("normal_child", DataType::Int32, false),
                ])),
                false,
            ),
        ]);

        let schema = Schema::try_from(&arrow_schema).unwrap();

        // Test resolving regular field
        let resolved = schema.resolve("regular_field");
        assert!(resolved.is_some());
        let fields = resolved.unwrap();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name, "regular_field");

        // Test resolving nested field with dots using quotes
        let resolved = schema.resolve("parent.`child.with.dot`");
        assert!(resolved.is_some());
        let fields = resolved.unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "parent");
        assert_eq!(fields[1].name, "child.with.dot");

        // Test resolving normal nested field
        let resolved = schema.resolve("parent.normal_child");
        assert!(resolved.is_some());
        let fields = resolved.unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "parent");
        assert_eq!(fields[1].name, "normal_child");
    }

    use arrow_schema::DataType;

    #[test]
    fn test_schema_projection() {
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("a", DataType::Int32, false),
            ArrowField::new(
                "b",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("f1", DataType::Utf8, true),
                    ArrowField::new("f2", DataType::Boolean, false),
                    ArrowField::new("f3", DataType::Float32, false),
                ])),
                true,
            ),
            ArrowField::new("c", DataType::Float64, false),
        ]);
        let schema = Schema::try_from(&arrow_schema).unwrap();
        let projected = schema.project(&["b.f1", "b.f3", "c"]).unwrap();

        let expected_arrow_schema = ArrowSchema::new(vec![
            ArrowField::new(
                "b",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("f1", DataType::Utf8, true),
                    ArrowField::new("f3", DataType::Float32, false),
                ])),
                true,
            ),
            ArrowField::new("c", DataType::Float64, false),
        ]);
        assert_eq!(ArrowSchema::from(&projected), expected_arrow_schema);
    }

    #[test]
    fn test_schema_project_by_ids() {
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("a", DataType::Int32, false),
            ArrowField::new(
                "b",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("f1", DataType::Utf8, true),
                    ArrowField::new("f2", DataType::Boolean, false),
                    ArrowField::new("f3", DataType::Float32, false),
                ])),
                true,
            ),
            ArrowField::new("c", DataType::Float64, false),
        ]);
        let mut schema = Schema::try_from(&arrow_schema).unwrap();
        schema.set_field_id(None);
        let projected = schema.project_by_ids(&[2, 4, 5], true);

        let expected_arrow_schema = ArrowSchema::new(vec![
            ArrowField::new(
                "b",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("f1", DataType::Utf8, true),
                    ArrowField::new("f3", DataType::Float32, false),
                ])),
                true,
            ),
            ArrowField::new("c", DataType::Float64, false),
        ]);
        assert_eq!(ArrowSchema::from(&projected), expected_arrow_schema);

        let projected = schema.project_by_ids(&[2], true);
        let expected_arrow_schema = ArrowSchema::new(vec![ArrowField::new(
            "b",
            DataType::Struct(ArrowFields::from(vec![ArrowField::new(
                "f1",
                DataType::Utf8,
                true,
            )])),
            true,
        )]);
        assert_eq!(ArrowSchema::from(&projected), expected_arrow_schema);

        let projected = schema.project_by_ids(&[1], true);
        let expected_arrow_schema = ArrowSchema::new(vec![ArrowField::new(
            "b",
            DataType::Struct(ArrowFields::from(vec![
                ArrowField::new("f1", DataType::Utf8, true),
                ArrowField::new("f2", DataType::Boolean, false),
                ArrowField::new("f3", DataType::Float32, false),
            ])),
            true,
        )]);
        assert_eq!(ArrowSchema::from(&projected), expected_arrow_schema);

        let projected = schema.project_by_ids(&[1, 2], false);
        let expected_arrow_schema = ArrowSchema::new(vec![ArrowField::new(
            "b",
            DataType::Struct(ArrowFields::from(vec![ArrowField::new(
                "f1",
                DataType::Utf8,
                true,
            )])),
            true,
        )]);
        assert_eq!(ArrowSchema::from(&projected), expected_arrow_schema);
    }

    #[test]
    fn test_schema_project_by_schema() {
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("a", DataType::Int32, false),
            ArrowField::new(
                "b",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("f1", DataType::Utf8, true),
                    ArrowField::new("f2", DataType::Boolean, false),
                    ArrowField::new("f3", DataType::Float32, false),
                ])),
                true,
            ),
            ArrowField::new("c", DataType::Float64, false),
            ArrowField::new("s", DataType::Utf8, false),
            ArrowField::new(
                "l",
                DataType::List(Arc::new(ArrowField::new("le", DataType::Int32, false))),
                false,
            ),
            ArrowField::new(
                "fixed_l",
                DataType::List(Arc::new(ArrowField::new("elem", DataType::Float32, false))),
                false,
            ),
            ArrowField::new(
                "d",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                false,
            ),
        ]);
        let schema = Schema::try_from(&arrow_schema).unwrap();

        let projection = ArrowSchema::new(vec![
            ArrowField::new(
                "b",
                DataType::Struct(ArrowFields::from(vec![ArrowField::new(
                    "f1",
                    DataType::Utf8,
                    true,
                )])),
                true,
            ),
            ArrowField::new("s", DataType::Utf8, false),
            ArrowField::new(
                "l",
                DataType::List(Arc::new(ArrowField::new("le", DataType::Int32, false))),
                false,
            ),
            ArrowField::new(
                "fixed_l",
                DataType::List(Arc::new(ArrowField::new("elem", DataType::Float32, false))),
                false,
            ),
            ArrowField::new(
                "d",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                false,
            ),
        ]);
        let projected = schema
            .project_by_schema(&projection, OnMissing::Error, OnTypeMismatch::TakeSelf)
            .unwrap();

        assert_eq!(ArrowSchema::from(&projected), projection);
    }

    #[test]
    fn test_get_nested_field() {
        let arrow_schema = ArrowSchema::new(vec![ArrowField::new(
            "b",
            DataType::Struct(ArrowFields::from(vec![
                ArrowField::new("f1", DataType::Utf8, true),
                ArrowField::new("f2", DataType::Boolean, false),
                ArrowField::new("f3", DataType::Float32, false),
            ])),
            true,
        )]);
        let schema = Schema::try_from(&arrow_schema).unwrap();

        let field = schema.field("b.f2").unwrap();
        assert_eq!(field.data_type(), DataType::Boolean);
    }

    #[test]
    fn test_exclude_fields() {
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("a", DataType::Int32, false),
            ArrowField::new(
                "b",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("f1", DataType::Utf8, true),
                    ArrowField::new("f2", DataType::Boolean, false),
                    ArrowField::new("f3", DataType::Float32, false),
                ])),
                true,
            ),
            ArrowField::new("c", DataType::Float64, false),
        ]);
        let schema = Schema::try_from(&arrow_schema).unwrap();

        let projection = schema.project(&["a", "b.f2", "b.f3"]).unwrap();
        let excluded = schema.exclude(&projection).unwrap();

        let expected_arrow_schema = ArrowSchema::new(vec![
            ArrowField::new(
                "b",
                DataType::Struct(ArrowFields::from(vec![ArrowField::new(
                    "f1",
                    DataType::Utf8,
                    true,
                )])),
                true,
            ),
            ArrowField::new("c", DataType::Float64, false),
        ]);
        assert_eq!(ArrowSchema::from(&excluded), expected_arrow_schema);
    }

    #[test]
    fn test_intersection() {
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("a", DataType::Int32, false),
            ArrowField::new(
                "b",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("f1", DataType::Utf8, true),
                    ArrowField::new("f2", DataType::Boolean, false),
                    ArrowField::new("f3", DataType::Float32, false),
                ])),
                true,
            ),
            ArrowField::new("c", DataType::Float64, false),
        ]);
        let schema = Schema::try_from(&arrow_schema).unwrap();

        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new(
                "b",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("f1", DataType::Utf8, true),
                    ArrowField::new("f2", DataType::Boolean, false),
                ])),
                true,
            ),
            ArrowField::new("c", DataType::Float64, false),
            ArrowField::new("d", DataType::Utf8, false),
        ]);
        let other = Schema::try_from(&arrow_schema).unwrap();

        let actual: ArrowSchema = (&schema.intersection(&other).unwrap()).into();

        let expected = ArrowSchema::new(vec![
            ArrowField::new(
                "b",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("f1", DataType::Utf8, true),
                    ArrowField::new("f2", DataType::Boolean, false),
                ])),
                true,
            ),
            ArrowField::new("c", DataType::Float64, false),
        ]);
        assert_eq!(actual, expected);

        let schema_with_list_struct = ArrowSchema::new(vec![ArrowField::new(
            "struct_list",
            DataType::List(Arc::new(ArrowField::new(
                "item",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("f1", DataType::Utf8, true),
                    ArrowField::new("f2", DataType::Boolean, false),
                ])),
                true,
            ))),
            true,
        )]);
        let schema_with_list_struct = Schema::try_from(&schema_with_list_struct).unwrap();

        let with_missing_field = schema_with_list_struct.project_by_ids(&[1, 3], false);
        let intersection = schema_with_list_struct
            .intersection_ignore_types(&with_missing_field)
            .unwrap();
        assert_eq!(intersection, with_missing_field);
        let intersection = with_missing_field
            .intersection_ignore_types(&schema_with_list_struct)
            .unwrap();
        assert_eq!(intersection, with_missing_field);
    }

    #[test]
    fn test_merge_schemas_and_assign_field_ids() {
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("a", DataType::Int32, false),
            ArrowField::new(
                "b",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("f1", DataType::Utf8, true),
                    ArrowField::new("f2", DataType::Boolean, false),
                    ArrowField::new("f3", DataType::Float32, false),
                ])),
                true,
            ),
            ArrowField::new("c", DataType::Float64, false),
        ]);
        let schema = Schema::try_from(&arrow_schema).unwrap();

        assert_eq!(schema.max_field_id(), Some(5));

        let to_merged_arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("d", DataType::Int32, false),
            ArrowField::new("e", DataType::Binary, false),
        ]);
        let to_merged = Schema::try_from(&to_merged_arrow_schema).unwrap();
        // It is already assigned with field ids.
        assert_eq!(to_merged.max_field_id(), Some(1));

        let mut merged = schema.merge(&to_merged).unwrap();
        assert_eq!(merged.max_field_id(), Some(5));

        let field = merged.field("d").unwrap();
        assert_eq!(field.id, -1);
        let field = merged.field("e").unwrap();
        assert_eq!(field.id, -1);

        // Need to explicitly assign field ids. Testing we can pass a larger
        // field id to set_field_id.
        merged.set_field_id(Some(7));
        let field = merged.field("d").unwrap();
        assert_eq!(field.id, 8);
        let field = merged.field("e").unwrap();
        assert_eq!(field.id, 9);
        assert_eq!(merged.max_field_id(), Some(9));
    }

    #[test]
    fn test_merge_arrow_schema() {
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("a", DataType::Int32, false),
            ArrowField::new(
                "b",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("f1", DataType::Utf8, true),
                    ArrowField::new("f2", DataType::Boolean, false),
                    ArrowField::new("f3", DataType::Float32, false),
                ])),
                true,
            ),
            ArrowField::new("c", DataType::Float64, false),
        ]);
        let schema = Schema::try_from(&arrow_schema).unwrap();

        assert_eq!(schema.max_field_id(), Some(5));

        let to_merged_arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("d", DataType::Int32, false),
            ArrowField::new("e", DataType::Binary, false),
        ]);
        let mut merged = schema.merge(&to_merged_arrow_schema).unwrap();
        merged.set_field_id(None);
        assert_eq!(merged.max_field_id(), Some(7));

        let field = merged.field("d").unwrap();
        assert_eq!(field.id, 6);
        let field = merged.field("e").unwrap();
        assert_eq!(field.id, 7);
    }

    #[test]
    fn test_merge_nested_field() {
        let arrow_schema1 = ArrowSchema::new(vec![ArrowField::new(
            "b",
            DataType::Struct(ArrowFields::from(vec![
                ArrowField::new(
                    "f1",
                    DataType::Struct(ArrowFields::from(vec![ArrowField::new(
                        "f11",
                        DataType::Utf8,
                        true,
                    )])),
                    true,
                ),
                ArrowField::new("f2", DataType::Float32, false),
            ])),
            true,
        )]);
        let schema1 = Schema::try_from(&arrow_schema1).unwrap();

        let arrow_schema2 = ArrowSchema::new(vec![ArrowField::new(
            "b",
            DataType::Struct(ArrowFields::from(vec![
                ArrowField::new(
                    "f1",
                    DataType::Struct(ArrowFields::from(vec![ArrowField::new(
                        "f22",
                        DataType::Utf8,
                        true,
                    )])),
                    true,
                ),
                ArrowField::new("f3", DataType::Float32, false),
            ])),
            true,
        )]);
        let schema2 = Schema::try_from(&arrow_schema2).unwrap();

        let expected_arrow_schema = ArrowSchema::new(vec![ArrowField::new(
            "b",
            DataType::Struct(ArrowFields::from(vec![
                ArrowField::new(
                    "f1",
                    DataType::Struct(ArrowFields::from(vec![
                        ArrowField::new("f11", DataType::Utf8, true),
                        ArrowField::new("f22", DataType::Utf8, true),
                    ])),
                    true,
                ),
                ArrowField::new("f2", DataType::Float32, false),
                ArrowField::new("f3", DataType::Float32, false),
            ])),
            true,
        )]);
        let mut expected_schema = Schema::try_from(&expected_arrow_schema).unwrap();
        expected_schema.fields[0]
            .child_mut("f1")
            .unwrap()
            .child_mut("f22")
            .unwrap()
            .id = 4;
        expected_schema.fields[0].child_mut("f2").unwrap().id = 3;

        let mut result = schema1.merge(&schema2).unwrap();
        result.set_field_id(None);
        assert_eq!(result, expected_schema);
    }

    #[test]
    fn test_field_by_id() {
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("a", DataType::Int32, false),
            ArrowField::new(
                "b",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("f1", DataType::Utf8, true),
                    ArrowField::new("f2", DataType::Boolean, false),
                    ArrowField::new("f3", DataType::Float32, false),
                ])),
                true,
            ),
            ArrowField::new("c", DataType::Float64, false),
        ]);
        let schema = Schema::try_from(&arrow_schema).unwrap();

        let field = schema.field_by_id(1).unwrap();
        assert_eq!(field.name, "b");

        let field = schema.field_by_id(3).unwrap();
        assert_eq!(field.name, "f2");
    }

    #[test]
    fn test_explain_difference() {
        let expected = ArrowSchema::new(vec![
            ArrowField::new("a", DataType::Int32, false),
            ArrowField::new(
                "b",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("f1", DataType::Utf8, true),
                    ArrowField::new("f2", DataType::Boolean, false),
                    ArrowField::new("f3", DataType::Float32, false),
                ])),
                true,
            ),
            ArrowField::new("c", DataType::Float64, false),
        ]);
        let expected = Schema::try_from(&expected).unwrap();

        let mismatched = ArrowSchema::new(vec![
            ArrowField::new("a", DataType::Int32, false),
            ArrowField::new(
                "b",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("f1", DataType::Utf8, true),
                    ArrowField::new("f3", DataType::Float32, false),
                ])),
                true,
            ),
            ArrowField::new("c", DataType::Float64, true),
        ]);
        let mismatched = Schema::try_from(&mismatched).unwrap();

        assert_eq!(
            mismatched.explain_difference(&expected, &SchemaCompareOptions::default()),
            Some(
                "`b` had mismatched children: fields did not match, missing=[b.f2], \
                  unexpected=[], `c` should have nullable=false but nullable=true"
                    .to_string()
            )
        );
    }

    #[test]
    fn test_schema_difference_subschema() {
        let expected = ArrowSchema::new(vec![
            ArrowField::new("a", DataType::Int32, false),
            ArrowField::new(
                "b",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("f1", DataType::Utf8, true),
                    ArrowField::new("f2", DataType::Boolean, false),
                    ArrowField::new("f3", DataType::Float32, false),
                ])),
                true,
            ),
            ArrowField::new("c", DataType::Float64, true),
        ]);
        let expected = Schema::try_from(&expected).unwrap();

        // Can omit nullable fields and subfields
        let subschema = ArrowSchema::new(vec![
            ArrowField::new("a", DataType::Int32, false),
            ArrowField::new(
                "b",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("f2", DataType::Boolean, false),
                    ArrowField::new("f3", DataType::Float32, false),
                ])),
                true,
            ),
        ]);
        let subschema = Schema::try_from(&subschema).unwrap();

        assert!(!subschema.compare_with_options(&expected, &SchemaCompareOptions::default()));
        assert_eq!(
            subschema.explain_difference(&expected, &SchemaCompareOptions::default()),
            Some(
                "fields did not match, missing=[c], unexpected=[], `b` had mismatched \
                 children: fields did not match, missing=[b.f1], unexpected=[]"
                    .to_string()
            )
        );
        let options = SchemaCompareOptions {
            allow_missing_if_nullable: true,
            ..Default::default()
        };
        assert!(subschema.compare_with_options(&expected, &options));
        let res = subschema.explain_difference(&expected, &options);
        assert!(res.is_none(), "Expected None, got {:?}", res);

        // Omitting non-nullable fields should fail
        let subschema = ArrowSchema::new(vec![ArrowField::new(
            "b",
            DataType::Struct(ArrowFields::from(vec![ArrowField::new(
                "f2",
                DataType::Boolean,
                false,
            )])),
            true,
        )]);
        let subschema = Schema::try_from(&subschema).unwrap();
        assert!(!subschema.compare_with_options(&expected, &options));
        assert_eq!(
            subschema.explain_difference(&expected, &options),
            Some(
                "fields did not match, missing=[a], unexpected=[], `b` had mismatched \
                 children: fields did not match, missing=[b.f3], unexpected=[]"
                    .to_string()
            )
        );

        let out_of_order = ArrowSchema::new(vec![
            ArrowField::new("c", DataType::Float64, true),
            ArrowField::new(
                "b",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("f3", DataType::Float32, false),
                    ArrowField::new("f2", DataType::Boolean, false),
                    ArrowField::new("f1", DataType::Utf8, true),
                ])),
                true,
            ),
            ArrowField::new("a", DataType::Int32, false),
        ]);
        let out_of_order = Schema::try_from(&out_of_order).unwrap();
        assert!(!out_of_order.compare_with_options(&expected, &options));
        assert_eq!(
            subschema.explain_difference(&expected, &options),
            Some(
                "fields did not match, missing=[a], unexpected=[], `b` had mismatched \
                 children: fields did not match, missing=[b.f3], unexpected=[]"
                    .to_string()
            )
        );

        let options = SchemaCompareOptions {
            ignore_field_order: true,
            ..Default::default()
        };
        assert!(out_of_order.compare_with_options(&expected, &options));
        let res = out_of_order.explain_difference(&expected, &options);
        assert!(res.is_none(), "Expected None, got {:?}", res);
    }

    #[test]
    pub fn test_all_fields_nullable() {
        let test_cases = vec![
            (
                vec![], // empty schema
                true,
            ),
            (
                vec![
                    Field::new_arrow("a", DataType::Int32, true).unwrap(),
                    Field::new_arrow("b", DataType::Utf8, true).unwrap(),
                ], // basic case
                true,
            ),
            (
                vec![
                    Field::new_arrow("a", DataType::Int32, false).unwrap(),
                    Field::new_arrow("b", DataType::Utf8, true).unwrap(),
                ],
                false,
            ),
            (
                // check nested schema, parent is nullable
                vec![Field::new_arrow(
                    "struct",
                    DataType::Struct(ArrowFields::from(vec![ArrowField::new(
                        "a",
                        DataType::Int32,
                        false,
                    )])),
                    true,
                )
                .unwrap()],
                false,
            ),
            (
                // check nested schema, child is nullable
                vec![Field::new_arrow(
                    "struct",
                    DataType::Struct(ArrowFields::from(vec![ArrowField::new(
                        "a",
                        DataType::Int32,
                        true,
                    )])),
                    false,
                )
                .unwrap()],
                false,
            ),
            (
                // check nested schema, all is nullable
                vec![Field::new_arrow(
                    "struct",
                    DataType::Struct(ArrowFields::from(vec![ArrowField::new(
                        "a",
                        DataType::Int32,
                        true,
                    )])),
                    true,
                )
                .unwrap()],
                true,
            ),
        ];

        for (fields, expected) in test_cases {
            let schema = Schema {
                fields,
                metadata: Default::default(),
            };
            assert_eq!(schema.all_fields_nullable(), expected);
        }
    }

    #[test]
    fn test_schema_unenforced_primary_key() {
        let cases = vec![
            ArrowSchema::new(vec![ArrowField::new("a", DataType::Int32, false)]),
            ArrowSchema::new(vec![ArrowField::new("a", DataType::Int32, false)
                .with_metadata(
                    vec![(
                        "lance-schema:unenforced-primary-key".to_owned(),
                        "true".to_owned(),
                    )]
                    .into_iter()
                    .collect::<HashMap<_, _>>(),
                )]),
            ArrowSchema::new(vec![
                ArrowField::new("a", DataType::Int32, false).with_metadata(
                    vec![(
                        "lance-schema:unenforced-primary-key".to_owned(),
                        "true".to_owned(),
                    )]
                    .into_iter()
                    .collect::<HashMap<_, _>>(),
                ),
                ArrowField::new(
                    "b",
                    DataType::Struct(ArrowFields::from(vec![ArrowField::new(
                        "f1",
                        DataType::Utf8,
                        false,
                    )
                    .with_metadata(
                        vec![(
                            "lance-schema:unenforced-primary-key".to_owned(),
                            "true".to_owned(),
                        )]
                        .into_iter()
                        .collect::<HashMap<_, _>>(),
                    )])),
                    false,
                ),
            ]),
        ];
        let expected = [
            vec![],
            vec!["a".to_owned()],
            vec!["a".to_owned(), "f1".to_owned()],
        ];

        for (idx, case) in cases.into_iter().enumerate() {
            let schema = Schema::try_from(&case).unwrap();
            assert_eq!(
                schema
                    .unenforced_primary_key()
                    .iter()
                    .map(|f| f.name.clone())
                    .collect::<Vec<_>>(),
                expected[idx]
            );
        }
    }

    #[test]
    fn test_schema_unenforced_primary_key_failures() {
        let cases = vec![
            ArrowSchema::new(vec![ArrowField::new("a", DataType::Int32, true)
                .with_metadata(
                    vec![(
                        "lance-schema:unenforced-primary-key".to_owned(),
                        "true".to_owned(),
                    )]
                    .into_iter()
                    .collect::<HashMap<_, _>>(),
                )]),
            ArrowSchema::new(vec![ArrowField::new(
                "b",
                DataType::Struct(ArrowFields::from(vec![ArrowField::new(
                    "f1",
                    DataType::Utf8,
                    false,
                )])),
                false,
            )
            .with_metadata(
                vec![(
                    "lance-schema:unenforced-primary-key".to_owned(),
                    "true".to_owned(),
                )]
                .into_iter()
                .collect::<HashMap<_, _>>(),
            )]),
            ArrowSchema::new(vec![ArrowField::new(
                "b",
                DataType::Struct(ArrowFields::from(vec![ArrowField::new(
                    "f1",
                    DataType::Utf8,
                    false,
                )
                .with_metadata(
                    vec![(
                        "lance-schema:unenforced-primary-key".to_owned(),
                        "true".to_owned(),
                    )]
                    .into_iter()
                    .collect::<HashMap<_, _>>(),
                )])),
                true,
            )]),
            ArrowSchema::new(vec![ArrowField::new(
                "b",
                DataType::List(Arc::new(
                    ArrowField::new("f1", DataType::Utf8, false).with_metadata(
                        vec![(
                            "lance-schema:unenforced-primary-key".to_owned(),
                            "true".to_owned(),
                        )]
                        .into_iter()
                        .collect::<HashMap<_, _>>(),
                    ),
                )),
                false,
            )]),
        ];
        let error_message_contains = [
            "Primary key column and all its ancestors must not be nullable",
            "Primary key column must be a leaf",
            "Primary key column and all its ancestors must not be nullable",
            "Primary key column must not be in a list type",
        ];

        for (idx, case) in cases.into_iter().enumerate() {
            let result = Schema::try_from(&case);
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains(error_message_contains[idx]));
        }
    }
}
