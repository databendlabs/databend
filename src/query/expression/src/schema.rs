// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::datatypes::DataType as ArrowDataType;
use common_arrow::arrow::datatypes::Field as ArrowField;
use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::datatypes::TimeUnit;
use common_exception::ErrorCode;
use common_exception::Result;
use common_jsonb::Number as JsonbNumber;
use common_jsonb::Object as JsonbObject;
use common_jsonb::Value as JsonbValue;
use ethnum::i256;
use itertools::Itertools;
use rand::distributions::Alphanumeric;
use rand::distributions::DistString;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;
use serde::Deserialize;
use serde::Serialize;

use crate::types::array::ArrayColumn;
use crate::types::date::DATE_MAX;
use crate::types::date::DATE_MIN;
use crate::types::decimal::DecimalColumn;
use crate::types::decimal::DecimalDataType;
use crate::types::decimal::DecimalSize;
use crate::types::nullable::NullableColumn;
use crate::types::timestamp::TIMESTAMP_MAX;
use crate::types::timestamp::TIMESTAMP_MIN;
use crate::types::BooleanType;
use crate::types::DataType;
use crate::types::DateType;
use crate::types::NumberDataType;
use crate::types::NumberType;
use crate::types::StringType;
use crate::types::TimestampType;
use crate::types::VariantType;
use crate::with_number_mapped_type;
use crate::with_number_type;
use crate::BlockEntry;
use crate::Column;
use crate::FromData;
use crate::TypeDeserializerImpl;
use crate::Value;
use crate::ARROW_EXT_TYPE_EMPTY_ARRAY;
use crate::ARROW_EXT_TYPE_VARIANT;

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DataSchema {
    pub(crate) fields: Vec<DataField>,
    pub(crate) metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct DataField {
    name: String,
    default_expr: Option<String>,
    data_type: DataType,
}

fn uninit_column_id() -> u32 {
    0
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TableSchema {
    pub(crate) fields: Vec<TableField>,
    pub(crate) metadata: BTreeMap<String, String>,
    // next column id that assign to TableField.column_id
    #[serde(default = "uninit_column_id")]
    pub(crate) next_column_id: u32,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TableField {
    name: String,
    default_expr: Option<String>,
    data_type: TableDataType,
    #[serde(default = "uninit_column_id")]
    column_id: u32,
}

/// DataType with more information that is only available for table field, e.g, the
/// tuple field name, or the scale of decimal.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum TableDataType {
    Null,
    EmptyArray,
    Boolean,
    String,
    Number(NumberDataType),
    Decimal(DecimalDataType),
    Timestamp,
    Date,
    Nullable(Box<TableDataType>),
    Array(Box<TableDataType>),
    Map(Box<TableDataType>),
    Tuple {
        fields_name: Vec<String>,
        fields_type: Vec<TableDataType>,
    },
    Variant,
}

impl DataSchema {
    pub fn empty() -> Self {
        Self {
            fields: vec![],
            metadata: BTreeMap::new(),
        }
    }

    pub fn new(fields: Vec<DataField>) -> Self {
        Self {
            fields,
            metadata: BTreeMap::new(),
        }
    }

    pub fn new_from(fields: Vec<DataField>, metadata: BTreeMap<String, String>) -> Self {
        Self { fields, metadata }
    }

    /// Returns an immutable reference of the vector of `Field` instances.
    #[inline]
    pub const fn fields(&self) -> &Vec<DataField> {
        &self.fields
    }

    #[inline]
    pub fn num_fields(&self) -> usize {
        self.fields.len()
    }

    #[inline]
    pub fn has_field(&self, name: &str) -> bool {
        for i in 0..self.fields.len() {
            if self.fields[i].name() == name {
                return true;
            }
        }
        false
    }

    pub fn fields_map(&self) -> BTreeMap<usize, DataField> {
        let x = self.fields().iter().cloned().enumerate();
        x.collect::<BTreeMap<_, _>>()
    }

    /// Returns an immutable reference of a specific `Field` instance selected using an
    /// offset within the internal `fields` vector.
    pub fn field(&self, i: usize) -> &DataField {
        &self.fields[i]
    }

    /// Returns an immutable reference of a specific `Field` instance selected by name.
    pub fn field_with_name(&self, name: &str) -> Result<&DataField> {
        Ok(&self.fields[self.index_of(name)?])
    }

    /// Returns an immutable reference to field `metadata`.
    #[inline]
    pub const fn meta(&self) -> &BTreeMap<String, String> {
        &self.metadata
    }

    /// Find the index of the column with the given name.
    pub fn index_of(&self, name: &str) -> Result<usize> {
        for i in 0..self.fields.len() {
            if self.fields[i].name() == name {
                return Ok(i);
            }
        }
        let valid_fields: Vec<String> = self.fields.iter().map(|f| f.name().clone()).collect();

        Err(ErrorCode::BadArguments(format!(
            "Unable to get field named \"{}\". Valid fields: {:?}",
            name, valid_fields
        )))
    }

    /// Look up a column by name and return a immutable reference to the column along with
    /// its index.
    pub fn column_with_name(&self, name: &str) -> Option<(usize, &DataField)> {
        self.fields
            .iter()
            .enumerate()
            .find(|&(_, c)| c.name() == name)
    }

    /// Check to see if `self` is a superset of `other` schema. Here are the comparison rules:
    pub fn contains(&self, other: &DataSchema) -> bool {
        if self.fields.len() != other.fields.len() {
            return false;
        }

        for (i, field) in other.fields.iter().enumerate() {
            if &self.fields[i] != field {
                return false;
            }
        }
        true
    }

    /// project will do column pruning.
    #[must_use]
    pub fn project(&self, projection: &[usize]) -> Self {
        let fields = projection
            .iter()
            .map(|idx| self.fields()[*idx].clone())
            .collect();
        Self::new_from(fields, self.meta().clone())
    }

    /// project will do column pruning.
    #[must_use]
    pub fn project_by_fields(&self, fields: Vec<DataField>) -> Self {
        Self::new_from(fields, self.meta().clone())
    }

    pub fn to_arrow(&self) -> ArrowSchema {
        let fields = self.fields().iter().map(|f| f.into()).collect::<Vec<_>>();

        ArrowSchema::from(fields).with_metadata(self.metadata.clone())
    }

    pub fn create_deserializers(&self, capacity: usize) -> Vec<TypeDeserializerImpl> {
        let mut deserializers = Vec::with_capacity(self.num_fields());
        for field in self.fields() {
            deserializers.push(field.data_type.create_deserializer(capacity));
        }
        deserializers
    }
}

impl TableSchema {
    pub fn empty() -> Self {
        Self {
            fields: vec![],
            metadata: BTreeMap::new(),
            next_column_id: 0,
        }
    }

    pub fn init_if_need(data_schema: TableSchema) -> Self {
        // If next_column_id is 0, it is an old version needs to compatibility
        if data_schema.next_column_id == 0 {
            Self::new_from(data_schema.fields, data_schema.metadata)
        } else {
            data_schema
        }
    }

    fn build_members_from_fields(
        fields: Vec<TableField>,
        next_column_id: u32,
    ) -> (u32, Vec<TableField>) {
        if next_column_id > 0 {
            // make sure that field column id has been inited.
            if fields.len() > 1 {
                assert!(fields[1].column_id() > 0);
            }
            return (next_column_id, fields);
        }

        let mut new_next_column_id = 0;
        let mut new_fields = Vec::with_capacity(fields.len());

        for field in fields {
            let new_field = field.build_column_id(&mut new_next_column_id);
            // check next_column_id value cannot fallback
            assert!(new_next_column_id >= new_field.column_id());
            new_fields.push(new_field);
        }

        // check next_column_id value cannot fallback
        assert!(new_next_column_id >= next_column_id);

        (new_next_column_id, new_fields)
    }

    pub fn new(fields: Vec<TableField>) -> Self {
        let (next_column_id, new_fields) = Self::build_members_from_fields(fields, 0);
        Self {
            fields: new_fields,
            metadata: BTreeMap::new(),
            next_column_id,
        }
    }

    pub fn new_from(fields: Vec<TableField>, metadata: BTreeMap<String, String>) -> Self {
        let (next_column_id, new_fields) = Self::build_members_from_fields(fields, 0);
        Self {
            fields: new_fields,
            metadata,
            next_column_id,
        }
    }

    pub fn new_from_column_ids(
        fields: Vec<TableField>,
        metadata: BTreeMap<String, String>,
        next_column_id: u32,
    ) -> Self {
        let (next_column_id, new_fields) = Self::build_members_from_fields(fields, next_column_id);
        Self {
            fields: new_fields,
            metadata,
            next_column_id,
        }
    }

    #[inline]
    pub fn next_column_id(&self) -> u32 {
        self.next_column_id
    }

    pub fn column_id_of_index(&self, i: usize) -> Result<u32> {
        Ok(self.fields[i].column_id())
    }

    pub fn column_id_of(&self, name: &str) -> Result<u32> {
        let i = self.index_of(name)?;
        Ok(self.fields[i].column_id())
    }

    pub fn is_column_deleted(&self, column_id: u32) -> bool {
        for field in &self.fields {
            if field.contain_column_id(column_id) {
                return false;
            }
        }

        true
    }

    pub fn add_columns(&mut self, fields: &[TableField]) -> Result<()> {
        for f in fields {
            if self.index_of(f.name()).is_ok() {
                return Err(ErrorCode::AddColumnExistError(format!(
                    "add column {} already exist",
                    f.name(),
                )));
            }
            let field = f.build_column_id(&mut self.next_column_id);
            self.fields.push(field);
        }
        Ok(())
    }

    pub fn drop_column(&mut self, column: &str) -> Result<()> {
        if self.fields.len() == 1 {
            return Err(ErrorCode::DropColumnEmptyError(
                "cannot drop table column to empty",
            ));
        }
        let i = self.index_of(column)?;
        self.fields.remove(i);

        Ok(())
    }

    pub fn to_column_ids(&self) -> Vec<u32> {
        let mut column_ids = Vec::with_capacity(self.fields.len());

        self.fields.iter().for_each(|f| {
            column_ids.extend(f.column_ids());
        });

        column_ids
    }

    pub fn to_leaf_column_ids(&self) -> Vec<u32> {
        let mut column_ids = Vec::with_capacity(self.fields.len());

        self.fields.iter().for_each(|f| {
            column_ids.extend(f.leaf_column_ids());
        });

        column_ids
    }

    /// Returns an immutable reference of the vector of `Field` instances.
    #[inline]
    pub const fn fields(&self) -> &Vec<TableField> {
        &self.fields
    }

    #[inline]
    pub fn field_column_ids(&self) -> Vec<Vec<u32>> {
        let mut field_column_ids = Vec::with_capacity(self.fields.len());

        self.fields.iter().for_each(|f| {
            field_column_ids.push(f.column_ids());
        });

        field_column_ids
    }

    #[inline]
    pub fn field_leaf_column_ids(&self) -> Vec<Vec<u32>> {
        let mut field_column_ids = Vec::with_capacity(self.fields.len());

        self.fields.iter().for_each(|f| {
            field_column_ids.push(f.leaf_column_ids());
        });

        field_column_ids
    }

    #[inline]
    pub fn num_fields(&self) -> usize {
        self.fields.len()
    }

    #[inline]
    pub fn has_field(&self, name: &str) -> bool {
        for i in 0..self.fields.len() {
            if self.fields[i].name == name {
                return true;
            }
        }
        false
    }

    pub fn fields_map(&self) -> BTreeMap<usize, TableField> {
        let x = self.fields().iter().cloned().enumerate();
        x.collect::<BTreeMap<_, _>>()
    }

    /// Returns an immutable reference of a specific `Field` instance selected using an
    /// offset within the internal `fields` vector.
    pub fn field(&self, i: usize) -> &TableField {
        &self.fields[i]
    }

    /// Returns an immutable reference of a specific `Field` instance selected by name.
    pub fn field_with_name(&self, name: &str) -> Result<&TableField> {
        Ok(&self.fields[self.index_of(name)?])
    }

    /// Returns an immutable reference to field `metadata`.
    #[inline]
    pub const fn meta(&self) -> &BTreeMap<String, String> {
        &self.metadata
    }

    /// Find the index of the column with the given name.
    pub fn index_of(&self, name: &str) -> Result<usize> {
        for i in 0..self.fields.len() {
            if self.fields[i].name == name {
                return Ok(i);
            }
        }
        let valid_fields: Vec<String> = self.fields.iter().map(|f| f.name.clone()).collect();

        Err(ErrorCode::BadArguments(format!(
            "Unable to get field named \"{}\". Valid fields: {:?}",
            name, valid_fields
        )))
    }

    /// Look up a column by name and return a immutable reference to the column along with
    /// its index.
    pub fn column_with_name(&self, name: &str) -> Option<(usize, &TableField)> {
        self.fields
            .iter()
            .enumerate()
            .find(|&(_, c)| c.name == name)
    }

    /// Check to see if `self` is a superset of `other` schema. Here are the comparison rules:
    pub fn contains(&self, other: &TableSchema) -> bool {
        if self.fields.len() != other.fields.len() {
            return false;
        }

        for (i, field) in other.fields.iter().enumerate() {
            if &self.fields[i] != field {
                return false;
            }
        }
        true
    }

    /// project will do column pruning.
    #[must_use]
    pub fn project(&self, projection: &[usize]) -> Self {
        let mut fields = Vec::with_capacity(projection.len());
        for idx in projection {
            fields.push(self.fields[*idx].clone());
        }

        Self {
            fields,
            metadata: self.metadata.clone(),
            next_column_id: self.next_column_id,
        }
    }

    /// project with inner columns by path.
    pub fn inner_project(&self, path_indices: &BTreeMap<usize, Vec<usize>>) -> Self {
        let paths: Vec<Vec<usize>> = path_indices.values().cloned().collect();
        let schema_fields = self.fields();
        let column_ids = self.to_column_ids();

        let fields = paths
            .iter()
            .map(|path| Self::traverse_paths(schema_fields, path, &column_ids).unwrap())
            .collect();

        Self {
            fields,
            metadata: self.metadata.clone(),
            next_column_id: self.next_column_id,
        }
    }

    fn traverse_paths(
        fields: &[TableField],
        path: &[usize],
        column_ids: &[u32],
    ) -> Result<TableField> {
        if path.is_empty() {
            return Err(ErrorCode::BadArguments(
                "path should not be empty".to_string(),
            ));
        }
        let index = path[0];
        let field = &fields[index];
        if path.len() == 1 {
            return Ok(field.clone());
        }

        // If the data type is Tuple, we can read the innner columns directly.
        // For example, `select t:a from table`, we can only read column t:a.
        // So we can project the inner field as a independent field (`inner_project` and `traverse_paths` will be called).
        //
        // For more complex type, such as Array(Tuple), and sql `select array[0]:field from table`,
        // we can't do inner project, because get field from these types will turn into calling `get` method. (Use `EXPLAIN ...` to see the plan.)
        // When calling `get` method, the whole outer column will be read,
        // so `inner_project` and `traverse_paths` methods will not be called (`project` is called instead).
        //
        // Although `inner_project` and `traverse_paths` methods will not be called for complex types like Array(Tuple),
        // when constructing column leaves (for reading parquet) for these types, we still need to dfs the inner fields.
        // See comments in `common_storage::ColumnNodes::traverse_fields_dfs` for more details.
        if let TableDataType::Tuple {
            fields_name,
            fields_type,
        } = &field.data_type
        {
            let field_name = field.name();
            let mut next_column_id = column_ids[1 + index];
            let fields = fields_name
                .iter()
                .zip(fields_type)
                .map(|(name, ty)| {
                    let inner_name = format!("{}:{}", field_name, name.to_lowercase());
                    let field = TableField::new(&inner_name, ty.clone());
                    field.build_column_id(&mut next_column_id)
                })
                .collect::<Vec<_>>();
            return Self::traverse_paths(&fields, &path[1..], &column_ids[index + 1..]);
        }
        let valid_fields: Vec<String> = fields.iter().map(|f| f.name.clone()).collect();
        Err(ErrorCode::BadArguments(format!(
            "Unable to get field paths. Valid fields: {:?}",
            valid_fields
        )))
    }

    // return leaf fields and column ids
    pub fn leaf_fields(&self) -> (Vec<u32>, Vec<TableField>) {
        fn collect_in_field(field: &TableField, fields: &mut Vec<TableField>) {
            match field.data_type() {
                TableDataType::Tuple {
                    fields_type,
                    fields_name,
                } => {
                    for (name, ty) in fields_name.iter().zip(fields_type) {
                        collect_in_field(&TableField::new(name, ty.clone()), fields);
                    }
                }
                TableDataType::Array(ty) => {
                    collect_in_field(
                        &TableField::new(&format!("{}:0", field.name()), ty.as_ref().to_owned()),
                        fields,
                    );
                }
                _ => fields.push(field.clone()),
            }
        }

        let mut fields = Vec::new();
        for field in self.fields() {
            collect_in_field(field, &mut fields);
        }
        (self.to_leaf_column_ids(), fields)
    }

    /// project will do column pruning.
    #[must_use]
    pub fn project_by_fields(&self, fields: &BTreeMap<usize, TableField>) -> Self {
        let column_ids = self.to_column_ids();
        let mut new_fields = Vec::with_capacity(fields.len());
        for (index, f) in fields.iter() {
            let mut column_id = column_ids[*index];
            let field = f.build_column_id(&mut column_id);
            new_fields.push(field);
        }
        Self {
            fields: new_fields,
            metadata: self.metadata.clone(),
            next_column_id: self.next_column_id,
        }
    }

    pub fn to_arrow(&self) -> ArrowSchema {
        let fields = self.fields().iter().map(|f| f.into()).collect::<Vec<_>>();

        ArrowSchema::from(fields).with_metadata(self.metadata.clone())
    }

    pub fn create_deserializers(&self, capacity: usize) -> Vec<TypeDeserializerImpl> {
        let mut deserializers = Vec::with_capacity(self.num_fields());
        for field in self.fields() {
            let data_type: DataType = field.data_type().into();
            deserializers.push(data_type.create_deserializer(capacity));
        }
        deserializers
    }
}

impl DataField {
    pub fn new(name: &str, data_type: DataType) -> Self {
        DataField {
            name: name.to_string(),
            default_expr: None,
            data_type,
        }
    }

    pub fn new_nullable(name: &str, data_type: DataType) -> Self {
        DataField {
            name: name.to_string(),
            default_expr: None,
            data_type: DataType::Nullable(Box::new(data_type)),
        }
    }

    #[must_use]
    pub fn with_default_expr(mut self, default_expr: Option<String>) -> Self {
        self.default_expr = default_expr;
        self
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn default_expr(&self) -> Option<&String> {
        self.default_expr.as_ref()
    }

    #[inline]
    pub fn is_nullable(&self) -> bool {
        self.data_type.is_nullable()
    }

    #[inline]
    pub fn is_nullable_or_null(&self) -> bool {
        self.data_type.is_nullable_or_null()
    }
}

impl TableField {
    pub fn new(name: &str, data_type: TableDataType) -> Self {
        TableField {
            name: name.to_string(),
            default_expr: None,
            data_type,
            column_id: 0,
        }
    }

    pub fn new_from_column_id(name: &str, data_type: TableDataType, column_id: u32) -> Self {
        TableField {
            name: name.to_string(),
            default_expr: None,
            data_type,
            column_id,
        }
    }

    fn build_column_ids_from_data_type(
        data_type: &TableDataType,
        column_ids: &mut Vec<u32>,
        next_column_id: &mut u32,
    ) {
        column_ids.push(*next_column_id);

        match data_type {
            TableDataType::Tuple {
                fields_name: _,
                ref fields_type,
            } => {
                for inner_type in fields_type {
                    Self::build_column_ids_from_data_type(inner_type, column_ids, next_column_id);
                }
            }
            TableDataType::Array(a) => {
                Self::build_column_ids_from_data_type(a.as_ref(), column_ids, next_column_id);
            }
            _ => {
                *next_column_id += 1;
            }
        }
    }

    pub fn build_column_id(&self, next_column_id: &mut u32) -> Self {
        let data_type = self.data_type();

        let column_id = *next_column_id;
        let mut new_next_column_id = *next_column_id;
        let mut column_ids = vec![];

        Self::build_column_ids_from_data_type(data_type, &mut column_ids, &mut new_next_column_id);

        *next_column_id = new_next_column_id;
        Self {
            name: self.name.clone(),
            default_expr: self.default_expr.clone(),
            data_type: self.data_type.clone(),
            column_id,
        }
    }

    pub fn contain_column_id(&self, column_id: u32) -> bool {
        self.column_ids().contains(&column_id)
    }

    // `column_ids` contains nest-type parent column id,
    // if field is Tuple(t1, t2), it will return a column id vector of 3 column id.
    // `leaf_column_ids` return only the child column id.
    pub fn leaf_column_ids(&self) -> Vec<u32> {
        let h: BTreeSet<u32> = BTreeSet::from_iter(self.column_ids().iter().cloned());
        h.into_iter().sorted().collect()
    }

    pub fn column_ids(&self) -> Vec<u32> {
        let mut column_ids = vec![];
        let mut new_next_column_id = self.column_id;
        Self::build_column_ids_from_data_type(
            self.data_type(),
            &mut column_ids,
            &mut new_next_column_id,
        );

        column_ids
    }

    pub fn column_id(&self) -> u32 {
        self.column_id
    }

    #[must_use]
    pub fn with_default_expr(mut self, default_expr: Option<String>) -> Self {
        self.default_expr = default_expr;
        self
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn data_type(&self) -> &TableDataType {
        &self.data_type
    }

    pub fn default_expr(&self) -> Option<&String> {
        self.default_expr.as_ref()
    }

    #[inline]
    pub fn is_nullable(&self) -> bool {
        self.data_type.is_nullable()
    }

    #[inline]
    pub fn is_nullable_or_null(&self) -> bool {
        self.data_type.is_nullable_or_null()
    }
}

impl From<&TableDataType> for DataType {
    fn from(data_type: &TableDataType) -> DataType {
        match data_type {
            TableDataType::Null => DataType::Null,
            TableDataType::EmptyArray => DataType::EmptyArray,
            TableDataType::Boolean => DataType::Boolean,
            TableDataType::String => DataType::String,
            TableDataType::Number(ty) => DataType::Number(*ty),
            TableDataType::Decimal(ty) => DataType::Decimal(*ty),
            TableDataType::Timestamp => DataType::Timestamp,
            TableDataType::Date => DataType::Date,
            TableDataType::Nullable(ty) => DataType::Nullable(Box::new((&**ty).into())),
            TableDataType::Array(ty) => DataType::Array(Box::new((&**ty).into())),
            TableDataType::Map(ty) => DataType::Map(Box::new((&**ty).into())),
            TableDataType::Tuple { fields_type, .. } => {
                DataType::Tuple(fields_type.iter().map(Into::into).collect())
            }
            TableDataType::Variant => DataType::Variant,
        }
    }
}

impl TableDataType {
    pub fn wrap_nullable(&self) -> Self {
        match self {
            TableDataType::Nullable(_) => self.clone(),
            _ => Self::Nullable(Box::new(self.clone())),
        }
    }

    pub fn is_nullable(&self) -> bool {
        matches!(self, &TableDataType::Nullable(_))
    }

    pub fn is_nullable_or_null(&self) -> bool {
        matches!(self, &TableDataType::Nullable(_) | &TableDataType::Null)
    }

    pub fn can_inside_nullable(&self) -> bool {
        !self.is_nullable_or_null()
    }

    pub fn remove_nullable(&self) -> Self {
        match self {
            TableDataType::Nullable(ty) => (**ty).clone(),
            _ => self.clone(),
        }
    }

    pub fn remove_recursive_nullable(&self) -> Self {
        match self {
            TableDataType::Nullable(ty) => ty.as_ref().remove_recursive_nullable(),
            TableDataType::Tuple {
                fields_name,
                fields_type,
            } => {
                let mut new_fields_type = vec![];
                for ty in fields_type {
                    new_fields_type.push(ty.remove_recursive_nullable());
                }
                TableDataType::Tuple {
                    fields_name: fields_name.clone(),
                    fields_type: new_fields_type,
                }
            }
            TableDataType::Array(ty) => {
                TableDataType::Array(Box::new(ty.as_ref().remove_recursive_nullable()))
            }
            _ => self.clone(),
        }
    }

    pub fn wrapped_display(&self) -> String {
        match self {
            TableDataType::Nullable(inner_ty) => {
                format!("Nullable({})", inner_ty.wrapped_display())
            }
            TableDataType::Tuple { fields_type, .. } => {
                format!(
                    "Tuple({})",
                    fields_type.iter().map(|ty| ty.wrapped_display()).join(", ")
                )
            }
            TableDataType::Array(inner_ty) => {
                format!("Array({})", inner_ty.wrapped_display())
            }
            _ => format!("{}", self),
        }
    }

    pub fn sql_name(&self) -> String {
        match self {
            TableDataType::Number(num_ty) => match num_ty {
                NumberDataType::UInt8 => "TINYINT UNSIGNED".to_string(),
                NumberDataType::UInt16 => "SMALLINT UNSIGNED".to_string(),
                NumberDataType::UInt32 => "INT UNSIGNED".to_string(),
                NumberDataType::UInt64 => "BIGINT UNSIGNED".to_string(),
                NumberDataType::Int8 => "TINYINT".to_string(),
                NumberDataType::Int16 => "SMALLINT".to_string(),
                NumberDataType::Int32 => "INT".to_string(),
                NumberDataType::Int64 => "BIGINT".to_string(),
                NumberDataType::Float32 => "FLOAT".to_string(),
                NumberDataType::Float64 => "DOUBLE".to_string(),
            },
            TableDataType::String => "VARCHAR".to_string(),
            TableDataType::Nullable(inner_ty) => format!("{} NULL", inner_ty.sql_name()),
            _ => self.to_string().to_uppercase(),
        }
    }

    pub fn create_random_column(&self, len: usize) -> BlockEntry {
        match self {
            TableDataType::Null => BlockEntry {
                data_type: DataType::Null,
                value: Value::Column(Column::Null { len }),
            },
            TableDataType::EmptyArray => BlockEntry {
                data_type: DataType::EmptyArray,
                value: Value::Column(Column::EmptyArray { len }),
            },
            TableDataType::Boolean => BlockEntry {
                data_type: DataType::Boolean,
                value: Value::Column(BooleanType::from_data(
                    (0..len).map(|_| SmallRng::from_entropy().gen_bool(0.5)),
                )),
            },
            TableDataType::String => BlockEntry {
                data_type: DataType::String,
                value: Value::Column(StringType::from_data((0..len).map(|_| {
                    let rng = SmallRng::from_entropy();
                    rng.sample_iter(&Alphanumeric)
                        // randomly generate 5 characters.
                        .take(5)
                        .map(u8::from)
                        .collect::<Vec<_>>()
                }))),
            },
            TableDataType::Number(num_ty) => BlockEntry {
                data_type: DataType::Number(*num_ty),
                value: Value::Column(with_number_mapped_type!(|NUM_TYPE| match num_ty {
                    NumberDataType::NUM_TYPE => NumberType::<NUM_TYPE>::from_data(
                        (0..len).map(|_| SmallRng::from_entropy().gen())
                    ),
                })),
            },
            // useless for now.
            TableDataType::Decimal(t) => match t {
                DecimalDataType::Decimal128(x) => BlockEntry {
                    data_type: DataType::Decimal(*t),
                    value: Value::Column(Column::Decimal(DecimalColumn::Decimal128(
                        vec![0i128; len].into(),
                        *x,
                    ))),
                },
                DecimalDataType::Decimal256(x) => BlockEntry {
                    data_type: DataType::Decimal(*t),
                    value: Value::Column(Column::Decimal(DecimalColumn::Decimal256(
                        vec![i256::ZERO; len].into(),
                        *x,
                    ))),
                },
            },
            TableDataType::Timestamp => BlockEntry {
                data_type: DataType::Timestamp,
                value: Value::Column(TimestampType::from_data(
                    (0..len)
                        .map(|_| SmallRng::from_entropy().gen_range(TIMESTAMP_MIN..=TIMESTAMP_MAX))
                        .collect::<Vec<i64>>(),
                )),
            },
            TableDataType::Date => BlockEntry {
                data_type: DataType::Date,
                value: Value::Column(DateType::from_data(
                    (0..len)
                        .map(|_| SmallRng::from_entropy().gen_range(DATE_MIN..=DATE_MAX))
                        .collect::<Vec<i32>>(),
                )),
            },
            TableDataType::Nullable(inner_ty) => {
                let entry = inner_ty.create_random_column(len);
                BlockEntry {
                    data_type: DataType::Nullable(Box::new(entry.data_type)),
                    value: Value::Column(Column::Nullable(Box::new(NullableColumn {
                        column: entry.value.into_column().unwrap(),
                        validity: Bitmap::from(
                            (0..len)
                                .map(|_| SmallRng::from_entropy().gen_bool(0.5))
                                .collect::<Vec<bool>>(),
                        ),
                    }))),
                }
            }
            TableDataType::Array(inner_ty) => {
                let mut inner_len = 0;
                let mut offsets: Vec<u64> = Vec::with_capacity(len + 1);
                offsets.push(inner_len);
                for _ in 0..len {
                    inner_len += SmallRng::from_entropy().gen_range(0..=3);
                    offsets.push(inner_len);
                }
                let entry = inner_ty.create_random_column(inner_len as usize);
                BlockEntry {
                    data_type: DataType::Array(Box::new(entry.data_type)),
                    value: Value::Column(Column::Array(Box::new(ArrayColumn {
                        values: entry.value.into_column().unwrap(),
                        offsets: offsets.into(),
                    }))),
                }
            }
            TableDataType::Tuple { fields_type, .. } => {
                let mut fields = Vec::with_capacity(len);
                let mut types = Vec::with_capacity(len);
                for field_type in fields_type.iter() {
                    let entry = field_type.create_random_column(len);
                    fields.push(entry.value.into_column().unwrap());
                    types.push(entry.data_type);
                }
                BlockEntry {
                    data_type: DataType::Tuple(types),
                    value: Value::Column(Column::Tuple { fields, len }),
                }
            }
            TableDataType::Variant => {
                let mut data = Vec::with_capacity(len);
                for _ in 0..len {
                    let opt = SmallRng::from_entropy().gen_range(0..=6);
                    let val = match opt {
                        0 => JsonbValue::Null,
                        1 => JsonbValue::Bool(true),
                        2 => JsonbValue::Bool(false),
                        3 => {
                            let s = Alphanumeric.sample_string(&mut rand::thread_rng(), 5);
                            JsonbValue::String(Cow::from(s))
                        }
                        4 => {
                            let num = SmallRng::from_entropy().gen_range(i64::MIN..=i64::MAX);
                            JsonbValue::Number(JsonbNumber::Int64(num))
                        }
                        5 => {
                            let arr_len = SmallRng::from_entropy().gen_range(0..=5);
                            let mut values = Vec::with_capacity(arr_len);
                            for _ in 0..arr_len {
                                let num = SmallRng::from_entropy().gen_range(i64::MIN..=i64::MAX);
                                values.push(JsonbValue::Number(JsonbNumber::Int64(num)))
                            }
                            JsonbValue::Array(values)
                        }
                        6 => {
                            let obj_len = SmallRng::from_entropy().gen_range(0..=5);
                            let mut obj = JsonbObject::new();
                            for _ in 0..obj_len {
                                let k = Alphanumeric.sample_string(&mut rand::thread_rng(), 5);
                                let num = SmallRng::from_entropy().gen_range(i64::MIN..=i64::MAX);
                                let v = JsonbValue::Number(JsonbNumber::Int64(num));
                                obj.insert(k, v);
                            }
                            JsonbValue::Object(obj)
                        }
                        _ => JsonbValue::Null,
                    };
                    data.push(val.to_vec());
                }
                BlockEntry {
                    data_type: DataType::Variant,
                    value: Value::Column(VariantType::from_data(data)),
                }
            }
            _ => todo!(),
        }
    }
}

pub type DataSchemaRef = Arc<DataSchema>;
pub type TableSchemaRef = Arc<TableSchema>;

pub struct DataSchemaRefExt;

pub struct TableSchemaRefExt;

impl DataSchemaRefExt {
    pub fn create(fields: Vec<DataField>) -> DataSchemaRef {
        Arc::new(DataSchema::new(fields))
    }
}

impl TableSchemaRefExt {
    pub fn create(fields: Vec<TableField>) -> TableSchemaRef {
        Arc::new(TableSchema::new(fields))
    }
}

impl From<&ArrowSchema> for TableSchema {
    fn from(a_schema: &ArrowSchema) -> Self {
        let fields = a_schema
            .fields
            .iter()
            .map(|arrow_f| arrow_f.into())
            .collect::<Vec<_>>();

        TableSchema::new(fields)
    }
}

impl From<&TableField> for DataField {
    fn from(f: &TableField) -> Self {
        let data_type = f.data_type.clone();
        let name = f.name.clone();
        DataField::new(&name, DataType::from(&data_type)).with_default_expr(f.default_expr.clone())
    }
}

impl<T: AsRef<TableSchema>> From<T> for DataSchema {
    fn from(t_schema: T) -> Self {
        let fields = t_schema
            .as_ref()
            .fields()
            .iter()
            .map(|t_f| t_f.into())
            .collect::<Vec<_>>();

        DataSchema::new(fields)
    }
}

impl AsRef<TableSchema> for &TableSchema {
    fn as_ref(&self) -> &TableSchema {
        self
    }
}

// conversions code
// =========================
impl From<&ArrowField> for TableField {
    fn from(f: &ArrowField) -> Self {
        Self {
            name: f.name.clone(),
            data_type: f.into(),
            default_expr: None,
            column_id: 0,
        }
    }
}

impl From<&ArrowField> for DataField {
    fn from(f: &ArrowField) -> Self {
        Self {
            name: f.name.clone(),
            data_type: DataType::from(&TableDataType::from(f)),
            default_expr: None,
        }
    }
}

// ArrowType can't map to DataType, we don't know the nullable flag
impl From<&ArrowField> for TableDataType {
    fn from(f: &ArrowField) -> Self {
        let ty = with_number_type!(|TYPE| match f.data_type() {
            ArrowDataType::TYPE => TableDataType::Number(NumberDataType::TYPE),

            ArrowDataType::Decimal(precision, scale) =>
                TableDataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
                    precision: *precision as u8,
                    scale: *scale as u8,
                })),
            ArrowDataType::Decimal256(precision, scale) =>
                TableDataType::Decimal(DecimalDataType::Decimal256(DecimalSize {
                    precision: *precision as u8,
                    scale: *scale as u8,
                })),

            ArrowDataType::Null => return TableDataType::Null,
            ArrowDataType::Boolean => TableDataType::Boolean,

            ArrowDataType::List(f)
            | ArrowDataType::LargeList(f)
            | ArrowDataType::FixedSizeList(f, _) =>
                TableDataType::Array(Box::new(f.as_ref().into())),

            ArrowDataType::Binary
            | ArrowDataType::LargeBinary
            | ArrowDataType::Utf8
            | ArrowDataType::LargeUtf8 => TableDataType::String,

            ArrowDataType::Timestamp(_, _) => TableDataType::Timestamp,
            ArrowDataType::Date32 | ArrowDataType::Date64 => TableDataType::Date,

            ArrowDataType::Struct(fields) => {
                let (fields_name, fields_type) =
                    fields.iter().map(|f| (f.name.clone(), f.into())).unzip();
                TableDataType::Tuple {
                    fields_name,
                    fields_type,
                }
            }
            ArrowDataType::Extension(custom_name, _, _) => match custom_name.as_str() {
                ARROW_EXT_TYPE_VARIANT => TableDataType::Variant,
                ARROW_EXT_TYPE_EMPTY_ARRAY => TableDataType::EmptyArray,
                _ => unimplemented!("data_type: {:?}", f.data_type()),
            },
            // this is safe, because we define the datatype firstly
            _ => {
                unimplemented!("data_type: {:?}", f.data_type())
            }
        });

        if f.is_nullable {
            TableDataType::Nullable(Box::new(ty))
        } else {
            ty
        }
    }
}

impl From<&DataField> for ArrowField {
    fn from(f: &DataField) -> Self {
        let ty = f.data_type().into();
        ArrowField::new(f.name(), ty, f.is_nullable())
    }
}

impl From<&TableField> for ArrowField {
    fn from(f: &TableField) -> Self {
        let ty = f.data_type().into();
        ArrowField::new(f.name(), ty, f.is_nullable())
    }
}

impl From<&DataType> for ArrowDataType {
    fn from(ty: &DataType) -> Self {
        match ty {
            DataType::Null => ArrowDataType::Null,
            DataType::EmptyArray => ArrowDataType::Extension(
                ARROW_EXT_TYPE_EMPTY_ARRAY.to_string(),
                Box::new(ArrowDataType::Null),
                None,
            ),
            DataType::Boolean => ArrowDataType::Boolean,
            DataType::String => ArrowDataType::LargeBinary,
            DataType::Number(ty) => with_number_type!(|TYPE| match ty {
                NumberDataType::TYPE => ArrowDataType::TYPE,
            }),
            DataType::Timestamp => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Date => ArrowDataType::Date32,
            DataType::Nullable(ty) => ty.as_ref().into(),
            DataType::Array(ty) => {
                let arrow_ty = ty.as_ref().into();
                ArrowDataType::LargeList(Box::new(ArrowField::new(
                    "_array",
                    arrow_ty,
                    ty.is_nullable(),
                )))
            }
            DataType::Map(ty) => {
                let arrow_ty = ty.as_ref().into();
                ArrowDataType::LargeList(Box::new(ArrowField::new(
                    "_map",
                    arrow_ty,
                    ty.is_nullable(),
                )))
            }
            DataType::Tuple(types) => {
                let fields = types
                    .iter()
                    .enumerate()
                    .map(|(index, ty)| {
                        let name = format!("{index}");
                        ArrowField::new(name.as_str(), ty.into(), ty.is_nullable())
                    })
                    .collect();
                ArrowDataType::Struct(fields)
            }
            DataType::Variant => ArrowDataType::Extension(
                ARROW_EXT_TYPE_VARIANT.to_string(),
                Box::new(ArrowDataType::LargeBinary),
                None,
            ),

            _ => unreachable!(),
        }
    }
}

impl From<&TableDataType> for ArrowDataType {
    fn from(ty: &TableDataType) -> Self {
        match ty {
            TableDataType::Null => ArrowDataType::Null,
            TableDataType::EmptyArray => ArrowDataType::Extension(
                ARROW_EXT_TYPE_EMPTY_ARRAY.to_string(),
                Box::new(ArrowDataType::Null),
                None,
            ),
            TableDataType::Boolean => ArrowDataType::Boolean,
            TableDataType::String => ArrowDataType::LargeBinary,
            TableDataType::Number(ty) => with_number_type!(|TYPE| match ty {
                NumberDataType::TYPE => ArrowDataType::TYPE,
            }),
            TableDataType::Decimal(DecimalDataType::Decimal128(size)) => {
                ArrowDataType::Decimal(size.precision as usize, size.scale as usize)
            }
            TableDataType::Decimal(DecimalDataType::Decimal256(size)) => {
                ArrowDataType::Decimal256(size.precision as usize, size.scale as usize)
            }
            TableDataType::Timestamp => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            TableDataType::Date => ArrowDataType::Date32,
            TableDataType::Nullable(ty) => ty.as_ref().into(),
            TableDataType::Array(ty) => {
                let arrow_ty = ty.as_ref().into();
                ArrowDataType::LargeList(Box::new(ArrowField::new(
                    "_array",
                    arrow_ty,
                    ty.is_nullable(),
                )))
            }
            TableDataType::Map(ty) => {
                let arrow_ty = ty.as_ref().into();
                ArrowDataType::LargeList(Box::new(ArrowField::new(
                    "_map",
                    arrow_ty,
                    ty.is_nullable(),
                )))
            }
            TableDataType::Tuple {
                fields_name,
                fields_type,
            } => {
                let fields = fields_name
                    .iter()
                    .zip(fields_type)
                    .map(|(name, ty)| ArrowField::new(name.as_str(), ty.into(), ty.is_nullable()))
                    .collect();
                ArrowDataType::Struct(fields)
            }
            TableDataType::Variant => ArrowDataType::Extension(
                ARROW_EXT_TYPE_VARIANT.to_string(),
                Box::new(ArrowDataType::LargeBinary),
                None,
            ),
        }
    }
}

/// Convert a `DataType` to `TableDataType`.
/// Generally, we don't allow to convert `DataType` to `TableDataType` directly.
/// But for some special cases, for example creating table from a query without specifying
/// the schema. Then we need to infer the corresponding `TableDataType` from `DataType`, and
/// this function may report an error if the conversion is not allowed.
///
/// Do not use this function in other places.
pub fn infer_schema_type(data_type: &DataType) -> Result<TableDataType> {
    match data_type {
        DataType::Null => Ok(TableDataType::Null),
        DataType::Boolean => Ok(TableDataType::Boolean),
        DataType::EmptyArray => Ok(TableDataType::EmptyArray),
        DataType::String => Ok(TableDataType::String),
        DataType::Number(number_type) => Ok(TableDataType::Number(*number_type)),
        DataType::Timestamp => Ok(TableDataType::Timestamp),
        DataType::Decimal(x) => Ok(TableDataType::Decimal(*x)),
        DataType::Date => Ok(TableDataType::Date),
        DataType::Nullable(inner_type) => Ok(TableDataType::Nullable(Box::new(infer_schema_type(
            inner_type,
        )?))),
        DataType::Array(elem_type) => Ok(TableDataType::Array(Box::new(infer_schema_type(
            elem_type,
        )?))),
        DataType::Map(inner_type) => {
            Ok(TableDataType::Map(Box::new(infer_schema_type(inner_type)?)))
        }
        DataType::Variant => Ok(TableDataType::Variant),
        DataType::Tuple(fields) => {
            let fields_type = fields
                .iter()
                .map(infer_schema_type)
                .collect::<Result<Vec<_>>>()?;
            let fields_name = fields
                .iter()
                .enumerate()
                .map(|(idx, _)| idx.to_string())
                .collect::<Vec<_>>();
            Ok(TableDataType::Tuple {
                fields_name,
                fields_type,
            })
        }
        DataType::Generic(_) => Err(ErrorCode::SemanticError(format!(
            "Cannot create table with type: {}",
            data_type
        ))),
    }
}

/// Infer TableSchema from DataSchema, this is useful when creating table from a query.
pub fn infer_table_schema(data_schema: &DataSchemaRef) -> Result<TableSchemaRef> {
    let mut fields = Vec::with_capacity(data_schema.fields().len());
    for field in data_schema.fields() {
        let field_type = infer_schema_type(field.data_type())?;
        fields.push(TableField::new(field.name(), field_type));
    }
    Ok(TableSchemaRefExt::create(fields))
}

// a complex schema to cover all data types tests.
pub fn create_test_complex_schema() -> TableSchema {
    let child_field11 = TableDataType::Number(NumberDataType::UInt64);
    let child_field12 = TableDataType::Number(NumberDataType::UInt64);
    let child_field22 = TableDataType::Number(NumberDataType::UInt64);

    let s = TableDataType::Tuple {
        fields_name: vec!["0".to_string(), "1".to_string()],
        fields_type: vec![child_field11, child_field12],
    };

    let tuple = TableDataType::Tuple {
        fields_name: vec!["0".to_string(), "1".to_string()],
        fields_type: vec![s.clone(), TableDataType::Array(Box::new(child_field22))],
    };

    let array = TableDataType::Array(Box::new(s));
    let nullarray = TableDataType::Nullable(Box::new(TableDataType::Array(Box::new(
        TableDataType::Number(NumberDataType::UInt64),
    ))));
    let maparray = TableDataType::Map(Box::new(TableDataType::Array(Box::new(
        TableDataType::Number(NumberDataType::UInt64),
    ))));

    let field1 = TableField::new("u64", TableDataType::Number(NumberDataType::UInt64));
    let field2 = TableField::new("tuplearray", tuple);
    let field3 = TableField::new("arraytuple", array);
    let field4 = TableField::new("nullarray", nullarray);
    let field5 = TableField::new("maparray", maparray);
    let field6 = TableField::new(
        "nullu64",
        TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
    );
    let field7 = TableField::new(
        "u64array",
        TableDataType::Array(Box::new(TableDataType::Number(NumberDataType::UInt64))),
    );
    let field8 = TableField::new("tuplesimple", TableDataType::Tuple {
        fields_name: vec!["a".to_string(), "b".to_string()],
        fields_type: vec![
            TableDataType::Number(NumberDataType::Int32),
            TableDataType::Number(NumberDataType::Int32),
        ],
    });

    TableSchema::new(vec![
        field1, field2, field3, field4, field5, field6, field7, field8,
    ])
}
