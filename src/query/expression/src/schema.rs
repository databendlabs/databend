// Copyright 2021 Datafuse Labs
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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use common_arrow::arrow::datatypes::DataType as ArrowDataType;
use common_arrow::arrow::datatypes::Field as ArrowField;
use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::datatypes::TimeUnit;
use common_exception::ErrorCode;
use common_exception::Result;
use itertools::Itertools;
use serde::Deserialize;
use serde::Serialize;

use crate::types::decimal::DecimalDataType;
use crate::types::decimal::DecimalSize;
use crate::types::DataType;
use crate::types::NumberDataType;
use crate::with_number_type;
use crate::BlockMetaInfo;
use crate::BlockMetaInfoDowncast;
use crate::Scalar;
use crate::ARROW_EXT_TYPE_BITMAP;
use crate::ARROW_EXT_TYPE_EMPTY_ARRAY;
use crate::ARROW_EXT_TYPE_EMPTY_MAP;
use crate::ARROW_EXT_TYPE_VARIANT;

// Column id of TableField
pub type ColumnId = u32;
// Index of TableSchema.fields array
pub type FieldIndex = usize;

pub const ROW_ID_COLUMN_ID: u32 = u32::MAX;
pub const BLOCK_NAME_COLUMN_ID: u32 = u32::MAX - 1;
pub const SEGMENT_NAME_COLUMN_ID: u32 = u32::MAX - 2;
pub const SNAPSHOT_NAME_COLUMN_ID: u32 = u32::MAX - 3;

pub const ROW_ID_COL_NAME: &str = "_row_id";
pub const SNAPSHOT_NAME_COL_NAME: &str = "_snapshot_name";
pub const SEGMENT_NAME_COL_NAME: &str = "_segment_name";
pub const BLOCK_NAME_COL_NAME: &str = "_block_name";

#[inline]
pub fn is_internal_column_id(column_id: ColumnId) -> bool {
    column_id >= SNAPSHOT_NAME_COLUMN_ID
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DataSchema {
    pub fields: Vec<DataField>,
    pub(crate) metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum ComputedExpr {
    Virtual(String),
    Stored(String),
}

impl ComputedExpr {
    #[inline]
    pub fn expr(&self) -> &String {
        match self {
            ComputedExpr::Virtual(expr) => expr,
            ComputedExpr::Stored(expr) => expr,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct DataField {
    name: String,
    default_expr: Option<String>,
    data_type: DataType,
    computed_expr: Option<ComputedExpr>,
}

fn uninit_column_id() -> ColumnId {
    0
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TableSchema {
    pub fields: Vec<TableField>,
    pub metadata: BTreeMap<String, String>,
    // next column id that assign to TableField.column_id
    #[serde(default = "uninit_column_id")]
    pub next_column_id: ColumnId,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TableField {
    pub name: String,
    pub default_expr: Option<String>,
    pub data_type: TableDataType,
    #[serde(default = "uninit_column_id")]
    pub column_id: ColumnId,
    pub computed_expr: Option<ComputedExpr>,
}

/// DataType with more information that is only available for table field, e.g, the
/// tuple field name, or the scale of decimal.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum TableDataType {
    Null,
    EmptyArray,
    EmptyMap,
    Boolean,
    String,
    Number(NumberDataType),
    Decimal(DecimalDataType),
    Timestamp,
    Date,
    Nullable(Box<TableDataType>),
    Array(Box<TableDataType>),
    Map(Box<TableDataType>),
    Bitmap,
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

    /// Returns an immutable reference of a specific `Field` instance selected using an
    /// offset within the internal `fields` vector.
    pub fn field(&self, i: FieldIndex) -> &DataField {
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
    pub fn index_of(&self, name: &str) -> Result<FieldIndex> {
        for i in (0..self.fields.len()).rev() {
            // Use `rev` is because unnest columns will be attached to end of schema,
            // but their names are the same as the original column.
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
    pub fn column_with_name(&self, name: &str) -> Option<(FieldIndex, &DataField)> {
        self.fields
            .iter()
            .enumerate()
            .find(|&(_, c)| c.name() == name)
    }

    pub fn set_field_type(&mut self, i: FieldIndex, data_type: DataType) {
        self.fields[i].data_type = data_type;
    }

    pub fn rename_field(&mut self, i: FieldIndex, new_name: &str) {
        self.fields[i].name = new_name.to_string();
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
    pub fn project(&self, projection: &[FieldIndex]) -> Self {
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
        next_column_id: ColumnId,
    ) -> (u32, Vec<TableField>) {
        if next_column_id > 0 {
            // make sure that field column id has been inited.
            if fields.len() > 1 {
                assert!(fields[1].column_id() > 0 || fields[0].column_id() > 0);
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
        next_column_id: ColumnId,
    ) -> Self {
        let (next_column_id, new_fields) = Self::build_members_from_fields(fields, next_column_id);
        Self {
            fields: new_fields,
            metadata,
            next_column_id,
        }
    }

    #[inline]
    pub fn next_column_id(&self) -> ColumnId {
        self.next_column_id
    }

    pub fn column_id_of_index(&self, i: FieldIndex) -> Result<ColumnId> {
        Ok(self.fields[i].column_id())
    }

    pub fn column_id_of(&self, name: &str) -> Result<ColumnId> {
        let i = self.index_of(name)?;
        Ok(self.fields[i].column_id())
    }

    pub fn is_column_deleted(&self, column_id: ColumnId) -> bool {
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

    pub fn add_column(&mut self, field: &TableField, index: usize) -> Result<()> {
        if self.index_of(field.name()).is_ok() {
            return Err(ErrorCode::AddColumnExistError(format!(
                "add column {} already exist",
                field.name(),
            )));
        }
        let field = field.build_column_id(&mut self.next_column_id);
        self.fields.insert(index, field);
        Ok(())
    }

    // Every internal column has constant column id, no need to generate column id of internal columns.
    pub fn add_internal_field(
        &mut self,
        name: &str,
        data_type: TableDataType,
        column_id: ColumnId,
    ) {
        let field = TableField::new_from_column_id(name, data_type, column_id);
        self.fields.push(field);
    }

    pub fn remove_internal_fields(&mut self) {
        self.fields.retain(|f| !is_internal_column_id(f.column_id));
    }

    pub fn drop_column(&mut self, column: &str) -> Result<FieldIndex> {
        if self.fields.len() == 1 {
            return Err(ErrorCode::DropColumnEmptyError(
                "cannot drop table column to empty",
            ));
        }
        let i = self.index_of(column)?;
        self.fields.remove(i);

        Ok(i)
    }

    pub fn to_leaf_column_id_set(&self) -> HashSet<ColumnId> {
        HashSet::from_iter(self.to_leaf_column_ids().iter().cloned())
    }

    pub fn to_column_ids(&self) -> Vec<ColumnId> {
        let mut column_ids = Vec::with_capacity(self.fields.len());

        self.fields.iter().for_each(|f| {
            column_ids.extend(f.column_ids());
        });

        column_ids
    }

    pub fn to_leaf_column_ids(&self) -> Vec<ColumnId> {
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
    pub fn field_column_ids(&self) -> Vec<Vec<ColumnId>> {
        let mut field_column_ids = Vec::with_capacity(self.fields.len());

        self.fields.iter().for_each(|f| {
            field_column_ids.push(f.column_ids());
        });

        field_column_ids
    }

    #[inline]
    pub fn field_leaf_column_ids(&self) -> Vec<Vec<ColumnId>> {
        let mut field_column_ids = Vec::with_capacity(self.fields.len());

        self.fields.iter().for_each(|f| {
            field_column_ids.push(f.leaf_column_ids());
        });

        field_column_ids
    }

    pub fn field_leaf_default_values(
        &self,
        default_values: &[Scalar],
    ) -> HashMap<ColumnId, Scalar> {
        fn collect_leaf_default_values(
            default_value: &Scalar,
            column_ids: &[ColumnId],
            index: &mut usize,
            leaf_default_values: &mut HashMap<ColumnId, Scalar>,
        ) {
            match default_value {
                Scalar::Tuple(s) => {
                    s.iter().for_each(|default_val| {
                        collect_leaf_default_values(
                            default_val,
                            column_ids,
                            index,
                            leaf_default_values,
                        )
                    });
                }
                _ => {
                    leaf_default_values.insert(column_ids[*index], default_value.to_owned());
                    *index += 1;
                }
            }
        }

        let mut leaf_default_values = HashMap::with_capacity(self.num_fields());
        let leaf_field_column_ids = self.field_leaf_column_ids();
        for (default_value, field_column_ids) in default_values.iter().zip_eq(leaf_field_column_ids)
        {
            let mut index = 0;
            collect_leaf_default_values(
                default_value,
                &field_column_ids,
                &mut index,
                &mut leaf_default_values,
            );
        }

        leaf_default_values
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

    pub fn rename_field(&mut self, i: FieldIndex, new_name: &str) {
        self.fields[i].name = new_name.to_string();
    }

    /// Returns an immutable reference of a specific `Field` instance selected using an
    /// offset within the internal `fields` vector.
    pub fn field(&self, i: FieldIndex) -> &TableField {
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
    pub fn index_of(&self, name: &str) -> Result<FieldIndex> {
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
    pub fn column_with_name(&self, name: &str) -> Option<(FieldIndex, &TableField)> {
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
    pub fn project(&self, projection: &[FieldIndex]) -> Self {
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

    /// Project with inner columns by path.
    pub fn inner_project(&self, path_indices: &BTreeMap<FieldIndex, Vec<FieldIndex>>) -> Self {
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

    // Returns all inner column ids of the given column, including itself,
    // only tuple columns may have inner fields, like `a.1`, `a:b`.
    pub fn leaf_columns_of(&self, col_name: &String) -> Vec<ColumnId> {
        fn collect_inner_column_ids(
            col_name: &String,
            field_name: &String,
            data_type: &TableDataType,
            column_ids: &mut Vec<ColumnId>,
            next_column_id: &mut ColumnId,
        ) -> bool {
            if col_name == field_name {
                let n = data_type.num_leaf_columns();
                for i in 0..n {
                    column_ids.push(*next_column_id + i as u32);
                }
                return true;
            }

            if let TableDataType::Tuple {
                fields_name,
                fields_type,
            } = data_type
            {
                if col_name.starts_with(field_name) {
                    for ((i, inner_field_name), inner_field_type) in
                        fields_name.iter().enumerate().zip(fields_type.iter())
                    {
                        let inner_name = format!("{}:{}", field_name, inner_field_name);
                        if col_name.starts_with(&inner_name) {
                            return collect_inner_column_ids(
                                col_name,
                                &inner_name,
                                inner_field_type,
                                column_ids,
                                next_column_id,
                            );
                        }
                        let inner_name = format!("{}:{}", field_name, i + 1);
                        if col_name.starts_with(&inner_name) {
                            return collect_inner_column_ids(
                                col_name,
                                &inner_name,
                                inner_field_type,
                                column_ids,
                                next_column_id,
                            );
                        }
                        *next_column_id += inner_field_type.num_leaf_columns() as u32;
                    }
                }
            }
            false
        }

        let mut column_ids = Vec::new();
        for field in self.fields() {
            let mut next_column_id = field.column_id;
            if collect_inner_column_ids(
                col_name,
                &field.name,
                &field.data_type,
                &mut column_ids,
                &mut next_column_id,
            ) {
                break;
            }
        }

        column_ids
    }

    fn traverse_paths(
        fields: &[TableField],
        path: &[FieldIndex],
        column_ids: &[ColumnId],
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

        // If the data type is Tuple, we can read the inner columns directly.
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

    /// Return leaf fields with column id.
    ///
    /// Note: the name of the inner fields is a full-path name.
    ///
    /// For example, if the field is `s Tuple(f1 Tuple(f2 Int32))`
    /// its name will be `s:f1:f2` instead of `f2`.
    pub fn leaf_fields(&self) -> Vec<TableField> {
        fn collect_in_field(
            field: &TableField,
            fields: &mut Vec<TableField>,
            is_nullable: bool,
            next_column_id: &mut ColumnId,
        ) {
            let ty = field.data_type();
            let is_nullable = ty.is_nullable() || is_nullable;
            match ty.remove_nullable() {
                TableDataType::Tuple {
                    fields_type,
                    fields_name,
                } => {
                    for (name, ty) in fields_name.iter().zip(fields_type) {
                        collect_in_field(
                            &TableField::new_from_column_id(
                                &format!("{}:{}", field.name(), name),
                                ty.clone(),
                                *next_column_id,
                            ),
                            fields,
                            is_nullable,
                            next_column_id,
                        );
                    }
                }
                TableDataType::Array(ty) => {
                    collect_in_field(
                        &TableField::new_from_column_id(
                            &format!("{}:0", field.name()),
                            ty.as_ref().to_owned(),
                            *next_column_id,
                        ),
                        fields,
                        is_nullable,
                        next_column_id,
                    );
                }
                TableDataType::Map(ty) => {
                    collect_in_field(
                        &TableField::new_from_column_id(
                            field.name(),
                            ty.as_ref().to_owned(),
                            *next_column_id,
                        ),
                        fields,
                        is_nullable,
                        next_column_id,
                    );
                }
                _ => {
                    *next_column_id += 1;
                    let mut field = field.clone();
                    if is_nullable {
                        field.data_type = field.data_type.wrap_nullable();
                    }
                    fields.push(field)
                }
            }
        }

        let mut fields = Vec::new();
        for field in self.fields() {
            if is_internal_column_id(field.column_id) {
                // Skip internal columns
                continue;
            }
            let mut next_column_id = field.column_id;
            collect_in_field(field, &mut fields, false, &mut next_column_id);
        }
        fields
    }

    /// project will do column pruning.
    #[must_use]
    pub fn project_by_fields(&self, fields: &BTreeMap<FieldIndex, TableField>) -> Self {
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

    #[must_use]
    pub fn remove_computed_fields(&self) -> Self {
        let new_fields = self
            .fields()
            .iter()
            .filter(|f| f.computed_expr().is_none())
            .cloned()
            .collect::<Vec<_>>();

        Self {
            fields: new_fields,
            metadata: self.metadata.clone(),
            next_column_id: self.next_column_id,
        }
    }

    #[must_use]
    pub fn remove_virtual_computed_fields(&self) -> Self {
        let new_fields = self
            .fields()
            .iter()
            .filter(|f| !matches!(f.computed_expr(), Some(ComputedExpr::Virtual(_))))
            .cloned()
            .collect::<Vec<_>>();

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
}

impl DataField {
    pub fn new(name: &str, data_type: DataType) -> Self {
        DataField {
            name: name.to_string(),
            default_expr: None,
            data_type,
            computed_expr: None,
        }
    }

    pub fn new_nullable(name: &str, data_type: DataType) -> Self {
        DataField {
            name: name.to_string(),
            default_expr: None,
            data_type: data_type.wrap_nullable(),
            computed_expr: None,
        }
    }

    #[must_use]
    pub fn with_default_expr(mut self, default_expr: Option<String>) -> Self {
        self.default_expr = default_expr;
        self
    }

    pub fn with_computed_expr(mut self, computed_expr: Option<ComputedExpr>) -> Self {
        self.computed_expr = computed_expr;
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

    pub fn computed_expr(&self) -> Option<&ComputedExpr> {
        self.computed_expr.as_ref()
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
            computed_expr: None,
        }
    }

    pub fn new_from_column_id(name: &str, data_type: TableDataType, column_id: ColumnId) -> Self {
        TableField {
            name: name.to_string(),
            default_expr: None,
            data_type,
            column_id,
            computed_expr: None,
        }
    }

    fn build_column_ids_from_data_type(
        data_type: &TableDataType,
        column_ids: &mut Vec<ColumnId>,
        next_column_id: &mut ColumnId,
    ) {
        column_ids.push(*next_column_id);
        match data_type.remove_nullable() {
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
            TableDataType::Map(a) => {
                Self::build_column_ids_from_data_type(a.as_ref(), column_ids, next_column_id);
            }
            _ => {
                *next_column_id += 1;
            }
        }
    }

    pub fn build_column_id(&self, next_column_id: &mut ColumnId) -> Self {
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
            computed_expr: self.computed_expr.clone(),
        }
    }

    pub fn contain_column_id(&self, column_id: ColumnId) -> bool {
        self.column_ids().contains(&column_id)
    }

    // `leaf_column_ids` return only the child column id.
    // if field is Tuple(t1, t2), it will return a column id vector of 2 column id.
    pub fn leaf_column_ids(&self) -> Vec<ColumnId> {
        let mut column_ids = self.column_ids();
        column_ids.sort();
        column_ids.dedup();
        column_ids
    }

    // `column_ids` contains nest-type parent column id,
    // if field is Tuple(t1, t2), it will return a column id vector of 3 column id.
    pub fn column_ids(&self) -> Vec<ColumnId> {
        let mut column_ids = vec![];
        let mut new_next_column_id = self.column_id;
        Self::build_column_ids_from_data_type(
            self.data_type(),
            &mut column_ids,
            &mut new_next_column_id,
        );

        column_ids
    }

    pub fn column_id(&self) -> ColumnId {
        self.column_id
    }

    #[must_use]
    pub fn with_default_expr(mut self, default_expr: Option<String>) -> Self {
        self.default_expr = default_expr;
        self
    }

    pub fn with_computed_expr(mut self, computed_expr: Option<ComputedExpr>) -> Self {
        self.computed_expr = computed_expr;
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

    pub fn computed_expr(&self) -> Option<&ComputedExpr> {
        self.computed_expr.as_ref()
    }

    #[inline]
    pub fn is_nullable(&self) -> bool {
        self.data_type.is_nullable()
    }

    #[inline]
    pub fn is_nullable_or_null(&self) -> bool {
        self.data_type.is_nullable_or_null()
    }

    pub fn is_nested(&self) -> bool {
        matches!(
            self.data_type,
            TableDataType::Tuple { .. } | TableDataType::Array(_) | TableDataType::Map(_)
        )
    }
}

impl From<&TableDataType> for DataType {
    fn from(data_type: &TableDataType) -> DataType {
        match data_type {
            TableDataType::Null => DataType::Null,
            TableDataType::EmptyArray => DataType::EmptyArray,
            TableDataType::EmptyMap => DataType::EmptyMap,
            TableDataType::Boolean => DataType::Boolean,
            TableDataType::String => DataType::String,
            TableDataType::Number(ty) => DataType::Number(*ty),
            TableDataType::Decimal(ty) => DataType::Decimal(*ty),
            TableDataType::Timestamp => DataType::Timestamp,
            TableDataType::Date => DataType::Date,
            TableDataType::Nullable(ty) => DataType::Nullable(Box::new((&**ty).into())),
            TableDataType::Array(ty) => DataType::Array(Box::new((&**ty).into())),
            TableDataType::Map(ty) => DataType::Map(Box::new((&**ty).into())),
            TableDataType::Bitmap => DataType::Bitmap,
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
            TableDataType::Map(ty) => {
                TableDataType::Map(Box::new(ty.as_ref().remove_recursive_nullable()))
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
            TableDataType::Map(inner_ty) => match *inner_ty.clone() {
                TableDataType::Tuple {
                    fields_name: _fields_name,
                    fields_type,
                } => {
                    format!(
                        "Map({}, {})",
                        fields_type[0].wrapped_display(),
                        fields_type[1].wrapped_display()
                    )
                }
                _ => unreachable!(),
            },
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

    // Returns the number of leaf columns of the TableDataType
    pub fn num_leaf_columns(&self) -> usize {
        match self {
            TableDataType::Nullable(box inner_ty)
            | TableDataType::Array(box inner_ty)
            | TableDataType::Map(box inner_ty) => inner_ty.num_leaf_columns(),
            TableDataType::Tuple { fields_type, .. } => fields_type
                .iter()
                .map(|inner_ty| inner_ty.num_leaf_columns())
                .sum(),
            _ => 1,
        }
    }
}

pub type DataSchemaRef = Arc<DataSchema>;
pub type TableSchemaRef = Arc<TableSchema>;

#[typetag::serde(name = "data_schema_meta")]
impl BlockMetaInfo for DataSchemaRef {
    #[allow(clippy::borrowed_box)]
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        DataSchemaRef::downcast_ref_from(info).is_some_and(|other| other == self)
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

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
        DataField::new(&name, DataType::from(&data_type))
            .with_default_expr(f.default_expr.clone())
            .with_computed_expr(f.computed_expr.clone())
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
            computed_expr: None,
        }
    }
}

impl From<&ArrowField> for DataField {
    fn from(f: &ArrowField) -> Self {
        Self {
            name: f.name.clone(),
            data_type: DataType::from(&TableDataType::from(f)),
            default_expr: None,
            computed_expr: None,
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
            | ArrowDataType::FixedSizeBinary(_)
            | ArrowDataType::Utf8
            | ArrowDataType::LargeUtf8 => TableDataType::String,

            ArrowDataType::Timestamp(_, _) => TableDataType::Timestamp,
            ArrowDataType::Date32 | ArrowDataType::Date64 => TableDataType::Date,
            ArrowDataType::Map(f, _) => {
                let inner_ty = f.as_ref().into();
                TableDataType::Map(Box::new(inner_ty))
            }
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
                ARROW_EXT_TYPE_EMPTY_MAP => TableDataType::EmptyMap,
                ARROW_EXT_TYPE_BITMAP => TableDataType::Bitmap,
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
        match ty {
            ArrowDataType::Struct(_) if f.is_nullable() => {
                let ty = set_nullable(&ty);
                ArrowField::new(f.name(), ty, f.is_nullable())
            }
            _ => ArrowField::new(f.name(), ty, f.is_nullable()),
        }
    }
}

impl From<&TableField> for ArrowField {
    fn from(f: &TableField) -> Self {
        let ty = f.data_type().into();
        match ty {
            ArrowDataType::Struct(_) if f.is_nullable() => {
                let ty = set_nullable(&ty);
                ArrowField::new(f.name(), ty, f.is_nullable())
            }
            _ => ArrowField::new(f.name(), ty, f.is_nullable()),
        }
    }
}

fn set_nullable(ty: &ArrowDataType) -> ArrowDataType {
    // if the struct type is nullable, need to set inner fields as nullable
    match ty {
        ArrowDataType::Struct(fields) => {
            let fields = fields
                .iter()
                .map(|f| {
                    let data_type = set_nullable(&f.data_type);
                    ArrowField::new(f.name.clone(), data_type, true)
                })
                .collect();
            ArrowDataType::Struct(fields)
        }
        _ => ty.clone(),
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
            DataType::EmptyMap => ArrowDataType::Extension(
                ARROW_EXT_TYPE_EMPTY_MAP.to_string(),
                Box::new(ArrowDataType::Null),
                None,
            ),
            DataType::Boolean => ArrowDataType::Boolean,
            DataType::String => ArrowDataType::LargeBinary,
            DataType::Number(ty) => with_number_type!(|TYPE| match ty {
                NumberDataType::TYPE => ArrowDataType::TYPE,
            }),
            DataType::Decimal(DecimalDataType::Decimal128(s)) => {
                ArrowDataType::Decimal(s.precision.into(), s.scale.into())
            }
            DataType::Decimal(DecimalDataType::Decimal256(s)) => {
                ArrowDataType::Decimal256(s.precision.into(), s.scale.into())
            }
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
                let inner_ty = match ty.as_ref() {
                    DataType::Tuple(tys) => {
                        let key_ty = ArrowDataType::from(&tys[0]);
                        let val_ty = ArrowDataType::from(&tys[1]);
                        let key_field = ArrowField::new("key", key_ty, tys[0].is_nullable());
                        let val_field = ArrowField::new("value", val_ty, tys[1].is_nullable());
                        ArrowDataType::Struct(vec![key_field, val_field])
                    }
                    _ => unreachable!(),
                };
                ArrowDataType::Map(
                    Box::new(ArrowField::new("entries", inner_ty, ty.is_nullable())),
                    false,
                )
            }
            DataType::Bitmap => ArrowDataType::Extension(
                ARROW_EXT_TYPE_BITMAP.to_string(),
                Box::new(ArrowDataType::LargeBinary),
                None,
            ),
            DataType::Tuple(types) => {
                let fields = types
                    .iter()
                    .enumerate()
                    .map(|(index, ty)| {
                        let index = index + 1;
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
            TableDataType::EmptyMap => ArrowDataType::Extension(
                ARROW_EXT_TYPE_EMPTY_MAP.to_string(),
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
                let inner_ty = match ty.as_ref() {
                    TableDataType::Tuple {
                        fields_name: _fields_name,
                        fields_type,
                    } => {
                        let key_ty = ArrowDataType::from(&fields_type[0]);
                        let val_ty = ArrowDataType::from(&fields_type[1]);
                        let key_field =
                            ArrowField::new("key", key_ty, fields_type[0].is_nullable());
                        let val_field =
                            ArrowField::new("value", val_ty, fields_type[1].is_nullable());
                        ArrowDataType::Struct(vec![key_field, val_field])
                    }
                    _ => unreachable!(),
                };
                ArrowDataType::Map(
                    Box::new(ArrowField::new("entries", inner_ty, ty.is_nullable())),
                    false,
                )
            }
            TableDataType::Bitmap => ArrowDataType::Extension(
                ARROW_EXT_TYPE_BITMAP.to_string(),
                Box::new(ArrowDataType::LargeBinary),
                None,
            ),
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
        DataType::EmptyMap => Ok(TableDataType::EmptyMap),
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
        DataType::Bitmap => Ok(TableDataType::Bitmap),
        DataType::Variant => Ok(TableDataType::Variant),
        DataType::Tuple(fields) => {
            let fields_type = fields
                .iter()
                .map(infer_schema_type)
                .collect::<Result<Vec<_>>>()?;
            let fields_name = fields
                .iter()
                .enumerate()
                .map(|(idx, _)| (idx + 1).to_string())
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
        fields.push(
            TableField::new(field.name(), field_type)
                .with_default_expr(field.default_expr.clone())
                .with_computed_expr(field.computed_expr.clone()),
        );
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
    let maparray = TableDataType::Map(Box::new(TableDataType::Tuple {
        fields_name: vec!["key".to_string(), "value".to_string()],
        fields_type: vec![
            TableDataType::Number(NumberDataType::UInt64),
            TableDataType::String,
        ],
    }));

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
