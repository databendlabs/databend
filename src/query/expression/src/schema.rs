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

use std::collections::BTreeMap;
use std::sync::Arc;

use common_arrow::arrow::datatypes::DataType as ArrowDataType;
use common_arrow::arrow::datatypes::Field as ArrowField;
use common_arrow::arrow::datatypes::Schema as ArrowSchema;

use crate::error::Result;
use crate::types::DataType;
use crate::types::NumberDataType;
use crate::with_number_type;

#[derive(serde::Serialize, serde::Deserialize, Debug, Eq, PartialEq, Clone)]
pub struct DataField {
    name: String,
    /// default_expr is serialized representation from PlanExpression
    default_expr: Option<String>,
    data_type: DataType,
}

impl DataField {
    pub fn new(name: &str, data_type: DataType) -> Self {
        DataField {
            name: name.to_string(),
            default_expr: None,
            data_type,
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

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct DataSchema {
    pub(crate) fields: Vec<DataField>,
    pub(crate) metadata: BTreeMap<String, String>,
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

        Err((
            None,
            format!(
                "Unable to get field named \"{}\". Valid fields: {:?}",
                name, valid_fields
            ),
        ))
    }

    /// Look up a column by name and return a immutable reference to the column along with
    /// its index.
    pub fn column_with_name(&self, name: &str) -> Option<(usize, &DataField)> {
        self.fields
            .iter()
            .enumerate()
            .find(|&(_, c)| c.name() == name)
    }

    /// Check to see if `self` is a superset of `other` schema. Here are the comparision rules:
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

    /// project with inner columns by path.
    pub fn inner_project(&self, path_indices: &BTreeMap<usize, Vec<usize>>) -> Self {
        let paths: Vec<Vec<usize>> = path_indices.values().cloned().collect();
        let fields = paths
            .iter()
            .map(|path| Self::traverse_paths(self.fields(), path).unwrap())
            .collect();
        Self::new_from(fields, self.meta().clone())
    }

    fn traverse_paths(fields: &[DataField], path: &[usize]) -> Result<DataField> {
        if path.is_empty() {
            return Err((None, "path should not be empty".to_string()));
        }
        let field = &fields[path[0]];
        if path.len() == 1 {
            return Ok(field.clone());
        }

        let field_name = field.name();
        if let DataType::Tuple(tys) = &field.data_type() {
            let inner_names = (0..tys.len())
                .map(|i| format!("{}:{}", field_name, i))
                .collect::<Vec<_>>();

            let inner_fields = inner_names
                .iter()
                .zip(tys.iter())
                .map(|(inner_name, inner_type)| {
                    DataField::new(&inner_name.clone(), inner_type.clone())
                })
                .collect::<Vec<DataField>>();
            return Self::traverse_paths(&inner_fields, &path[1..]);
        }
        let valid_fields: Vec<String> = fields.iter().map(|f| f.name().clone()).collect();
        Err((
            None,
            format!(
                "Unable to get field paths. Valid fields: {:?}",
                valid_fields
            ),
        ))
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

pub type DataSchemaRef = Arc<DataSchema>;

pub struct DataSchemaRefExt;

impl DataSchemaRefExt {
    pub fn create(fields: Vec<DataField>) -> DataSchemaRef {
        Arc::new(DataSchema::new(fields))
    }
}

impl From<&ArrowSchema> for DataSchema {
    fn from(a_schema: &ArrowSchema) -> Self {
        let fields = a_schema
            .fields
            .iter()
            .map(|arrow_f| arrow_f.into())
            .collect::<Vec<_>>();

        DataSchema::new(fields)
    }
}

// conversions code
// =========================
impl From<&ArrowField> for DataField {
    fn from(f: &ArrowField) -> Self {
        Self {
            name: f.name.clone(),
            data_type: f.into(),
            default_expr: None,
        }
    }
}

// ArrowType can't map to DataType, we don't know the nullable flag
impl From<&ArrowField> for DataType {
    fn from(f: &ArrowField) -> Self {
        let ty = with_number_type!(|TYPE| match f.data_type() {
            ArrowDataType::TYPE => DataType::Number(NumberDataType::TYPE),

            ArrowDataType::Null => DataType::Null,
            ArrowDataType::Boolean => DataType::Boolean,

            ArrowDataType::List(f)
            | ArrowDataType::LargeList(f)
            | ArrowDataType::FixedSizeList(f, _) => DataType::Array(Box::new(f.as_ref().into())),

            ArrowDataType::Binary
            | ArrowDataType::LargeBinary
            | ArrowDataType::Utf8
            | ArrowDataType::LargeUtf8 => DataType::String,

            ArrowDataType::Timestamp(_, _) => DataType::Timestamp,
            ArrowDataType::Date32 | ArrowDataType::Date64 => DataType::Date,

            ArrowDataType::Struct(fields) => {
                let types = fields.iter().map(|f| f.into()).collect();
                DataType::Tuple(types)
            }
            ArrowDataType::Extension(custom_name, _, _) => match custom_name.as_str() {
                "Variant" => DataType::Variant,
                _ => unimplemented!("data_type: {:?}", f.data_type()),
            },
            // this is safe, because we define the datatype firstly
            _ => {
                unimplemented!("data_type: {:?}", f.data_type())
            }
        });

        if f.is_nullable {
            DataType::Nullable(Box::new(ty))
        } else {
            ty
        }
    }
}

impl Into<ArrowField> for &DataField {
    fn into(self) -> ArrowField {
        let ty = self.data_type().into();
        ArrowField::new(self.name(), ty, self.is_nullable())
    }
}

impl Into<ArrowDataType> for &DataType {
    fn into(self) -> ArrowDataType {
        match self {
            DataType::Number(ty) => with_number_type!(|TYPE| match ty {
                NumberDataType::TYPE => ArrowDataType::TYPE,
            }),
            DataType::Boolean => ArrowDataType::Boolean,
            DataType::String => ArrowDataType::LargeBinary,
            DataType::Timestamp => ArrowDataType::Date64,
            DataType::Date => ArrowDataType::Date32,
            DataType::Interval => ArrowDataType::Int64,
            DataType::Nullable(ty) => ty.as_ref().into(),
            DataType::Null => ArrowDataType::Null,
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
            DataType::Tuple(tys) => {
                let arrow_tys: Vec<ArrowDataType> = tys.iter().map(|ty| ty.into()).collect();
                let fields = arrow_tys
                    .iter()
                    .zip(tys.iter())
                    .enumerate()
                    .map(|(idx, (at, ty))| {
                        let name = format!("_map_{idx}");
                        ArrowField::new(name.as_str(), at.clone(), ty.is_nullable())
                    })
                    .collect();
                ArrowDataType::Struct(fields)
            }
            DataType::EmptyArray => ArrowDataType::Extension(
                "EmptyArray".to_string(),
                Box::new(ArrowDataType::Null),
                None,
            ),
            DataType::Variant => ArrowDataType::Extension(
                "Variant".to_string(),
                Box::new(ArrowDataType::LargeBinary),
                None,
            ),
            DataType::Generic(_) => unreachable!(),
        }
    }
}
