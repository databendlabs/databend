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

use core::fmt;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Arc;

use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::types::data_type::DataType;
use crate::types::data_type::DataTypeImpl;
use crate::DataField;
use crate::TypeDeserializerImpl;

/// memory layout.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct DataSchema {
    pub(crate) fields: Vec<DataField>,
    pub(crate) metadata: BTreeMap<String, String>,

    // define new fields as Option for compatibility
    pub max_column_id: Option<u32>,
    column_id_map: Option<BTreeMap<String, u32>>,
    column_id_set: Option<HashSet<u32>>,
}

impl DataSchema {
    pub fn empty() -> Self {
        Self {
            fields: vec![],
            metadata: BTreeMap::new(),
            max_column_id: Some(0),
            column_id_map: Some(BTreeMap::new()),
            column_id_set: Some(HashSet::new()),
        }
    }

    pub fn init_if_need(data_schema: DataSchema) -> Self {
        // If max_column_id is none, it is an old version needs to compatibility
        if data_schema.max_column_id.is_none() {
            Self::new_from(data_schema.fields, data_schema.metadata)
        } else {
            data_schema
        }
    }

    fn build_from_data_type(
        data_type: &DataTypeImpl,
        column_name: &str,
        max_column_id: &mut u32,
        column_id_map: &mut BTreeMap<String, u32>,
        column_id_set: &mut HashSet<u32>,
    ) {
        if let DataTypeImpl::Struct(s) = data_type {
            column_id_map.insert(column_name.to_string(), *max_column_id);
            column_id_set.insert(*max_column_id);
            let inner_types = s.types();
            for (i, inner_type) in inner_types.iter().enumerate() {
                let inner_name = match s.names() {
                    Some(names) => format!("{}:{}", column_name, names[i]),
                    None => format!("{}:{}", column_name, i),
                };
                Self::build_from_data_type(
                    inner_type,
                    &inner_name,
                    max_column_id,
                    column_id_map,
                    column_id_set,
                );
            }
        } else {
            column_id_map.insert(column_name.to_string(), *max_column_id);
            column_id_set.insert(*max_column_id);
            *max_column_id += 1;
        }
    }

    fn build_members_from_fields(
        fields: &[DataField],
        column_id_map: Option<BTreeMap<String, u32>>,
        max_column_id_opt: Option<u32>,
    ) -> (u32, BTreeMap<String, u32>, HashSet<u32>) {
        let mut max_column_id = 0;
        let mut has_column_id_map_inited = false;
        let mut column_id_map = match column_id_map {
            Some(column_id_map) => {
                has_column_id_map_inited = !column_id_map.is_empty();
                column_id_map
            }
            None => BTreeMap::new(),
        };
        let has_max_column_id_inited = match max_column_id_opt {
            Some(max_column_id) => max_column_id > 0,
            None => false,
        };
        // make sure that column_id_map and max_column_id init at the same time
        assert_eq!(has_column_id_map_inited, has_max_column_id_inited);

        let mut column_id_set = HashSet::new();

        let has_inited = has_column_id_map_inited;
        if has_inited {
            column_id_map.values().for_each(|id| {
                column_id_set.insert(*id);
            });
        } else {
            fields.iter().enumerate().for_each(|(_i, f)| {
                let data_type = f.data_type();
                Self::build_from_data_type(
                    data_type,
                    f.name(),
                    &mut max_column_id,
                    &mut column_id_map,
                    &mut column_id_set,
                );
            });
        }

        let new_max_column_id = if has_max_column_id_inited {
            max_column_id_opt.unwrap()
        } else {
            max_column_id
        };
        // check max_column_id_opt value cannot fallback
        assert!(new_max_column_id >= max_column_id);

        (new_max_column_id, column_id_map, column_id_set)
    }

    pub fn new(fields: Vec<DataField>) -> Self {
        let (max_column_id, column_id_map, column_id_set) =
            Self::build_members_from_fields(&fields, None, None);
        Self {
            fields,
            metadata: BTreeMap::new(),
            max_column_id: Some(max_column_id),
            column_id_map: Some(column_id_map),
            column_id_set: Some(column_id_set),
        }
    }

    pub fn new_from(fields: Vec<DataField>, metadata: BTreeMap<String, String>) -> Self {
        let (max_column_id, column_id_map, column_id_set) =
            Self::build_members_from_fields(&fields, None, None);
        Self {
            fields,
            metadata,
            max_column_id: Some(max_column_id),
            column_id_map: Some(column_id_map),
            column_id_set: Some(column_id_set),
        }
    }

    pub fn new_from_column_id_map(
        fields: Vec<DataField>,
        metadata: BTreeMap<String, String>,
        column_id_map: BTreeMap<String, u32>,
        max_column_id: u32,
    ) -> Self {
        let (max_column_id, column_id_map, column_id_set) =
            Self::build_members_from_fields(&fields, Some(column_id_map), Some(max_column_id));
        Self {
            fields,
            metadata,
            max_column_id: Some(max_column_id),
            column_id_map: Some(column_id_map),
            column_id_set: Some(column_id_set),
        }
    }

    #[inline]
    pub fn max_column_id(&self) -> u32 {
        *self.max_column_id.as_ref().unwrap()
    }

    #[inline]
    pub fn column_id_map(&self) -> &BTreeMap<String, u32> {
        self.column_id_map.as_ref().unwrap()
    }

    pub fn column_id_of_path(&self, path_in_schema: &[String]) -> Result<u32> {
        let name = path_in_schema.join(":");
        self.column_id_of(&name)
    }

    /// Find the column id with the given name.
    pub fn column_id_of(&self, name: &str) -> Result<u32> {
        match self.column_id_map.as_ref().unwrap().get(name) {
            Some(column_id) => Ok(*column_id),
            None => {
                return Err(ErrorCode::UnknownColumn(format!("UnknownColumn {}", name,)));
            }
        }
    }

    pub fn column_id_of_index(&self, i: usize) -> Result<u32> {
        self.column_id_of(self.fields[i].name())
    }

    pub fn is_column_deleted(&self, column_id: u32) -> bool {
        self.column_id_set.as_ref().unwrap().contains(&column_id)
    }

    pub fn column_id_set(&self) -> &HashSet<u32> {
        &self.column_id_set.as_ref().unwrap()
    }

    pub fn add_columns(&mut self, fields: &[DataField]) -> Result<()> {
        for f in fields {
            if self.index_of(f.name()).is_ok() {
                return Err(ErrorCode::AddColumnExistError(format!(
                    "add column {} already exist",
                    f.name(),
                )));
            }
            let data_type = f.data_type();
            Self::build_from_data_type(
                data_type,
                f.name(),
                self.max_column_id.as_mut().unwrap(),
                self.column_id_map.as_mut().unwrap(),
                self.column_id_set.as_mut().unwrap(),
            );
            self.fields.push(f.to_owned());
        }
        Ok(())
    }

    fn drop_field(&mut self, data_type: &DataTypeImpl, column_name: &str) -> Result<()> {
        if let DataTypeImpl::Struct(s) = data_type {
            let inner_types = s.types();
            for (i, inner_type) in inner_types.iter().enumerate() {
                let inner_name = format!("{}:{}", column_name, i);
                self.drop_field(inner_type, &inner_name)?;
            }
        }
        let column_id = self.column_id_of(column_name)?;
        self.column_id_map.as_mut().unwrap().remove(column_name);
        self.column_id_set.as_mut().unwrap().remove(&column_id);

        Ok(())
    }

    pub fn drop_column(&mut self, column: &str) -> Result<()> {
        if self.fields.len() == 1 {
            return Err(ErrorCode::DropColumnEmptyError(
                "cannot drop table column to empty",
            ));
        }
        let i = self.index_of(column)?;
        let field = self.fields[i].clone();
        self.drop_field(field.data_type(), column)?;
        self.fields.remove(i);

        Ok(())
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

    /// Check to see if `self` is a superset of `other` schema. Here are the comparision rules:
    pub fn contains(&self, other: &DataSchema) -> bool {
        if self.fields.len() != other.fields.len() {
            return false;
        }

        for (i, field) in other.fields.iter().enumerate() {
            if !self.fields[i].contains(field) {
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
        Self::new_from_column_id_map(
            fields,
            self.meta().clone(),
            self.column_id_map.as_ref().unwrap().clone(),
            *self.max_column_id.as_ref().unwrap(),
        )
    }

    /// project with inner columns by path.
    pub fn inner_project(&self, path_indices: &BTreeMap<usize, Vec<usize>>) -> Self {
        let paths: Vec<Vec<usize>> = path_indices.values().cloned().collect();
        let fields = paths
            .iter()
            .map(|path| Self::traverse_paths(self.fields(), path).unwrap())
            .collect();
        Self::new_from_column_id_map(
            fields,
            self.meta().clone(),
            self.column_id_map.as_ref().unwrap().clone(),
            *self.max_column_id.as_ref().unwrap(),
        )
    }

    fn traverse_paths(fields: &[DataField], path: &[usize]) -> Result<DataField> {
        if path.is_empty() {
            return Err(ErrorCode::BadArguments("path should not be empty"));
        }
        let field = &fields[path[0]];
        if path.len() == 1 {
            return Ok(field.clone());
        }

        let field_name = field.name();
        if let DataTypeImpl::Struct(struct_type) = &field.data_type() {
            let inner_types = struct_type.types();
            let inner_names = match struct_type.names() {
                Some(inner_names) => inner_names
                    .iter()
                    .map(|name| format!("{}:{}", field_name, name.to_lowercase()))
                    .collect::<Vec<_>>(),
                None => (0..inner_types.len())
                    .map(|i| format!("{}:{}", field_name, i + 1))
                    .collect::<Vec<_>>(),
            };

            let inner_fields = inner_names
                .iter()
                .zip(inner_types.iter())
                .map(|(inner_name, inner_type)| {
                    DataField::new(&inner_name.clone(), inner_type.clone())
                })
                .collect::<Vec<DataField>>();
            return Self::traverse_paths(&inner_fields, &path[1..]);
        }
        let valid_fields: Vec<String> = fields.iter().map(|f| f.name().clone()).collect();
        Err(ErrorCode::BadArguments(format!(
            "Unable to get field paths. Valid fields: {:?}",
            valid_fields
        )))
    }

    /// project will do column pruning.
    #[must_use]
    pub fn project_by_fields(&self, fields: Vec<DataField>) -> Self {
        Self::new_from_column_id_map(
            fields,
            self.meta().clone(),
            self.column_id_map.as_ref().unwrap().clone(),
            *self.max_column_id.as_ref().unwrap(),
        )
    }

    pub fn to_arrow(&self) -> ArrowSchema {
        let fields = self
            .fields()
            .iter()
            .map(|f| f.to_arrow())
            .collect::<Vec<_>>();
        ArrowSchema::from(fields).with_metadata(self.metadata.clone())
    }

    fn data_type_column_ids(
        &self,
        column_name: &str,
        data_type: &DataTypeImpl,
        column_ids: &mut Vec<u32>,
    ) -> Result<()> {
        if let DataTypeImpl::Struct(s) = data_type {
            let inner_types = s.types();
            for (i, inner_type) in inner_types.iter().enumerate() {
                let inner_name = match s.names() {
                    Some(names) => format!("{}:{}", column_name, names[i]),
                    None => format!("{}:{}", column_name, i),
                };

                self.data_type_column_ids(&inner_name, inner_type, column_ids)?;
            }
        } else {
            column_ids.push(self.column_id_of(column_name)?);
        }
        Ok(())
    }

    pub fn to_column_ids(&self) -> Result<Vec<u32>> {
        let mut column_ids = Vec::with_capacity(self.fields.len());
        for field in &self.fields {
            let data_type = field.data_type();
            self.data_type_column_ids(field.name(), data_type, &mut column_ids)?;
        }

        Ok(column_ids)
    }

    pub fn create_deserializers(&self, capacity: usize) -> Vec<TypeDeserializerImpl> {
        let mut deserializers = Vec::with_capacity(self.num_fields());
        for field in self.fields() {
            let data_type = field.data_type();
            deserializers.push(data_type.create_deserializer(capacity));
        }
        deserializers
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

#[allow(clippy::needless_borrow)]
impl From<ArrowSchema> for DataSchema {
    fn from(a_schema: ArrowSchema) -> Self {
        (&a_schema).into()
    }
}

impl From<ArrowSchemaRef> for DataSchema {
    fn from(a_schema: ArrowSchemaRef) -> Self {
        (a_schema.as_ref()).into()
    }
}

impl fmt::Display for DataSchema {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(
            &self
                .fields
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<String>>()
                .join(", "),
        )
    }
}
