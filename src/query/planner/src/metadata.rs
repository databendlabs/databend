// Copyright 2022 Datafuse Labs.
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

use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use common_catalog::table::Table;
use common_datavalues::DataField;
use common_datavalues::DataType;
use common_datavalues::DataTypeImpl;
use common_datavalues::StructType;
use common_datavalues::TypeID;
use parking_lot::RwLock;

/// Planner use [`usize`] as it's index type.
///
/// This type will be used across the whole planner.
pub type IndexType = usize;

/// Use IndexType::MAX to represent dummy table.
pub static DUMMY_TABLE_INDEX: IndexType = IndexType::MAX;

/// ColumnSet represents a set of columns identified by its IndexType.
pub type ColumnSet = HashSet<IndexType>;

/// A Send & Send version of [`Metadata`].
///
/// Callers can clone this ref safely and cheaply.
pub type MetadataRef = Arc<RwLock<Metadata>>;

/// Metadata stores information about columns and tables used in a query.
/// Tables and columns are identified with its unique index.
/// Notice that index value of a column can be same with that of a table.
#[derive(Clone, Debug, Default)]
pub struct Metadata {
    tables: Vec<TableEntry>,
    columns: Vec<ColumnEntry>,
}

impl Metadata {
    pub fn table(&self, index: IndexType) -> &TableEntry {
        self.tables.get(index).expect("metadata must contain table")
    }

    pub fn tables(&self) -> &[TableEntry] {
        self.tables.as_slice()
    }

    pub fn table_index_by_column_indexes(&self, column_indexes: &ColumnSet) -> Option<IndexType> {
        self.columns
            .iter()
            .find(|v| column_indexes.contains(&v.column_index))
            .and_then(|v| v.table_index)
    }

    pub fn column(&self, index: IndexType) -> &ColumnEntry {
        self.columns
            .get(index)
            .expect("metadata must contain column")
    }

    pub fn columns(&self) -> &[ColumnEntry] {
        self.columns.as_slice()
    }

    pub fn columns_by_table_index(&self, index: IndexType) -> Vec<ColumnEntry> {
        self.columns
            .iter()
            .filter(|v| v.table_index == Some(index))
            .cloned()
            .collect()
    }

    pub fn add_column(
        &mut self,
        name: String,
        data_type: DataTypeImpl,
        table_index: Option<IndexType>,
        path_indices: Option<Vec<IndexType>>,
    ) -> IndexType {
        let column_index = self.columns.len();
        let column_entry =
            ColumnEntry::new(name, data_type, column_index, table_index, path_indices);
        self.columns.push(column_entry);
        column_index
    }

    pub fn add_table(
        &mut self,
        catalog: String,
        database: String,
        table_meta: Arc<dyn Table>,
    ) -> IndexType {
        let table_name = table_meta.name().to_string();
        let table_index = self.tables.len();
        let table_entry = TableEntry {
            index: table_index,
            name: table_name,
            database,
            catalog,
            table: table_meta.clone(),
        };
        self.tables.push(table_entry);
        let mut struct_fields = VecDeque::new();
        for (i, field) in table_meta.schema().fields().iter().enumerate() {
            self.add_column(
                field.name().clone(),
                field.data_type().clone(),
                Some(table_index),
                None,
            );
            if field.data_type().data_type_id() == TypeID::Struct {
                struct_fields.push_back((vec![i], field.clone()));
            }
        }
        // add inner columns of struct column
        while !struct_fields.is_empty() {
            let (path_indices, field) = struct_fields.pop_front().unwrap();
            let struct_type: StructType = field.data_type().clone().try_into().unwrap();

            let inner_types = struct_type.types();
            let inner_names = match struct_type.names() {
                Some(inner_names) => inner_names
                    .iter()
                    .map(|name| format!("{}:{}", field.name(), name))
                    .collect::<Vec<_>>(),
                None => (0..inner_types.len())
                    .map(|i| format!("{}:{}", field.name(), i))
                    .collect::<Vec<_>>(),
            };
            for ((i, inner_name), inner_type) in
                inner_names.into_iter().enumerate().zip(inner_types.iter())
            {
                let mut inner_path_indices = path_indices.clone();
                inner_path_indices.push(i);

                self.add_column(
                    inner_name.clone(),
                    inner_type.clone(),
                    Some(table_index),
                    Some(inner_path_indices.clone()),
                );
                if inner_type.data_type_id() == TypeID::Struct {
                    let inner_field = DataField::new(&inner_name, inner_type.clone());
                    struct_fields.push_back((inner_path_indices, inner_field));
                }
            }
        }
        table_index
    }

    pub fn find_smallest_column_within(&self, indices: &[usize]) -> usize {
        let entries = indices
            .iter()
            .map(|i| self.column(*i).clone())
            .collect::<Vec<_>>();
        find_smallest_column(entries.as_slice())
    }
}

#[derive(Clone)]
pub struct TableEntry {
    catalog: String,
    database: String,
    name: String,
    index: IndexType,

    table: Arc<dyn Table>,
}

impl Debug for TableEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableEntry")
            .field("catalog", &self.catalog)
            .field("database", &self.database)
            .field("name", &self.name)
            .field("index", &self.index)
            .finish_non_exhaustive()
    }
}

impl TableEntry {
    pub fn new(
        index: IndexType,
        name: String,
        catalog: String,
        database: String,
        table: Arc<dyn Table>,
    ) -> Self {
        TableEntry {
            index,
            name,
            catalog,
            database,
            table,
        }
    }

    /// Get the catalog name of this table entry.
    pub fn catalog(&self) -> &str {
        &self.catalog
    }

    /// Get the database name of this table entry.
    pub fn database(&self) -> &str {
        &self.database
    }

    /// Get the name of this table entry.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the index this table entry.
    pub fn index(&self) -> IndexType {
        self.index
    }

    /// Get the table of this table entry.
    pub fn table(&self) -> Arc<dyn Table> {
        self.table.clone()
    }
}

#[derive(Clone, Debug)]
pub struct ColumnEntry {
    pub column_index: IndexType,
    pub name: String,
    pub data_type: DataTypeImpl,

    /// Table index of column entry. None if column is derived from a subquery.
    pub table_index: Option<IndexType>,
    /// Path indices for inner column of struct data type.
    pub path_indices: Option<Vec<IndexType>>,
}

impl ColumnEntry {
    pub fn new(
        name: String,
        data_type: DataTypeImpl,
        column_index: IndexType,
        table_index: Option<IndexType>,
        path_indices: Option<Vec<IndexType>>,
    ) -> Self {
        ColumnEntry {
            column_index,
            name,
            data_type,
            table_index,
            path_indices,
        }
    }
}

/// TODO(xuanwo): migrate this as a function of metadata.
pub fn find_smallest_column(entries: &[ColumnEntry]) -> usize {
    debug_assert!(!entries.is_empty());
    let mut column_indexes = entries
        .iter()
        .map(|entry| entry.column_index)
        .collect::<Vec<IndexType>>();
    column_indexes.sort();
    let mut smallest_index = column_indexes[0];
    let mut smallest_size = usize::MAX;
    for (idx, column_entry) in entries.iter().enumerate() {
        if let Ok(bytes) = column_entry.data_type.data_type_id().numeric_byte_size() {
            if smallest_size > bytes {
                smallest_size = bytes;
                smallest_index = entries[idx].column_index;
            }
        }
    }
    smallest_index
}
