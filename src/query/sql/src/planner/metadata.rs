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

use common_ast::ast::Expr;
use common_ast::ast::Literal;
use common_catalog::plan::InternalColumn;
use common_catalog::table::Table;
use common_expression::types::DataType;
use common_expression::Scalar;
use common_expression::TableDataType;
use common_expression::TableField;
use parking_lot::RwLock;

/// Planner use [`usize`] as it's index type.
///
/// This type will be used across the whole planner.
pub type IndexType = usize;

/// Use IndexType::MAX to represent dummy table.
pub static DUMMY_TABLE_INDEX: IndexType = IndexType::MAX;
pub static DUMMY_COLUMN_INDEX: IndexType = IndexType::MAX;

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
        self.columns.iter().find_map(|v| match v {
            ColumnEntry::BaseTableColumn(BaseTableColumn {
                column_index,
                table_index,
                ..
            }) if column_indexes.contains(column_index) => Some(*table_index),
            _ => None,
        })
    }

    pub fn get_table_index(
        &self,
        database_name: Option<&str>,
        table_name: &str,
    ) -> Option<IndexType> {
        self.tables
            .iter()
            .find(|table| match database_name {
                Some(database_name) => {
                    table.database == database_name && table.name == table_name
                        || table.alias_name == Some(table_name.to_string())
                }
                None => {
                    table.name == table_name || table.alias_name == Some(table_name.to_string())
                }
            })
            .map(|table| table.index)
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
            .filter(|column| match column {
                ColumnEntry::BaseTableColumn(BaseTableColumn { table_index, .. }) => {
                    index == *table_index
                }
                ColumnEntry::InternalColumn(TableInternalColumn { table_index, .. }) => {
                    index == *table_index
                }
                _ => false,
            })
            .cloned()
            .collect()
    }

    pub fn virtual_columns_by_table_index(&self, index: IndexType) -> Vec<ColumnEntry> {
        self.columns
            .iter()
            .filter(|column| match column {
                ColumnEntry::VirtualColumn(VirtualColumn { table_index, .. }) => {
                    index == *table_index
                }
                _ => false,
            })
            .cloned()
            .collect()
    }

    pub fn add_base_table_column(
        &mut self,
        name: String,
        data_type: TableDataType,
        table_index: IndexType,
        path_indices: Option<Vec<IndexType>>,
        leaf_index: Option<IndexType>,
    ) -> IndexType {
        let column_index = self.columns.len();
        let column_entry = ColumnEntry::BaseTableColumn(BaseTableColumn {
            column_name: name,
            data_type,
            column_index,
            table_index,
            path_indices,
            leaf_index,
        });
        self.columns.push(column_entry);
        column_index
    }

    pub fn add_derived_column(&mut self, alias: String, data_type: DataType) -> IndexType {
        let column_index = self.columns.len();
        let column_entry = ColumnEntry::DerivedColumn(DerivedColumn {
            column_index,
            alias,
            data_type,
        });
        self.columns.push(column_entry);
        column_index
    }

    pub fn add_internal_column(
        &mut self,
        table_index: IndexType,
        internal_column: InternalColumn,
    ) -> IndexType {
        let column_index = self.columns.len();
        self.columns
            .push(ColumnEntry::InternalColumn(TableInternalColumn {
                table_index,
                column_index,
                internal_column,
            }));
        column_index
    }

    pub fn add_virtual_column(
        &mut self,
        table_index: IndexType,
        source_column_name: String,
        source_column_index: IndexType,
        column_name: String,
        data_type: TableDataType,
        paths: Vec<Scalar>,
    ) -> IndexType {
        let column_index = self.columns.len();
        self.columns.push(ColumnEntry::VirtualColumn(VirtualColumn {
            table_index,
            source_column_name,
            source_column_index,
            column_index,
            column_name,
            data_type,
            paths,
        }));
        column_index
    }

    pub fn add_table(
        &mut self,
        catalog: String,
        database: String,
        table_meta: Arc<dyn Table>,
        table_alias_name: Option<String>,
        source_of_view: bool,
    ) -> IndexType {
        let table_name = table_meta.name().to_string();

        let table_index = self.tables.len();
        // If exists table alias name, use it instead of origin name
        let table_entry = TableEntry {
            index: table_index,
            name: table_name,
            database,
            catalog,
            table: table_meta.clone(),
            alias_name: table_alias_name,
            source_of_view,
        };
        self.tables.push(table_entry);
        let mut fields = VecDeque::new();
        for (i, field) in table_meta.schema().fields().iter().enumerate() {
            fields.push_back((vec![i], field.clone()));
        }

        // build leaf index in DFS order for primitive columns.
        let mut leaf_index = 0;
        while let Some((indices, field)) = fields.pop_front() {
            let path_indices = if indices.len() > 1 {
                Some(indices.clone())
            } else {
                None
            };

            // TODO handle Tuple inside Array.
            if let TableDataType::Tuple {
                fields_name,
                fields_type,
            } = field.data_type().remove_nullable()
            {
                self.add_base_table_column(
                    field.name().clone(),
                    field.data_type().clone(),
                    table_index,
                    path_indices,
                    None,
                );

                let mut i = fields_type.len();
                for (inner_field_name, inner_field_type) in
                    fields_name.iter().zip(fields_type.iter()).rev()
                {
                    i -= 1;
                    let mut inner_indices = indices.clone();
                    inner_indices.push(i);
                    // create tuple inner field
                    let inner_name = format!("{}:{}", field.name(), inner_field_name);
                    let inner_field = TableField::new(&inner_name, inner_field_type.clone());
                    fields.push_front((inner_indices, inner_field));
                }
            } else {
                self.add_base_table_column(
                    field.name().clone(),
                    field.data_type().clone(),
                    table_index,
                    path_indices,
                    Some(leaf_index),
                );
                leaf_index += 1;
            }
        }

        table_index
    }

    pub fn change_derived_column_alias(&mut self, index: IndexType, alias: String) {
        let derived_column = self
            .columns
            .get_mut(index)
            .expect("metadata must contain column");
        if let ColumnEntry::DerivedColumn(column) = derived_column {
            column.alias = alias;
        }
    }
}

#[derive(Clone)]
pub struct TableEntry {
    catalog: String,
    database: String,
    name: String,
    alias_name: Option<String>,
    index: IndexType,
    source_of_view: bool,

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
        alias_name: Option<String>,
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
            alias_name,
            source_of_view: false,
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

    /// Get the alias name of this table entry.
    pub fn alias_name(&self) -> &Option<String> {
        &self.alias_name
    }

    /// Get the index this table entry.
    pub fn index(&self) -> IndexType {
        self.index
    }

    /// Get the table of this table entry.
    pub fn table(&self) -> Arc<dyn Table> {
        self.table.clone()
    }

    /// Return true if it is source from view.
    pub fn is_source_of_view(&self) -> bool {
        self.source_of_view
    }
}

#[derive(Clone, Debug)]
pub struct BaseTableColumn {
    pub table_index: IndexType,
    pub column_index: IndexType,
    pub column_name: String,
    pub data_type: TableDataType,

    /// Path indices for inner column of struct data type.
    pub path_indices: Option<Vec<usize>>,
    /// Leaf index is the primitive column index in Parquet, constructed in DFS order.
    /// None if the data type of column is struct.
    pub leaf_index: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct DerivedColumn {
    pub column_index: IndexType,
    pub alias: String,
    pub data_type: DataType,
}

#[derive(Clone, Debug)]
pub struct TableInternalColumn {
    pub table_index: IndexType,
    pub column_index: IndexType,
    pub internal_column: InternalColumn,
}

#[derive(Clone, Debug)]
pub struct VirtualColumn {
    pub table_index: IndexType,
    pub source_column_name: String,
    pub source_column_index: IndexType,
    pub column_index: IndexType,
    pub column_name: String,
    pub data_type: TableDataType,

    /// Paths to generate virtual column from source column
    pub paths: Vec<Scalar>,
}

#[derive(Clone, Debug)]
pub enum ColumnEntry {
    /// Column from base table, for example `SELECT t.a, t.b FROM t`.
    BaseTableColumn(BaseTableColumn),

    /// Column synthesized from other columns, for example `SELECT t.a + t.b AS a FROM t`.
    DerivedColumn(DerivedColumn),

    /// Internal columns, such as `_row_id`, `_segment_name`, etc.
    InternalColumn(TableInternalColumn),

    /// Virtual column generated from source column by paths, such as `a.b.c`, `a[1][2]`, etc.
    VirtualColumn(VirtualColumn),
}

impl ColumnEntry {
    pub fn index(&self) -> IndexType {
        match self {
            ColumnEntry::BaseTableColumn(base) => base.column_index,
            ColumnEntry::DerivedColumn(derived) => derived.column_index,
            ColumnEntry::InternalColumn(internal) => internal.column_index,
            ColumnEntry::VirtualColumn(virtual_column) => virtual_column.column_index,
        }
    }

    pub fn name(&self) -> &String {
        match self {
            ColumnEntry::BaseTableColumn(base) => &base.column_name,
            ColumnEntry::DerivedColumn(derived) => &derived.alias,
            ColumnEntry::InternalColumn(internal) => internal.internal_column.column_name(),
            ColumnEntry::VirtualColumn(virtual_column) => &virtual_column.column_name,
        }
    }
}

pub fn optimize_remove_count_args(name: &str, distinct: bool, args: &[&Expr]) -> bool {
    name.eq_ignore_ascii_case("count")
        && !distinct
        && args
            .iter()
            .all(|expr| matches!(expr, Expr::Literal{lit,..} if *lit!=Literal::Null))
}
