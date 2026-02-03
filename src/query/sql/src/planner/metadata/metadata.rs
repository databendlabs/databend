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

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::collections::hash_map::Entry;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Literal;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::InternalColumn;
use databend_common_catalog::table::Table;
use databend_common_expression::ColumnId;
use databend_common_expression::ComputedExpr;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::display::display_tuple_field_name;
use databend_common_expression::is_stream_column_id;
use databend_common_expression::types::DataType;
use jsonb::keypath::OwnedKeyPaths;
use parking_lot::RwLock;

use crate::optimizer::ir::SExpr;

/// Planner use [`usize`] as it's index type.
///
/// This type will be used across the whole planner.
pub type IndexType = usize;

/// Use IndexType::MAX to represent dummy table.
pub const DUMMY_TABLE_INDEX: IndexType = IndexType::MAX;
pub const DUMMY_COLUMN_INDEX: IndexType = IndexType::MAX;

/// ColumnSet represents a set of columns identified by its IndexType.
pub type ColumnSet = BTreeSet<IndexType>;

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
    removed_mark_indexes: ColumnSet,
    /// Table column indexes that are lazy materialized.
    table_lazy_columns: HashMap<IndexType, ColumnSet>,
    table_source: HashMap<IndexType, DataSourcePlan>,
    retained_columns: ColumnSet,
    /// Columns that are lazy materialized.
    lazy_columns: ColumnSet,
    /// Columns that are used for compute lazy materialized.
    /// If outer query match the lazy materialized rule but inner query doesn't,
    /// we need add cols that inner query required to non_lazy_columns
    /// to prevent these cols to be pruned.
    non_lazy_columns: ColumnSet,
    /// Mappings from table index to _row_id column index.
    table_row_id_index: HashMap<IndexType, IndexType>,
    agg_indices: HashMap<String, Vec<(u64, String, SExpr)>>,
    max_column_position: usize, // for CSV

    /// Scan id of each scan operator.
    next_scan_id: usize,
    /// Mappings from base column index to scan id.
    base_column_scan_id: HashMap<IndexType, usize>,
    next_runtime_filter_id: usize,
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
        // Use `rev` is because a table may be queried multiple times in join clause,
        // and the virtual columns should add to the table newly added.
        self.tables
            .iter()
            .rev()
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

    pub fn add_removed_mark_index(&mut self, index: IndexType) {
        self.removed_mark_indexes.insert(index);
    }

    pub fn is_removed_mark_index(&self, index: IndexType) -> bool {
        self.removed_mark_indexes.contains(&index)
    }

    pub fn add_retained_column(&mut self, index: IndexType) {
        self.retained_columns.insert(index);
    }

    pub fn get_retained_column(&self) -> &ColumnSet {
        &self.retained_columns
    }

    pub fn set_table_lazy_columns(&mut self, table_index: IndexType, lazy_columns: ColumnSet) {
        self.table_lazy_columns.insert(table_index, lazy_columns);
    }

    pub fn get_table_lazy_columns(&self, table_index: &IndexType) -> Option<ColumnSet> {
        self.table_lazy_columns.get(table_index).cloned()
    }

    pub fn set_table_source(&mut self, table_index: IndexType, source: DataSourcePlan) {
        self.table_source.insert(table_index, source);
    }

    pub fn get_table_source(&self, table_index: &IndexType) -> Option<&DataSourcePlan> {
        self.table_source.get(table_index)
    }

    pub fn is_lazy_column(&self, index: usize) -> bool {
        self.lazy_columns.contains(&index)
    }

    pub fn add_lazy_columns(&mut self, indices: ColumnSet) {
        if !self.lazy_columns.is_empty() {
            // `lazy_columns` is only allowed to be set once.
            return;
        }
        debug_assert!(indices.iter().all(|i| *i < self.columns.len()));
        self.lazy_columns.extend(indices);
    }

    pub fn clear_lazy_columns(&mut self) {
        self.lazy_columns.clear();
    }

    pub fn add_non_lazy_columns(&mut self, indices: ColumnSet) {
        debug_assert!(indices.iter().all(|i| *i < self.columns.len()));
        self.non_lazy_columns.extend(indices);
    }

    pub fn lazy_columns(&self) -> &ColumnSet {
        &self.lazy_columns
    }

    pub fn non_lazy_columns(&self) -> &ColumnSet {
        &self.non_lazy_columns
    }

    pub fn set_table_row_id_index(&mut self, table_index: IndexType, row_id_index: IndexType) {
        self.table_row_id_index.insert(table_index, row_id_index);
    }

    pub fn row_id_index_by_table_index(&self, table_index: IndexType) -> Option<IndexType> {
        self.table_row_id_index.get(&table_index).copied()
    }

    pub fn row_id_indexes(&self) -> Vec<IndexType> {
        self.table_row_id_index.values().copied().collect()
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
                ColumnEntry::VirtualColumn(VirtualColumn { table_index, .. }) => {
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

    #[allow(clippy::too_many_arguments)]
    pub fn add_base_table_column(
        &mut self,
        name: String,
        data_type: TableDataType,
        table_index: IndexType,
        path_indices: Option<Vec<IndexType>>,
        column_id: ColumnId,
        column_position: Option<usize>,
        virtual_expr: Option<String>,
    ) -> IndexType {
        let column_index = self.columns.len();
        let column_entry = ColumnEntry::BaseTableColumn(BaseTableColumn {
            column_name: name,
            column_position,
            data_type,
            column_index,
            table_index,
            path_indices,
            column_id,
            virtual_expr,
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
        source_column_id: u32,
        column_id: u32,
        column_name: String,
        key_paths: OwnedKeyPaths,
        data_type: TableDataType,
        is_try: bool,
    ) -> IndexType {
        let column_index = self.columns.len();
        let column = ColumnEntry::VirtualColumn(VirtualColumn {
            table_index,
            source_column_name,
            source_column_id,
            column_id,
            column_index,
            column_name,
            key_paths,
            data_type,
            is_try,
        });
        self.columns.push(column);
        column_index
    }

    pub fn add_agg_indices(&mut self, table: String, agg_indices: Vec<(u64, String, SExpr)>) {
        match self.agg_indices.entry(table) {
            Entry::Occupied(occupied) => occupied.into_mut().extend(agg_indices),
            Entry::Vacant(vacant) => {
                vacant.insert(agg_indices);
            }
        }
    }

    pub fn agg_indices(&self) -> &HashMap<String, Vec<(u64, String, SExpr)>> {
        &self.agg_indices
    }

    pub fn replace_agg_indices(&mut self, agg_indices: HashMap<String, Vec<(u64, String, SExpr)>>) {
        self.agg_indices = agg_indices
    }

    pub fn get_agg_indices(&self, table: &str) -> Option<&[(u64, String, SExpr)]> {
        self.agg_indices.get(table).map(|v| v.as_slice())
    }

    pub fn has_agg_indices(&self) -> bool {
        !self.agg_indices.is_empty()
    }

    fn remove_cte_suffix(mut table_name: String, cte_suffix_name: Option<String>) -> String {
        if let Some(suffix) = cte_suffix_name {
            if table_name.ends_with(&suffix) {
                table_name.truncate(table_name.len() - suffix.len() - 1);
            }
        }
        table_name
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_table(
        &mut self,
        catalog: String,
        database: String,
        table_meta: Arc<dyn Table>,
        branch: Option<String>,
        table_alias_name: Option<String>,
        source_of_view: bool,
        source_of_index: bool,
        source_of_stage: bool,
        cte_suffix_name: Option<String>,
    ) -> IndexType {
        let table_name = table_meta.name().to_string();
        let table_name = Self::remove_cte_suffix(table_name, cte_suffix_name);

        let table_index = self.tables.len();
        // If exists table alias name, use it instead of origin name
        let table_entry = TableEntry {
            index: table_index,
            name: table_name,
            database,
            catalog,
            table: table_meta.clone(),
            branch,
            alias_name: table_alias_name,
            source_of_view,
            source_of_index,
            source_of_stage,
        };
        self.tables.push(table_entry);
        let table_schema = table_meta.schema_with_stream();
        let mut index = 0;
        let mut fields = VecDeque::with_capacity(table_schema.fields().len());
        for field in table_schema.fields().iter() {
            if let Some(ComputedExpr::Virtual(_)) = field.computed_expr() {
                fields.push_back((vec![], field.clone()));
            } else {
                fields.push_back((vec![index], field.clone()));
                index += 1;
            }
        }

        while let Some((indices, field)) = fields.pop_front() {
            if indices.is_empty() {
                self.add_base_table_column(
                    field.name().clone(),
                    field.data_type().clone(),
                    table_index,
                    None,
                    field.column_id,
                    None,
                    Some(field.computed_expr().unwrap().expr().clone()),
                );
                continue;
            }
            let path_indices = if indices.len() > 1 {
                Some(indices.clone())
            } else {
                None
            };

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
                    field.column_id,
                    None,
                    None,
                );

                let mut inner_column_id = field.column_id;
                for (index, (inner_field_name, inner_field_type)) in
                    fields_name.iter().zip(fields_type.iter()).enumerate()
                {
                    let mut inner_indices = indices.clone();
                    inner_indices.push(index);
                    // create tuple inner field
                    let inner_name = format!(
                        "{}:{}",
                        field.name(),
                        display_tuple_field_name(inner_field_name)
                    );
                    let inner_field = TableField::new_from_column_id(
                        &inner_name,
                        inner_field_type.clone(),
                        inner_column_id,
                    );
                    inner_column_id += inner_field_type.num_leaf_columns() as u32;
                    fields.push_front((inner_indices, inner_field));
                }
            } else {
                self.add_base_table_column(
                    field.name().clone(),
                    field.data_type().clone(),
                    table_index,
                    path_indices,
                    field.column_id,
                    Some(indices[0] + 1),
                    None,
                );
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

    pub fn set_max_column_position(&mut self, max_pos: usize) {
        self.max_column_position = max_pos
    }

    pub fn get_max_column_position(&self) -> usize {
        self.max_column_position
    }

    pub fn next_scan_id(&mut self) -> usize {
        let next_scan_id = self.next_scan_id;
        self.next_scan_id += 1;
        next_scan_id
    }

    pub fn next_runtime_filter_id(&mut self) -> usize {
        let next_runtime_filter_id = self.next_runtime_filter_id;
        self.next_runtime_filter_id += 1;
        next_runtime_filter_id
    }

    pub fn add_base_column_scan_id(&mut self, base_column_scan_id: HashMap<usize, usize>) {
        self.base_column_scan_id.extend(base_column_scan_id);
    }

    pub fn base_column_scan_id(&self, column_index: usize) -> Option<usize> {
        self.base_column_scan_id.get(&column_index).cloned()
    }

    pub fn replace_all_tables(&mut self, table: Arc<dyn Table>) {
        for entry in self.tables.iter_mut() {
            entry.table = table.clone();
        }
    }
}

#[derive(Clone)]
pub struct TableEntry {
    catalog: String,
    database: String,
    name: String,
    branch: Option<String>,
    alias_name: Option<String>,
    index: IndexType,
    source_of_view: bool,

    /// If this table is bound to an index.
    source_of_index: bool,

    source_of_stage: bool,
    table: Arc<dyn Table>,
}

impl Debug for TableEntry {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("TableEntry")
            .field("catalog", &self.catalog)
            .field("database", &self.database)
            .field("name", &self.name)
            .field("index", &self.index)
            .finish_non_exhaustive()
    }
}

impl TableEntry {
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

    pub fn branch(&self) -> &Option<String> {
        &self.branch
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

    /// Return true if it is source from stage.
    pub fn is_source_of_stage(&self) -> bool {
        self.source_of_stage
    }

    /// Return true if it is bound for an index.
    pub fn is_source_of_index(&self) -> bool {
        self.source_of_index
    }

    pub fn update_table_index(&mut self, table_index: IndexType) {
        self.index = table_index;
    }

    pub fn qualified_name(&self) -> String {
        match &self.branch {
            None => format!("{}.{}.{}", self.catalog, self.database, self.name),
            Some(branch) => format!(
                "{}.{}.{}/{}",
                self.catalog, self.database, self.name, branch
            ),
        }
    }
}

#[derive(Clone, Debug)]
pub struct BaseTableColumn {
    pub table_index: IndexType,
    pub column_index: IndexType,
    pub column_name: String,
    // column_position inside table schema
    pub column_position: Option<usize>,
    pub data_type: TableDataType,

    /// Path indices for inner column of struct data type.
    pub path_indices: Option<Vec<usize>>,
    /// The column id in table schema.
    pub column_id: ColumnId,
    /// Virtual computed expression, generated in query.
    pub virtual_expr: Option<String>,
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
    pub source_column_id: u32,
    pub column_id: u32,
    pub column_index: IndexType,
    pub column_name: String,
    pub key_paths: OwnedKeyPaths,
    pub data_type: TableDataType,
    /// try cast to target type or not
    pub is_try: bool,
}

#[allow(clippy::large_enum_variant)]
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

    pub fn data_type(&self) -> DataType {
        match self {
            ColumnEntry::BaseTableColumn(BaseTableColumn { data_type, .. }) => {
                DataType::from(data_type)
            }
            ColumnEntry::DerivedColumn(DerivedColumn { data_type, .. }) => data_type.clone(),
            ColumnEntry::InternalColumn(TableInternalColumn {
                internal_column, ..
            }) => internal_column.data_type(),
            ColumnEntry::VirtualColumn(VirtualColumn { data_type, .. }) => {
                DataType::from(data_type)
            }
        }
    }

    pub fn name(&self) -> String {
        match self {
            ColumnEntry::BaseTableColumn(BaseTableColumn { column_name, .. }) => {
                column_name.to_string()
            }
            ColumnEntry::DerivedColumn(DerivedColumn { alias, .. }) => alias.to_string(),
            ColumnEntry::InternalColumn(TableInternalColumn {
                internal_column, ..
            }) => internal_column.column_name.clone(),
            ColumnEntry::VirtualColumn(VirtualColumn { column_name, .. }) => {
                column_name.to_string()
            }
        }
    }

    pub fn table_index(&self) -> Option<IndexType> {
        match self {
            ColumnEntry::BaseTableColumn(BaseTableColumn { table_index, .. }) => Some(*table_index),
            ColumnEntry::DerivedColumn(_) => None,
            ColumnEntry::InternalColumn(TableInternalColumn { table_index, .. }) => {
                Some(*table_index)
            }
            ColumnEntry::VirtualColumn(VirtualColumn { table_index, .. }) => Some(*table_index),
        }
    }

    pub fn is_stream_column(&self) -> bool {
        if let ColumnEntry::BaseTableColumn(BaseTableColumn { column_id, .. }) = self {
            if is_stream_column_id(*column_id) {
                return true;
            }
        }
        false
    }
}

pub fn optimize_remove_count_args(name: &str, distinct: bool, args: &[&Expr]) -> bool {
    name.eq_ignore_ascii_case("count")
        && !distinct
        && args
            .iter()
            .all(|expr| matches!(expr, Expr::Literal { value,.. } if *value != Literal::Null))
}
