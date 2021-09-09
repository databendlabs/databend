// Copyright 2020 Datafuse Labs.
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

use std::collections::HashMap;
use crate::sql::planner::IndexType;
use common_exception::ErrorCode;
use common_exception::Result;
use common_datavalues::DataType;

#[derive(Debug, Clone)]
pub struct BindContext {
    // Table name -> Table binding index
    pub name_binding_map: HashMap<String, TableBinding>,
    // Append only bindings list, used to access `TableBinding` with index
    pub indexed_bindings: Vec<(String, TableBinding)>,
    // Unique table index counter, should maintained by root BindContext.
    pub table_index_count: IndexType,
}

impl BindContext {
    pub fn new(index: IndexType) -> Self {
        BindContext {
            name_binding_map: HashMap::new(),
            indexed_bindings: Vec::new(),
            table_index_count: index,
        }
    }

    pub fn get_table_binding_by_name(&self, table_name: &str) -> Result<TableBinding> {
        self.name_binding_map
            .get(table_name)
            .cloned()
            .ok_or(ErrorCode::LogicalError(format!(
                "Cannot find table {} in BindContext",
                table_name
            )))
    }

    pub fn get_column_binding_by_column_name(&self, column_name: &str) -> Result<ColumnBinding> {
        for (_, table_binding) in self.indexed_bindings.iter() {
            for (index, col) in table_binding.columns.iter().enumerate() {
                if col.as_str() == column_name {
                    return Ok(ColumnBinding {
                        table_index: table_binding.index,
                        column_index: index,
                    });
                }
            }
        }
        Err(ErrorCode::InvalidColumnAlias(format!(
            "column \"{}\" doesn't exists",
            column_name
        )))
    }

    // Use to generate column bindings for wildcard like `SELECT * FROM t`
    pub fn get_all_column_bindings(&self) -> Result<Vec<(String, ColumnBinding)>> {
        let mut result = vec![];
        for (_, binding) in self.indexed_bindings.iter() {
            result.append(
                &mut binding
                    .columns
                    .iter()
                    .cloned()
                    .zip(binding.get_column_bindings())
                    .collect(),
            );
        }
        Ok(result)
    }

    pub fn add_table_binding(&mut self, table_binding: TableBinding) -> Result<()> {
        if let Some(binding) = self.name_binding_map.get(table_binding.table_name.as_str()) {
            return Err(ErrorCode::DuplicatedName(format!(
                "table name \"{}\" specified more than once",
                binding.table_name
            )));
        }
        self.name_binding_map
            .insert(table_binding.table_name.to_owned(), table_binding.clone());
        self.indexed_bindings
            .push((table_binding.table_name.to_owned(), table_binding));
        Ok(())
    }

    pub fn merge_bind_context(&mut self, bind_context: BindContext) -> Result<()> {
        for (_, binding) in bind_context.indexed_bindings.into_iter() {
            self.add_table_binding(binding)?;
        }
        self.table_index_count = bind_context.table_index_count;
        Ok(())
    }

    pub fn add_table(
        &mut self,
        index: IndexType,
        table_name: String,
        columns: Vec<String>,
        data_types: Vec<DataType>,
    ) -> Result<()> {
        let table_binding = TableBinding::new(index, table_name, columns, data_types);

        self.add_table_binding(table_binding)
    }

    pub fn next_table_index(&mut self) -> IndexType {
        let res = self.table_index_count;
        self.table_index_count += 1;
        res
    }
}

#[derive(Debug, Clone)]
pub struct TableBinding {
    // Index of TableBinding, used in BindContext
    pub index: IndexType,
    // Table name or alias
    pub table_name: String,
    // Column names
    pub columns: Vec<String>,
    // Data types of columns
    pub data_types: Vec<DataType>,
}

impl TableBinding {
    pub fn new(
        index: IndexType,
        table_name: String,
        columns: Vec<String>,
        data_types: Vec<DataType>,
    ) -> Self {
        TableBinding {
            index,
            table_name,
            columns,
            data_types,
        }
    }

    pub fn get_column_bindings(&self) -> Vec<ColumnBinding> {
        self.columns
            .iter()
            .enumerate()
            .map(|(index, _)| ColumnBinding {
                table_index: self.index,
                column_index: index,
            })
            .collect()
    }

    pub fn get_column_by_index(&self, column_index: IndexType) -> Result<(String, DataType)> {
        assert_eq!(self.columns.len(), self.data_types.len());
        if column_index > self.columns.len() {
            Err(ErrorCode::LogicalError(format!(
                "Invalid column index {} for table binding {:?}",
                column_index, &self
            )))
        } else {
            Ok((
                self.columns[column_index].clone(),
                self.data_types[column_index].clone(),
            ))
        }
    }

    pub fn get_column_by_name(&self, column_name: &str) -> Result<(IndexType, DataType)> {
        assert_eq!(self.columns.len(), self.data_types.len());
        for (index, column) in self.columns.iter().enumerate() {
            if column_name == column.as_str() {
                return Ok((index, self.data_types[index].clone()));
            }
        }
        Err(ErrorCode::LogicalError(format!(
            "Cannot find column name {} in table binding {:?}",
            column_name, &self
        )))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnBinding {
    // Index of the table binding this column belongs to, used in BindContext
    pub table_index: IndexType,
    // Index of column binding inside table binding
    pub column_index: IndexType,
}
