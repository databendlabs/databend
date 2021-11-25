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

use std::marker;
use std::sync::Arc;

use crate::errors::Result;
use crate::types::block::ColumnIdx;
use crate::types::Block;
use crate::types::Column;
use crate::types::ColumnType;
use crate::types::FromSql;
use crate::types::SqlType;

/// A row from Clickhouse
pub struct Row<'a, K: ColumnType> {
    pub(crate) row: usize,
    pub(crate) block_ref: BlockRef<'a, K>,
    pub(crate) kind: marker::PhantomData<K>,
}

impl<'a, K: ColumnType> Row<'a, K> {
    /// Get the value of a particular cell of the row.
    pub fn get<T, I>(&'a self, col: I) -> Result<T>
    where
        T: FromSql<'a>,
        I: ColumnIdx + Copy,
    {
        self.block_ref.get(self.row, col)
    }

    /// Return the number of cells in the current row.
    pub fn len(&self) -> usize {
        self.block_ref.column_count()
    }

    /// Returns `true` if the row contains no cells.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the name of a particular cell of the row.
    pub fn name<I: ColumnIdx + Copy>(&self, col: I) -> Result<&str> {
        Ok(self.block_ref.get_column(col)?.name())
    }

    /// Get the type of a particular cell of the row.
    pub fn sql_type<I: ColumnIdx + Copy>(&self, col: I) -> Result<SqlType> {
        Ok(self.block_ref.get_column(col)?.sql_type())
    }
}

#[allow(dead_code)]
pub(crate) enum BlockRef<'a, K: ColumnType> {
    Borrowed(&'a Block<K>),
    Owned(Arc<Block<K>>),
}

impl<'a, K: ColumnType> Clone for BlockRef<'a, K> {
    fn clone(&self) -> Self {
        match self {
            BlockRef::Borrowed(block_ref) => BlockRef::Borrowed(*block_ref),
            BlockRef::Owned(block_ref) => BlockRef::Owned(block_ref.clone()),
        }
    }
}

impl<'a, K: ColumnType> BlockRef<'a, K> {
    fn row_count(&self) -> usize {
        match self {
            BlockRef::Borrowed(block) => block.row_count(),
            BlockRef::Owned(block) => block.row_count(),
        }
    }

    fn column_count(&self) -> usize {
        match self {
            BlockRef::Borrowed(block) => block.column_count(),
            BlockRef::Owned(block) => block.column_count(),
        }
    }

    fn get<'s, T, I>(&'s self, row: usize, col: I) -> Result<T>
    where
        T: FromSql<'s>,
        I: ColumnIdx + Copy,
    {
        match self {
            BlockRef::Borrowed(block) => block.get(row, col),
            BlockRef::Owned(block) => block.get(row, col),
        }
    }

    fn get_column<I: ColumnIdx + Copy>(&self, col: I) -> Result<&Column<K>> {
        match self {
            BlockRef::Borrowed(block) => {
                let column_index = col.get_index(block.columns())?;
                Ok(&block.columns[column_index])
            }
            BlockRef::Owned(block) => {
                let column_index = col.get_index(block.columns())?;
                Ok(&block.columns[column_index])
            }
        }
    }
}

/// Immutable rows iterator
pub struct Rows<'a, K: ColumnType> {
    pub(crate) row: usize,
    pub(crate) block_ref: BlockRef<'a, K>,
    pub(crate) kind: marker::PhantomData<K>,
}

impl<'a, K: ColumnType> Iterator for Rows<'a, K> {
    type Item = Row<'a, K>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row >= self.block_ref.row_count() {
            return None;
        }
        let result = Some(Row {
            row: self.row,
            block_ref: self.block_ref.clone(),
            kind: marker::PhantomData,
        });
        self.row += 1;
        result
    }
}
