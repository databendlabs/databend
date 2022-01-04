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

use std::cmp;
use std::default::Default;
use std::fmt;
use std::io::Cursor;
use std::io::Read;
use std::marker::PhantomData;
use std::os::raw::c_char;

use byteorder::LittleEndian;
use byteorder::WriteBytesExt;
use chrono_tz::Tz;
use lz4::liblz4::LZ4_compressBound;
use lz4::liblz4::LZ4_compress_default;
use naive_cityhash::cityhash128;

pub use self::block_info::BlockInfo;
pub use self::builder::RCons;
pub use self::builder::RNil;
pub use self::builder::RowBuilder;
use self::chunk_iterator::ChunkIterator;
pub use self::compressed::decompress_buffer;
pub(crate) use self::row::BlockRef;
pub use self::row::Row;
pub use self::row::Rows;
use crate::binary::Encoder;
use crate::binary::ReadEx;
use crate::errors::Error;
use crate::errors::FromSqlError;
use crate::errors::Result;
use crate::protocols;
use crate::types::column;
use crate::types::column::ArcColumnWrapper;
use crate::types::column::Column;
use crate::types::column::ColumnFrom;
use crate::types::ColumnType;
use crate::types::Complex;
use crate::types::FromSql;
use crate::types::Simple;
use crate::types::SqlType;

mod block_info;
mod builder;
mod chunk_iterator;
mod compressed;
mod row;

const INSERT_BLOCK_SIZE: usize = 1_048_576;

const DEFAULT_CAPACITY: usize = 100;

pub trait ColumnIdx {
    fn get_index<K: ColumnType>(&self, columns: &[Column<K>]) -> Result<usize>;
}

pub trait Sliceable {
    fn slice_type() -> SqlType;
}

macro_rules! sliceable {
    ( $($t:ty: $k:ident),* ) => {
        $(
            impl Sliceable for $t {
                fn slice_type() -> SqlType {
                    SqlType::$k
                }
            }
        )*
    };
}

sliceable! {
    u8: UInt8,
    u16: UInt16,
    u32: UInt32,
    u64: UInt64,

    i8: Int8,
    i16: Int16,
    i32: Int32,
    i64: Int64
}

/// Represents Clickhouse Block
#[derive(Default)]
pub struct Block<K: ColumnType = Simple> {
    info: BlockInfo,
    columns: Vec<Column<K>>,
    capacity: usize,
}

impl<L: ColumnType, R: ColumnType> PartialEq<Block<R>> for Block<L> {
    fn eq(&self, other: &Block<R>) -> bool {
        if self.columns.len() != other.columns.len() {
            return false;
        }

        for i in 0..self.columns.len() {
            if self.columns[i] != other.columns[i] {
                return false;
            }
        }

        true
    }
}

impl<K: ColumnType> Clone for Block<K> {
    fn clone(&self) -> Self {
        Self {
            info: self.info,
            columns: self.columns.iter().map(|c| (*c).clone()).collect(),
            capacity: self.capacity,
        }
    }
}

impl<K: ColumnType> AsRef<Block<K>> for Block<K> {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl ColumnIdx for usize {
    #[inline(always)]
    fn get_index<K: ColumnType>(&self, _: &[Column<K>]) -> Result<usize> {
        Ok(*self)
    }
}

impl<'a> ColumnIdx for &'a str {
    fn get_index<K: ColumnType>(&self, columns: &[Column<K>]) -> Result<usize> {
        match columns
            .iter()
            .enumerate()
            .find(|(_, column)| column.name() == *self)
        {
            None => Err(Error::FromSql(FromSqlError::OutOfRange)),
            Some((index, _)) => Ok(index),
        }
    }
}

impl ColumnIdx for String {
    fn get_index<K: ColumnType>(&self, columns: &[Column<K>]) -> Result<usize> {
        self.as_str().get_index(columns)
    }
}

impl Block {
    /// Constructs a new, empty `Block`.
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    /// Constructs a new, empty `Block` with the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            info: Default::default(),
            columns: vec![],
            capacity,
        }
    }

    pub fn load<R>(reader: &mut R, tz: Tz, compress: bool) -> Result<Self>
    where R: Read + ReadEx {
        if compress {
            let mut cr = compressed::make(reader);
            Self::raw_load(&mut cr, tz)
        } else {
            Self::raw_load(reader, tz)
        }
    }

    fn raw_load<R>(reader: &mut R, tz: Tz) -> Result<Block<Simple>>
    where R: ReadEx {
        let mut block = Block::new();
        block.info = BlockInfo::read(reader)?;

        let num_columns = reader.read_uvarint()?;
        let num_rows = reader.read_uvarint()?;

        for _ in 0..num_columns {
            let column = Column::read(reader, num_rows as usize, tz)?;
            block.append_column(column);
        }

        Ok(block)
    }
}

impl<K: ColumnType> Block<K> {
    /// Return the number of rows in the current block.
    pub fn row_count(&self) -> usize {
        self.columns.first().map_or(0, |column| column.len())
    }

    /// Return the number of columns in the current block.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// This method returns a slice of columns.
    #[inline(always)]
    pub fn columns(&self) -> &[Column<K>] {
        &self.columns
    }

    pub fn append_column(&mut self, column: Column<K>) {
        let column_len = column.len();

        if !self.columns.is_empty() && self.row_count() != column_len {
            panic!("all columns in block must have same size.")
        }

        self.columns.push(column);
    }

    /// Get the value of a particular cell of the block.
    pub fn get<'a, T, I>(&'a self, row: usize, col: I) -> Result<T>
    where
        T: FromSql<'a>,
        I: ColumnIdx + Copy,
    {
        let column_index = col.get_index(self.columns())?;
        T::from_sql(self.columns[column_index].at(row))
    }

    /// Add new column into this block
    #[must_use]
    pub fn add_column<S>(self, name: &str, values: S) -> Self
    where S: ColumnFrom {
        self.column(name, values)
    }

    /// Add new column into this block
    #[must_use]
    pub fn column<S>(mut self, name: &str, values: S) -> Self
    where S: ColumnFrom {
        let data = S::column_from::<ArcColumnWrapper>(values);
        let column = column::new_column(name, data);

        self.append_column(column);
        self
    }

    /// Returns true if the block contains no elements.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// This method returns a iterator of rows.
    pub fn rows(&self) -> Rows<K> {
        Rows {
            row: 0,
            block_ref: BlockRef::Borrowed(self),
            kind: PhantomData,
        }
    }

    /// This method is a convenient way to pass row into a block.
    pub fn push<B: RowBuilder>(&mut self, row: B) -> Result<()> {
        row.apply(self)
    }

    /// This method finds a column by identifier.
    pub fn get_column<I>(&self, col: I) -> Result<&Column<K>>
    where I: ColumnIdx + Copy {
        let column_index = col.get_index(self.columns())?;
        let column = &self.columns[column_index];
        Ok(column)
    }
}

#[allow(dead_code)]
impl Block<Simple> {
    pub fn concat(blocks: &[Self]) -> Block<Complex> {
        let first = blocks.first().expect("blocks should not be empty.");

        for block in blocks {
            assert_eq!(
                first.column_count(),
                block.column_count(),
                "all columns should have the same size."
            );
        }

        let num_columns = first.column_count();
        let mut columns = Vec::with_capacity(num_columns);
        for i in 0_usize..num_columns {
            let chunks = blocks.iter().map(|block| &block.columns[i]);
            columns.push(Column::concat(chunks));
        }

        Block {
            info: first.info,
            columns,
            capacity: blocks.iter().map(|b| b.capacity).sum(),
        }
    }
}

#[allow(dead_code)]
impl<K: ColumnType> Block<K> {
    pub(crate) fn cast_to(&self, header: &Block<K>) -> Result<Self> {
        let info = self.info;
        let mut columns = self.columns.clone();
        columns.reverse();

        if header.column_count() != columns.len() {
            return Err(Error::FromSql(FromSqlError::OutOfRange));
        }

        let mut new_columns = Vec::with_capacity(columns.len());
        for column in header.columns() {
            let dst_type = column.sql_type();
            let old_column = columns.pop().unwrap();
            let new_column = old_column.cast_to(dst_type)?;
            new_columns.push(new_column);
        }

        Ok(Block {
            info,
            columns: new_columns,
            capacity: self.capacity,
        })
    }

    pub fn write(&self, encoder: &mut Encoder, compress: bool) {
        if compress {
            let mut tmp_encoder = Encoder::new();
            self.write(&mut tmp_encoder, false);
            let tmp = tmp_encoder.get_buffer();

            let mut buf = Vec::new();
            let size;
            unsafe {
                buf.resize(9 + LZ4_compressBound(tmp.len() as i32) as usize, 0_u8);
                size = LZ4_compress_default(
                    tmp.as_ptr() as *const c_char,
                    (buf.as_mut_ptr() as *mut c_char).add(9),
                    tmp.len() as i32,
                    buf.len() as i32,
                );
            }
            buf.resize(9 + size as usize, 0_u8);

            let buf_len = buf.len() as u32;
            {
                let mut cursor = Cursor::new(&mut buf);
                cursor.write_u8(0x82).unwrap();
                cursor.write_u32::<LittleEndian>(buf_len).unwrap();
                cursor.write_u32::<LittleEndian>(tmp.len() as u32).unwrap();
            }

            let hash = cityhash128(&buf);
            encoder.write(hash.lo);
            encoder.write(hash.hi);
            encoder.write_bytes(buf.as_ref());
        } else {
            self.info.write(encoder);
            encoder.uvarint(self.column_count() as u64);
            encoder.uvarint(self.row_count() as u64);

            for column in &self.columns {
                column.write(encoder);
            }
        }
    }

    pub(crate) fn send_client_data(&self, encoder: &mut Encoder, compress: bool) {
        encoder.uvarint(protocols::CLIENT_DATA);
        encoder.string(""); // temporary table
        for chunk in self.chunks(INSERT_BLOCK_SIZE) {
            chunk.write(encoder, compress);
        }
    }

    pub(crate) fn send_server_data(&self, encoder: &mut Encoder, compress: bool) {
        encoder.uvarint(protocols::SERVER_DATA);
        encoder.string(""); // temporary table
        for chunk in self.chunks(INSERT_BLOCK_SIZE) {
            chunk.write(encoder, compress);
        }
    }

    pub fn chunks(&self, n: usize) -> ChunkIterator<K> {
        ChunkIterator::new(n, self)
    }
}

impl<K: ColumnType> fmt::Debug for Block<K> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let titles: Vec<&str> = self.columns.iter().map(|column| column.name()).collect();

        let cells: Vec<_> = self.columns.iter().map(|col| text_cells(col)).collect();

        let titles_len: Vec<_> = titles
            .iter()
            .map(|t| t.chars().count())
            .zip(cells.iter().map(|w| column_width(w)))
            .map(|(a, b)| cmp::max(a, b))
            .collect();

        print_line(f, &titles_len, "\n\u{250c}", '┬', "\u{2510}\n")?;

        for (i, title) in titles.iter().enumerate() {
            write!(f, "\u{2502}{:>width$} ", title, width = titles_len[i] + 1)?;
        }
        write!(f, "\u{2502}")?;

        if self.row_count() > 0 {
            print_line(f, &titles_len, "\n\u{251c}", '┼', "\u{2524}\n")?;
        }

        for j in 0..self.row_count() {
            for (i, col) in cells.iter().enumerate() {
                write!(f, "\u{2502}{:>width$} ", col[j], width = titles_len[i] + 1)?;
            }

            let new_line = (j + 1) != self.row_count();
            write!(f, "\u{2502}{}", if new_line { "\n" } else { "" })?;
        }

        print_line(f, &titles_len, "\n\u{2514}", '┴', "\u{2518}")
    }
}

fn column_width(column: &[String]) -> usize {
    column.iter().map(|cell| cell.len()).max().unwrap_or(0)
}

fn print_line(
    f: &mut fmt::Formatter,
    lens: &[usize],
    left: &str,
    center: char,
    right: &str,
) -> fmt::Result {
    write!(f, "{}", left)?;
    for (i, len) in lens.iter().enumerate() {
        if i != 0 {
            write!(f, "{}", center)?;
        }

        write!(f, "{:\u{2500}>width$}", "", width = len + 2)?;
    }
    write!(f, "{}", right)
}

fn text_cells<K: ColumnType>(data: &Column<K>) -> Vec<String> {
    (0..data.len()).map(|i| format!("{}", data.at(i))).collect()
}
