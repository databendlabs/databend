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

//! Decimal column deserialization for Parquet data

use databend_common_column::buffer::Buffer;
use databend_common_expression::types::i256;
use databend_common_expression::types::DecimalColumn;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::Column;
use parquet2::schema::types::PhysicalType;

use crate::column::common::ParquetColumnIterator;
use crate::column::common::ParquetColumnType;
use crate::reader::decompressor::Decompressor;

// =============================================================================
// Wrapper Types for Decimal Usage
// =============================================================================

/// Wrapper for i64 as Decimal64 - enables zero-cost transmute via #[repr(transparent)]
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Decimal64(pub i64);

/// Wrapper for i128 as Decimal128 - enables zero-cost transmute via #[repr(transparent)]
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Decimal128(pub i128);

/// Wrapper for i256 as Decimal256 - enables zero-cost transmute via #[repr(transparent)]
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Decimal256(pub i256);

#[derive(Clone)]
pub struct DecimalMetadata {
    pub precision: u8,
    pub scale: u8,
}

impl ParquetColumnType for Decimal64 {
    type Metadata = DecimalMetadata;
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Int64;

    fn create_column(data: Vec<Self>, metadata: &Self::Metadata) -> Column {
        let decimal_size = DecimalSize::new_unchecked(metadata.precision, metadata.scale);
        let raw_data: Vec<i64> = unsafe { std::mem::transmute(data) };
        Column::Decimal(DecimalColumn::Decimal64(
            Buffer::from(raw_data),
            decimal_size,
        ))
    }
}

impl ParquetColumnType for Decimal128 {
    type Metadata = DecimalMetadata;
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::FixedLenByteArray(16);

    fn create_column(data: Vec<Self>, metadata: &Self::Metadata) -> Column {
        let decimal_size = DecimalSize::new_unchecked(metadata.precision, metadata.scale);
        let raw_data: Vec<i128> = unsafe { std::mem::transmute(data) };
        Column::Decimal(DecimalColumn::Decimal128(
            Buffer::from(raw_data),
            decimal_size,
        ))
    }
}

impl ParquetColumnType for Decimal256 {
    type Metadata = DecimalMetadata;
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::FixedLenByteArray(32);

    fn create_column(data: Vec<Self>, metadata: &Self::Metadata) -> Column {
        let decimal_size = DecimalSize::new_unchecked(metadata.precision, metadata.scale);
        let raw_data: Vec<i256> = unsafe { std::mem::transmute(data) };
        Column::Decimal(DecimalColumn::Decimal256(
            Buffer::from(raw_data),
            decimal_size,
        ))
    }
}

// =============================================================================
// Iterator Type Aliases
// =============================================================================

pub type DecimalIter<'a, T> = ParquetColumnIterator<'a, T>;

// =============================================================================
// Constructor Functions
// =============================================================================

/// Generic decimal iterator constructor
pub fn new_decimal_iter<T>(
    pages: Decompressor,
    num_rows: usize,
    precision: u8,
    scale: u8,
    is_nullable: bool,
    chunk_size: Option<usize>,
) -> DecimalIter<T>
where
    T: ParquetColumnType<Metadata = DecimalMetadata>,
{
    let metadata = DecimalMetadata { precision, scale };
    ParquetColumnIterator::new(pages, num_rows, is_nullable, metadata, chunk_size)
}

pub fn new_decimal64_iter(
    pages: Decompressor,
    num_rows: usize,
    precision: u8,
    scale: u8,
    is_nullable: bool,
    chunk_size: Option<usize>,
) -> DecimalIter<Decimal64> {
    new_decimal_iter(pages, num_rows, precision, scale, is_nullable, chunk_size)
}

pub fn new_decimal128_iter(
    pages: Decompressor,
    num_rows: usize,
    precision: u8,
    scale: u8,
    is_nullable: bool,
    chunk_size: Option<usize>,
) -> DecimalIter<Decimal128> {
    new_decimal_iter(pages, num_rows, precision, scale, is_nullable, chunk_size)
}

pub fn new_decimal256_iter(
    pages: Decompressor,
    num_rows: usize,
    precision: u8,
    scale: u8,
    is_nullable: bool,
    chunk_size: Option<usize>,
) -> DecimalIter<Decimal256> {
    new_decimal_iter(pages, num_rows, precision, scale, is_nullable, chunk_size)
}
