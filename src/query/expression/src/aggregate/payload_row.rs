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

use bumpalo::Bump;
use databend_common_arrow::arrow::bitmap::Bitmap;
use ethnum::i256;

use crate::load;
use crate::select_vector::SelectVector;
use crate::store;
use crate::types::decimal::DecimalColumn;
use crate::types::decimal::DecimalType;
use crate::types::string::StringColumn;
use crate::types::ArgType;
use crate::types::BooleanType;
use crate::types::DataType;
use crate::types::DateType;
use crate::types::NumberColumn;
use crate::types::NumberType;
use crate::types::StringType;
use crate::types::TimestampType;
use crate::types::ValueType;
use crate::with_decimal_mapped_type;
use crate::with_number_mapped_type;
use crate::Column;

pub fn rowformat_size(data_type: &DataType) -> usize {
    match data_type {
        DataType::Null | DataType::EmptyArray | DataType::EmptyMap => 0,
        DataType::Boolean => 1,
        // use address instead
        DataType::Binary | DataType::String | DataType::Bitmap | DataType::Variant => 4 + 8, /* u32 len + address */
        DataType::Number(n) => n.bit_width() as usize / 8,
        DataType::Decimal(n) => match n {
            crate::types::DecimalDataType::Decimal128(_) => 16,
            crate::types::DecimalDataType::Decimal256(_) => 32,
        },
        DataType::Timestamp => 8,
        DataType::Date => 4,
        DataType::Nullable(_) => 4,
        DataType::Array(_) => todo!(),
        DataType::Map(_) => todo!(),
        DataType::Tuple(_) => todo!(),
        DataType::Generic(_) => unreachable!(),
    }
}

/// This serialize column into row format by fixed size
pub unsafe fn serialize_column_to_rowformat(
    arena: &Bump,
    column: &Column,
    select_index: &SelectVector,
    rows: usize,
    address: &[*const u8],
    offset: usize,
    _scratch: &mut Vec<u8>,
) {
    match column {
        Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => {}
        Column::Number(v) => with_number_mapped_type!(|NUM_TYPE| match v {
            NumberColumn::NUM_TYPE(buffer) => {
                for i in 0..rows {
                    let index = select_index.get_index(i);
                    store(&buffer[index], address[index].add(offset) as *mut u8);
                }
            }
        }),
        Column::Decimal(v) => {
            with_decimal_mapped_type!(|DECIMAL_TYPE| match v {
                DecimalColumn::DECIMAL_TYPE(buffer, _) => {
                    for i in 0..rows {
                        let index = select_index.get_index(i);
                        store(&buffer[index], address[index].add(offset) as *mut u8);
                    }
                }
            })
        }
        Column::Boolean(v) => {
            for i in 0..rows {
                let index = select_index.get_index(i);
                store(&v.get_bit(index), address[index].add(offset) as *mut u8);
            }
        }
        Column::Binary(v) | Column::String(v) | Column::Bitmap(v) | Column::Variant(v) => {
            for i in 0..rows {
                let index = select_index.get_index(i);
                let data = arena.alloc_slice_copy(v.index_unchecked(index));

                store(&(data.len() as u32), address[index].add(offset) as *mut u8);

                store(
                    &(data.as_ptr() as u64),
                    address[index].add(offset + 4) as *mut u8,
                );
            }
        }
        Column::Timestamp(buffer) => {
            for i in 0..rows {
                let index = select_index.get_index(i);
                store(&buffer[index], address[index].add(offset) as *mut u8);
            }
        }
        Column::Date(buffer) => {
            for i in 0..rows {
                let index = select_index.get_index(i);
                store(&buffer[index], address[index].add(offset) as *mut u8);
            }
        }
        Column::Nullable(c) => serialize_column_to_rowformat(
            arena,
            &c.column,
            select_index,
            rows,
            address,
            offset,
            _scratch,
        ),

        Column::Array(_array) | Column::Map(_array) => {
            todo!("nested tuple/array/map not supported yet")
        }
        Column::Tuple(_fields) => {
            todo!("nested tuple/array/map not supported yet")
        }
    }
}

pub unsafe fn row_match_columns(
    cols: &[Column],
    address: &[*const u8],
    select_index: &mut SelectVector,
    count: usize,
    validity_offset: &[usize],
    col_offsets: &[usize],
    no_match: &mut SelectVector,
    no_match_count: &mut usize,
) {
    let mut count = count;
    for ((col, col_offset), validity_offset) in cols
        .iter()
        .zip(col_offsets.iter())
        .zip(validity_offset.iter())
    {
        row_match_column(
            col,
            address,
            select_index,
            &mut count,
            *validity_offset,
            *col_offset,
            no_match,
            no_match_count,
        )
    }
}

pub unsafe fn row_match_column(
    col: &Column,
    address: &[*const u8],
    select_index: &mut SelectVector,
    count: &mut usize,
    validity_offset: usize,
    col_offset: usize,
    no_match: &mut SelectVector,
    no_match_count: &mut usize,
) {
    let (validity, col) = if let Column::Nullable(c) = col {
        (Some(&c.validity), &c.column)
    } else {
        (None, col)
    };

    match col {
        Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => {
            *count = *no_match_count;
        }

        Column::Number(v) => with_number_mapped_type!(|NUM_TYPE| match v {
            NumberColumn::NUM_TYPE(_) => {
                row_match_column_type::<NumberType<NUM_TYPE>>(
                    col,
                    validity,
                    address,
                    select_index,
                    count,
                    validity_offset,
                    col_offset,
                    no_match,
                    no_match_count,
                )
            }
        }),
        Column::Decimal(v) => match v {
            DecimalColumn::Decimal128(_, _) => row_match_column_type::<DecimalType<i128>>(
                col,
                validity,
                address,
                select_index,
                count,
                validity_offset,
                col_offset,
                no_match,
                no_match_count,
            ),
            DecimalColumn::Decimal256(_, _) => row_match_column_type::<DecimalType<i256>>(
                col,
                validity,
                address,
                select_index,
                count,
                validity_offset,
                col_offset,
                no_match,
                no_match_count,
            ),
        },
        Column::Boolean(_) => row_match_column_type::<BooleanType>(
            col,
            validity,
            address,
            select_index,
            count,
            validity_offset,
            col_offset,
            no_match,
            no_match_count,
        ),
        Column::Timestamp(_) => row_match_column_type::<TimestampType>(
            col,
            validity,
            address,
            select_index,
            count,
            validity_offset,
            col_offset,
            no_match,
            no_match_count,
        ),
        Column::Date(_) => row_match_column_type::<DateType>(
            col,
            validity,
            address,
            select_index,
            count,
            validity_offset,
            col_offset,
            no_match,
            no_match_count,
        ),
        Column::Binary(v) | Column::String(v) | Column::Bitmap(v) | Column::Variant(v) => {
            row_match_string_column(
                v,
                validity,
                address,
                select_index,
                count,
                validity_offset,
                col_offset,
                no_match,
                no_match_count,
            )
        }
        Column::Nullable(_) => unreachable!(),
        Column::Array(_) => todo!(),
        Column::Map(_) => todo!(),
        Column::Tuple(_) => todo!(),
    }
}

unsafe fn row_match_string_column(
    col: &StringColumn,
    validity: Option<&Bitmap>,
    address: &[*const u8],
    select_index: &mut SelectVector,
    count: &mut usize,
    validity_offset: usize,
    col_offset: usize,
    no_match: &mut SelectVector,
    no_match_count: &mut usize,
) {
    let mut match_count = 0;
    let mut equal: bool;

    if let Some(validity) = validity {
        for i in 0..*count {
            let idx = select_index.get_index(i);
            let isnull = !validity.get_bit(idx);

            let validity_address = address[idx].add(validity_offset);
            let isnull2 = load::<u8>(validity_address) != 0;

            equal = isnull == isnull2;
            if !isnull && !isnull2 {
                let len_address = address[idx].add(col_offset);
                let address = address[idx].add(col_offset + 4);
                let len = load::<u32>(len_address) as usize;

                let value = StringType::index_column_unchecked(col, idx);
                if len != value.len() {
                    equal = false;
                } else {
                    let data_address = load::<u64>(address) as usize as *const u8;
                    let scalar = std::slice::from_raw_parts(data_address, len);
                    equal = scalar.eq(value);
                }
            }

            if equal {
                select_index.set_index(match_count, idx);
                match_count += 1;
            } else {
                no_match.set_index(*no_match_count, idx);
                *no_match_count += 1;
            }
        }
    } else {
        for i in 0..*count {
            let idx = select_index.get_index(i);

            let len_address = address[idx].add(col_offset);
            let address = address[idx].add(col_offset + 4);

            let len = load::<u32>(len_address) as usize;

            let value = StringType::index_column_unchecked(col, idx);
            if len != value.len() {
                equal = false;
            } else {
                let data_address = load::<u64>(address) as usize as *const u8;
                let scalar = std::slice::from_raw_parts(data_address, len);
                equal = scalar.eq(value);
            }

            if equal {
                select_index.set_index(match_count, idx);
                match_count += 1;
            } else {
                no_match.set_index(*no_match_count, idx);
                *no_match_count += 1;
            }
        }
    }

    *count = match_count;
}

unsafe fn row_match_column_type<T: ArgType>(
    col: &Column,
    validity: Option<&Bitmap>,
    address: &[*const u8],
    select_index: &mut SelectVector,
    count: &mut usize,
    validity_offset: usize,
    col_offset: usize,
    no_match: &mut SelectVector,
    no_match_count: &mut usize,
) {
    let col = T::try_downcast_column(col).unwrap();
    let mut match_count = 0;

    let mut equal: bool;

    if let Some(validity) = validity {
        for i in 0..*count {
            let idx = select_index.get_index(i);
            let isnull = !validity.get_bit(idx);

            let validity_address = address[idx].add(validity_offset);
            let isnull2 = load::<u8>(validity_address) != 0;

            equal = isnull == isnull2;
            if !isnull && !isnull2 {
                let address = address[idx].add(col_offset);
                let scalar = load::<<T as ValueType>::Scalar>(address);
                let value = T::index_column_unchecked(&col, idx);
                let value = T::to_owned_scalar(value);
                equal = scalar.eq(&value);
            }

            if equal {
                select_index.set_index(match_count, idx);
                match_count += 1;
            } else {
                no_match.set_index(*no_match_count, idx);
                *no_match_count += 1;
            }
        }
    } else {
        for i in 0..*count {
            let idx = select_index.get_index(i);
            let value = T::index_column_unchecked(&col, idx);
            let address = address[idx].add(col_offset);
            let scalar = load::<<T as ValueType>::Scalar>(address);
            let value = T::to_owned_scalar(value);

            if scalar.eq(&value) {
                select_index.set_index(match_count, idx);
                match_count += 1;
            } else {
                no_match.set_index(*no_match_count, idx);
                *no_match_count += 1;
            }
        }
    }

    *count = match_count;
}
