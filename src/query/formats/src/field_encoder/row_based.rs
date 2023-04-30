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

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::buffer::Buffer;
use common_expression::types::array::ArrayColumn;
use common_expression::types::date::date_to_string;
use common_expression::types::decimal::DecimalColumn;
use common_expression::types::nullable::NullableColumn;
use common_expression::types::number::NumberColumn;
use common_expression::types::string::StringColumn;
use common_expression::types::timestamp::timestamp_to_string;
use common_expression::types::ValueType;
use common_expression::Column;
use lexical_core::ToLexical;
use micromarshal::Marshal;
use micromarshal::Unmarshal;
use ordered_float::OrderedFloat;

use crate::field_encoder::helpers::PrimitiveWithFormat;
use crate::CommonSettings;

pub trait FieldEncoderRowBased {
    fn common_settings(&self) -> &CommonSettings;

    /// 'raw' is mainly for string now and types need to be encoded as string in a format,
    /// to determine the quote/escape behavior.
    /// 'raw=true' can be roughly understood as 'no quote/escape'.
    /// for example, string inside tuple/struct are always quoted/escaped now.
    fn write_field(&self, column: &Column, row_index: usize, out_buf: &mut Vec<u8>, raw: bool) {
        match &column {
            Column::Null { .. } => self.write_null(out_buf),
            Column::EmptyArray { .. } => self.write_empty_array(out_buf),
            Column::EmptyMap { .. } => self.write_empty_map(out_buf),
            Column::Boolean(c) => self.write_bool(c, row_index, out_buf),
            Column::Number(col) => match col {
                NumberColumn::UInt8(c) => self.write_int(c, row_index, out_buf),
                NumberColumn::UInt16(c) => self.write_int(c, row_index, out_buf),
                NumberColumn::UInt32(c) => self.write_int(c, row_index, out_buf),
                NumberColumn::UInt64(c) => self.write_int(c, row_index, out_buf),
                NumberColumn::Int8(c) => self.write_int(c, row_index, out_buf),
                NumberColumn::Int16(c) => self.write_int(c, row_index, out_buf),
                NumberColumn::Int32(c) => self.write_int(c, row_index, out_buf),
                NumberColumn::Int64(c) => self.write_int(c, row_index, out_buf),
                NumberColumn::Float32(c) => self.write_float(c, row_index, out_buf),
                NumberColumn::Float64(c) => self.write_float(c, row_index, out_buf),
            },
            Column::Decimal(c) => self.write_decimal(c, row_index, out_buf),
            Column::Date(c) => self.write_date(c, row_index, out_buf, raw),
            Column::Timestamp(c) => self.write_timestamp(c, row_index, out_buf, raw),
            Column::String(c) => self.write_string(c, row_index, out_buf, raw),
            Column::Nullable(box c) => self.write_nullable(c, row_index, out_buf, raw),
            Column::Array(box c) => self.write_array(c, row_index, out_buf, raw),
            Column::Map(box c) => self.write_map(c, row_index, out_buf, raw),
            Column::Bitmap(b) => self.write_string(b, row_index, out_buf, raw),
            Column::Tuple(fields) => self.write_tuple(fields, row_index, out_buf, raw),
            Column::Variant(c) => self.write_variant(c, row_index, out_buf, raw),
        }
    }

    fn write_bool(&self, column: &Bitmap, row_index: usize, out_buf: &mut Vec<u8>) {
        let v = if column.get_bit(row_index) {
            &self.common_settings().true_bytes
        } else {
            &self.common_settings().false_bytes
        };

        out_buf.extend_from_slice(v);
    }

    fn write_null(&self, out_buf: &mut Vec<u8>) {
        out_buf.extend_from_slice(&self.common_settings().null_bytes);
    }

    fn write_empty_array(&self, out_buf: &mut Vec<u8>) {
        out_buf.extend_from_slice(b"[");
        out_buf.extend_from_slice(b"]");
    }

    fn write_empty_map(&self, out_buf: &mut Vec<u8>) {
        out_buf.extend_from_slice(b"{");
        out_buf.extend_from_slice(b"}");
    }

    fn write_nullable<T: ValueType>(
        &self,
        column: &NullableColumn<T>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        raw: bool,
    ) {
        if !column.validity.get_bit(row_index) {
            self.write_null(out_buf)
        } else {
            self.write_field(
                &T::upcast_column(column.column.clone()),
                row_index,
                out_buf,
                raw,
            )
        }
    }

    fn write_string_inner(&self, in_buf: &[u8], out_buf: &mut Vec<u8>, raw: bool);

    fn write_int<T>(&self, column: &Buffer<T>, row_index: usize, out_buf: &mut Vec<u8>)
    where T: Marshal + Unmarshal<T> + ToLexical + PrimitiveWithFormat {
        let v = unsafe { column.get_unchecked(row_index) };
        v.write_field(out_buf, self.common_settings())
    }

    fn write_float<T>(
        &self,
        column: &Buffer<OrderedFloat<T>>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
    ) where
        T: Marshal + Unmarshal<T> + ToLexical + PrimitiveWithFormat,
    {
        let v = unsafe { column.get_unchecked(row_index) };
        v.0.write_field(out_buf, self.common_settings())
    }

    fn write_decimal(&self, column: &DecimalColumn, row_index: usize, out_buf: &mut Vec<u8>) {
        let data = column.index(row_index).unwrap().to_string();
        out_buf.extend_from_slice(data.as_bytes());
    }

    fn write_string(
        &self,
        column: &StringColumn,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        raw: bool,
    ) {
        self.write_string_inner(unsafe { column.index_unchecked(row_index) }, out_buf, raw);
    }

    fn write_date(&self, column: &Buffer<i32>, row_index: usize, out_buf: &mut Vec<u8>, raw: bool) {
        let v = unsafe { column.get_unchecked(row_index) };
        let s = date_to_string(*v as i64, self.common_settings().timezone).to_string();
        self.write_string_inner(s.as_bytes(), out_buf, raw);
    }

    fn write_timestamp(
        &self,
        column: &Buffer<i64>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        raw: bool,
    ) {
        let v = unsafe { column.get_unchecked(row_index) };
        let s = timestamp_to_string(*v, self.common_settings().timezone).to_string();
        self.write_string_inner(s.as_bytes(), out_buf, raw);
    }

    fn write_variant(
        &self,
        column: &StringColumn,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        raw: bool,
    ) {
        let v = unsafe { column.index_unchecked(row_index) };
        let s = jsonb::to_string(v);
        self.write_string_inner(s.as_bytes(), out_buf, raw);
    }

    fn write_array<T: ValueType>(
        &self,
        column: &ArrayColumn<T>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        raw: bool,
    );

    fn write_map<T: ValueType>(
        &self,
        column: &ArrayColumn<T>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        raw: bool,
    );

    fn write_tuple(&self, columns: &[Column], row_index: usize, out_buf: &mut Vec<u8>, raw: bool);
}
