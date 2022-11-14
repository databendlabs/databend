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

use common_datavalues::serializations::ArraySerializer;
use common_datavalues::serializations::BooleanSerializer;
use common_datavalues::serializations::ConstSerializer;
use common_datavalues::serializations::DateSerializer;
use common_datavalues::serializations::NullableSerializer;
use common_datavalues::serializations::NumberSerializer;
use common_datavalues::serializations::StringSerializer;
use common_datavalues::serializations::StructSerializer;
use common_datavalues::serializations::TimestampSerializer;
use common_datavalues::serializations::VariantSerializer;
use common_datavalues::PrimitiveType;
use common_datavalues::TypeSerializerImpl;
use lexical_core::ToLexical;
use micromarshal::Marshal;
use micromarshal::Unmarshal;
use num::cast::AsPrimitive;

use crate::field_encoder::helpers::PrimitiveWithFormat;
use crate::CommonSettings;

pub trait FieldEncoderRowBased {
    fn common_settings(&self) -> &CommonSettings;

    fn write_field<'a>(
        &self,
        column: &TypeSerializerImpl<'a>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        raw: bool,
    ) {
        match &column {
            TypeSerializerImpl::Const(c) => self.write_const(c, out_buf, raw),
            TypeSerializerImpl::Null(_) => self.write_null(out_buf, raw),
            TypeSerializerImpl::Nullable(c) => self.write_nullable(c, row_index, out_buf, raw),
            TypeSerializerImpl::Boolean(c) => self.write_bool(c, row_index, out_buf, raw),
            TypeSerializerImpl::Int8(c) => self.write_int(c, row_index, out_buf, raw),
            TypeSerializerImpl::Int16(c) => self.write_int(c, row_index, out_buf, raw),
            TypeSerializerImpl::Int32(c) => self.write_int(c, row_index, out_buf, raw),
            TypeSerializerImpl::Int64(c) => self.write_int(c, row_index, out_buf, raw),
            TypeSerializerImpl::UInt8(c) => self.write_int(c, row_index, out_buf, raw),
            TypeSerializerImpl::UInt16(c) => self.write_int(c, row_index, out_buf, raw),
            TypeSerializerImpl::UInt32(c) => self.write_int(c, row_index, out_buf, raw),
            TypeSerializerImpl::UInt64(c) => self.write_int(c, row_index, out_buf, raw),
            TypeSerializerImpl::Float32(c) => self.write_float(c, row_index, out_buf, raw),
            TypeSerializerImpl::Float64(c) => self.write_float(c, row_index, out_buf, raw),
            TypeSerializerImpl::Date(c) => self.write_date(c, row_index, out_buf, raw),
            TypeSerializerImpl::Interval(c) => self.write_date(c, row_index, out_buf, raw),
            TypeSerializerImpl::Timestamp(c) => self.write_timestamp(c, row_index, out_buf, raw),
            TypeSerializerImpl::String(c) => self.write_string(c, row_index, out_buf, raw),
            TypeSerializerImpl::Array(c) => self.write_array(c, row_index, out_buf, raw),
            TypeSerializerImpl::Struct(c) => self.write_struct(c, row_index, out_buf, raw),
            TypeSerializerImpl::Variant(c) => self.write_variant(c, row_index, out_buf, raw),
        }
    }

    fn write_bool(
        &self,
        column: &BooleanSerializer,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        _raw: bool,
    ) {
        let v = if column.values.get_bit(row_index) {
            &self.common_settings().true_bytes
        } else {
            &self.common_settings().false_bytes
        };

        out_buf.extend_from_slice(v);
    }

    fn write_null(&self, out_buf: &mut Vec<u8>, _raw: bool) {
        out_buf.extend_from_slice(&self.common_settings().null_bytes);
    }

    fn write_nullable(
        &self,
        column: &NullableSerializer,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        raw: bool,
    ) {
        if !column.validity.get_bit(row_index) {
            self.write_null(out_buf, raw)
        } else {
            self.write_field(&column.inner, row_index, out_buf, raw)
        }
    }

    fn write_string_inner(&self, in_buf: &[u8], out_buf: &mut Vec<u8>, raw: bool);

    fn write_const(&self, column: &ConstSerializer, out_buf: &mut Vec<u8>, raw: bool) {
        self.write_field(&column.inner, 0, out_buf, raw)
    }

    fn write_int<'a, T>(
        &self,
        column: &NumberSerializer<'a, T>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        _raw: bool,
    ) where
        T: PrimitiveType + Marshal + Unmarshal<T> + ToLexical + PrimitiveWithFormat,
    {
        column.values[row_index].write_field(out_buf, self.common_settings())
    }

    fn write_float<'a, T>(
        &self,
        column: &NumberSerializer<'a, T>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        _raw: bool,
    ) where
        T: PrimitiveType + Marshal + Unmarshal<T> + ToLexical + PrimitiveWithFormat,
    {
        column.values[row_index].write_field(out_buf, self.common_settings())
    }

    fn write_string(
        &self,
        column: &StringSerializer,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        raw: bool,
    ) {
        self.write_string_inner(
            unsafe { column.column.value_unchecked(row_index) },
            out_buf,
            raw,
        );
    }

    fn write_date<'a, T: PrimitiveType + AsPrimitive<i64> + ToLexical>(
        &self,
        column: &DateSerializer<'a, T>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        raw: bool,
    ) {
        let s = column.fmt(row_index);
        self.write_string_inner(s.as_bytes(), out_buf, raw);
    }

    fn write_timestamp<'a>(
        &self,
        column: &TimestampSerializer<'a>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        raw: bool,
    ) {
        let s = column.to_string_micro(row_index, &self.common_settings().timezone);
        self.write_string_inner(s.as_bytes(), out_buf, raw);
    }

    fn write_variant<'a>(
        &self,
        column: &VariantSerializer<'a>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        raw: bool,
    ) {
        let s = column.values[row_index].to_string();
        self.write_string_inner(s.as_bytes(), out_buf, raw);
    }

    fn write_array<'a>(
        &self,
        column: &ArraySerializer<'a>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        raw: bool,
    );

    fn write_struct<'a>(
        &self,
        column: &StructSerializer<'a>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        raw: bool,
    );
}
