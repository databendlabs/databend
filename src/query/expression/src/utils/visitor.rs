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

use databend_common_column::bitmap::Bitmap;
use databend_common_column::buffer::Buffer;
use databend_common_column::types::months_days_micros;
use databend_common_exception::Result;
use decimal::DecimalType;
use geometry::GeometryType;

use crate::types::*;
use crate::*;

pub trait ValueVisitor {
    fn visit_scalar(&mut self, _scalar: Scalar) -> Result<()>;

    fn visit_null(&mut self, len: usize) -> Result<()> {
        self.visit_typed_column::<NullType>(len)
    }

    fn visit_empty_array(&mut self, len: usize) -> Result<()> {
        self.visit_typed_column::<EmptyArrayType>(len)
    }

    fn visit_empty_map(&mut self, len: usize) -> Result<()> {
        self.visit_typed_column::<EmptyMapType>(len)
    }

    fn visit_any_number(&mut self, column: NumberColumn) -> Result<()> {
        with_number_type!(|NUM_TYPE| match column {
            NumberColumn::NUM_TYPE(b) => self.visit_number(b),
        })
    }

    fn visit_number<T: Number>(
        &mut self,
        column: <NumberType<T> as ValueType>::Column,
    ) -> Result<()> {
        self.visit_typed_column::<NumberType<T>>(column)
    }

    fn visit_any_decimal(&mut self, column: DecimalColumn) -> Result<()> {
        with_decimal_type!(|DECIMAL_TYPE| match column {
            DecimalColumn::DECIMAL_TYPE(b, size) => self.visit_decimal(b, size),
        })
    }

    fn visit_decimal<T: Decimal>(&mut self, column: Buffer<T>, _size: DecimalSize) -> Result<()> {
        self.visit_typed_column::<DecimalType<T>>(column)
    }

    fn visit_boolean(&mut self, bitmap: Bitmap) -> Result<()> {
        self.visit_typed_column::<BooleanType>(bitmap)
    }

    fn visit_binary(&mut self, column: BinaryColumn) -> Result<()> {
        self.visit_typed_column::<BinaryType>(column)
    }

    fn visit_string(&mut self, column: StringColumn) -> Result<()> {
        self.visit_typed_column::<StringType>(column)
    }

    fn visit_timestamp(&mut self, buffer: Buffer<i64>) -> Result<()> {
        self.visit_typed_column::<TimestampType>(buffer)
    }

    fn visit_date(&mut self, buffer: Buffer<i32>) -> Result<()> {
        self.visit_typed_column::<DateType>(buffer)
    }

    fn visit_interval(&mut self, buffer: Buffer<months_days_micros>) -> Result<()> {
        self.visit_typed_column::<IntervalType>(buffer)
    }

    fn visit_array(&mut self, column: Box<ArrayColumn<AnyType>>) -> Result<()> {
        self.visit_typed_column::<AnyType>(Column::Array(column))
    }

    fn visit_map(&mut self, column: Box<ArrayColumn<AnyType>>) -> Result<()> {
        self.visit_typed_column::<AnyType>(Column::Map(column))
    }

    fn visit_tuple(&mut self, columns: Vec<Column>) -> Result<()> {
        self.visit_typed_column::<AnyType>(Column::Tuple(columns))
    }

    fn visit_bitmap(&mut self, column: BinaryColumn) -> Result<()> {
        self.visit_typed_column::<BitmapType>(column)
    }

    fn visit_nullable(&mut self, column: Box<NullableColumn<AnyType>>) -> Result<()> {
        self.visit_typed_column::<AnyType>(Column::Nullable(column))
    }

    fn visit_variant(&mut self, column: BinaryColumn) -> Result<()> {
        self.visit_typed_column::<VariantType>(column)
    }

    fn visit_geometry(&mut self, column: BinaryColumn) -> Result<()> {
        self.visit_typed_column::<GeometryType>(column)
    }

    fn visit_geography(&mut self, column: GeographyColumn) -> Result<()> {
        self.visit_typed_column::<GeographyType>(column)
    }

    fn visit_typed_column<T: ValueType>(&mut self, column: <T as ValueType>::Column) -> Result<()>;

    fn visit_value(&mut self, value: Value<AnyType>) -> Result<()> {
        match value {
            Value::Scalar(c) => self.visit_scalar(c),
            Value::Column(c) => self.visit_column(c),
        }
    }

    fn visit_column(&mut self, column: Column) -> Result<()> {
        match column {
            Column::Null { len } => self.visit_null(len),
            Column::EmptyArray { len } => self.visit_empty_array(len),
            Column::EmptyMap { len } => self.visit_empty_map(len),
            Column::Number(column) => self.visit_any_number(column),
            Column::Decimal(column) => self.visit_any_decimal(column),
            Column::Boolean(bitmap) => self.visit_boolean(bitmap),
            Column::Binary(column) => self.visit_binary(column),
            Column::String(column) => self.visit_string(column),
            Column::Timestamp(buffer) => self.visit_timestamp(buffer),
            Column::Date(buffer) => self.visit_date(buffer),
            Column::Interval(buffer) => self.visit_interval(buffer),
            Column::Array(column) => self.visit_array(column),
            Column::Map(column) => self.visit_map(column),
            Column::Tuple(columns) => self.visit_tuple(columns),
            Column::Bitmap(column) => self.visit_bitmap(column),
            Column::Nullable(column) => self.visit_nullable(column),
            Column::Variant(column) => self.visit_variant(column),
            Column::Geometry(column) => self.visit_geometry(column),
            Column::Geography(column) => self.visit_geography(column),
        }
    }
}
