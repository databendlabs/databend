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
use databend_common_column::types::timestamp_tz;
use databend_common_exception::ErrorCode;
use decimal::DecimalType;
use geometry::GeometryType;

use crate::types::opaque::OpaqueColumn;
use crate::types::simple_type::SimpleType;
use crate::types::timestamp_tz::TimestampTzType;
use crate::types::*;
use crate::*;

pub trait ValueVisitor: Sized {
    type U = ();
    type Error = ErrorCode;

    fn visit_scalar(&mut self, _scalar: Scalar) -> Result<Self::U, Self::Error>;

    fn visit_null(&mut self, len: usize) -> Result<Self::U, Self::Error> {
        self.visit_typed_column::<NullType>(len, &DataType::Null)
    }

    fn visit_empty_array(&mut self, len: usize) -> Result<Self::U, Self::Error> {
        self.visit_typed_column::<EmptyArrayType>(len, &DataType::EmptyArray)
    }

    fn visit_empty_map(&mut self, len: usize) -> Result<Self::U, Self::Error> {
        self.visit_typed_column::<EmptyMapType>(len, &DataType::EmptyMap)
    }

    fn visit_any_number(&mut self, column: NumberColumn) -> Result<Self::U, Self::Error> {
        with_number_type!(|NUM_TYPE| match column {
            NumberColumn::NUM_TYPE(b) => self.visit_number(b),
        })
    }

    fn visit_number<T: Number>(
        &mut self,
        column: <NumberType<T> as AccessType>::Column,
    ) -> Result<Self::U, Self::Error> {
        self.visit_typed_column::<NumberType<T>>(column, &DataType::Number(T::data_type()))
    }

    fn visit_any_decimal(&mut self, column: DecimalColumn) -> Result<Self::U, Self::Error> {
        with_decimal_type!(|DECIMAL_TYPE| match column {
            DecimalColumn::DECIMAL_TYPE(b, size) => self.visit_decimal(b, size),
        })
    }

    fn visit_decimal<T: Decimal>(
        &mut self,
        column: Buffer<T>,
        size: DecimalSize,
    ) -> Result<Self::U, Self::Error> {
        self.visit_typed_column::<DecimalType<T>>(column, &DataType::Decimal(size))
    }

    fn visit_boolean(&mut self, bitmap: Bitmap) -> Result<Self::U, Self::Error> {
        self.visit_typed_column::<BooleanType>(bitmap, &DataType::Boolean)
    }

    fn visit_binary(&mut self, column: BinaryColumn) -> Result<Self::U, Self::Error> {
        self.visit_typed_column::<BinaryType>(column, &DataType::Binary)
    }

    fn visit_string(&mut self, column: StringColumn) -> Result<Self::U, Self::Error> {
        self.visit_typed_column::<StringType>(column, &DataType::String)
    }

    fn visit_timestamp(&mut self, buffer: Buffer<i64>) -> Result<Self::U, Self::Error> {
        self.visit_typed_column::<TimestampType>(buffer, &DataType::Timestamp)
    }

    fn visit_timestamp_tz(&mut self, buffer: Buffer<timestamp_tz>) -> Result<Self::U, Self::Error> {
        self.visit_typed_column::<TimestampTzType>(buffer, &DataType::TimestampTz)
    }

    fn visit_date(&mut self, buffer: Buffer<i32>) -> Result<Self::U, Self::Error> {
        self.visit_typed_column::<DateType>(buffer, &DataType::Date)
    }

    fn visit_interval(
        &mut self,
        buffer: Buffer<months_days_micros>,
    ) -> Result<Self::U, Self::Error> {
        self.visit_typed_column::<IntervalType>(buffer, &DataType::Interval)
    }

    fn visit_array(&mut self, column: Box<ArrayColumn<AnyType>>) -> Result<Self::U, Self::Error> {
        let data_type = Box::new(column.values().data_type());
        self.visit_typed_column::<AnyType>(Column::Array(column), &DataType::Array(data_type))
    }

    fn visit_map(&mut self, column: Box<ArrayColumn<AnyType>>) -> Result<Self::U, Self::Error> {
        let data_type = Box::new(column.values().data_type());
        self.visit_typed_column::<AnyType>(Column::Map(column), &DataType::Map(data_type))
    }

    fn visit_tuple(&mut self, columns: Vec<Column>) -> Result<Self::U, Self::Error> {
        let data_types = columns.iter().map(|c| c.data_type()).collect();
        self.visit_typed_column::<AnyType>(Column::Tuple(columns), &DataType::Tuple(data_types))
    }

    fn visit_bitmap(&mut self, column: BinaryColumn) -> Result<Self::U, Self::Error> {
        self.visit_typed_column::<BitmapType>(column, &DataType::Bitmap)
    }

    fn visit_nullable(
        &mut self,
        column: Box<NullableColumn<AnyType>>,
    ) -> Result<Self::U, Self::Error> {
        let data_type = column.column.data_type().wrap_nullable();
        self.visit_typed_column::<AnyType>(Column::Nullable(column), &data_type)
    }

    fn visit_variant(&mut self, column: BinaryColumn) -> Result<Self::U, Self::Error> {
        self.visit_typed_column::<VariantType>(column, &DataType::Variant)
    }

    fn visit_geometry(&mut self, column: BinaryColumn) -> Result<Self::U, Self::Error> {
        self.visit_typed_column::<GeometryType>(column, &DataType::Geometry)
    }

    fn visit_geography(&mut self, column: GeographyColumn) -> Result<Self::U, Self::Error> {
        self.visit_typed_column::<GeographyType>(column, &DataType::Geography)
    }

    fn visit_vector(&mut self, column: VectorColumn) -> Result<Self::U, Self::Error> {
        let data_type = DataType::Vector(column.data_type());
        self.visit_typed_column::<VectorType>(column, &data_type)
    }

    fn visit_opaque(&mut self, column: OpaqueColumn) -> Result<Self::U, Self::Error> {
        let data_type = DataType::Opaque(column.size() as _);
        self.visit_typed_column::<AnyType>(Column::Opaque(column), &data_type)
    }

    fn visit_typed_column<T: ValueType>(
        &mut self,
        column: T::Column,
        data_type: &DataType,
    ) -> Result<Self::U, Self::Error>;

    fn visit_value(&mut self, value: Value<AnyType>) -> Result<Self::U, Self::Error> {
        match value {
            Value::Scalar(c) => self.visit_scalar(c),
            Value::Column(c) => self.visit_column(c),
        }
    }

    fn visit_column(&mut self, column: Column) -> Result<Self::U, Self::Error> {
        Self::default_visit_column(column, self)
    }

    fn default_visit_column<V: ValueVisitor>(
        column: Column,
        visitor: &mut V,
    ) -> Result<V::U, V::Error> {
        match column {
            Column::Null { len } => visitor.visit_null(len),
            Column::EmptyArray { len } => visitor.visit_empty_array(len),
            Column::EmptyMap { len } => visitor.visit_empty_map(len),
            Column::Number(column) => visitor.visit_any_number(column),
            Column::Decimal(column) => visitor.visit_any_decimal(column),
            Column::Boolean(bitmap) => visitor.visit_boolean(bitmap),
            Column::Binary(column) => visitor.visit_binary(column),
            Column::String(column) => visitor.visit_string(column),
            Column::Timestamp(buffer) => visitor.visit_timestamp(buffer),
            Column::TimestampTz(buffer) => visitor.visit_timestamp_tz(buffer),
            Column::Date(buffer) => visitor.visit_date(buffer),
            Column::Interval(buffer) => visitor.visit_interval(buffer),
            Column::Array(column) => visitor.visit_array(column),
            Column::Map(column) => visitor.visit_map(column),
            Column::Tuple(columns) => visitor.visit_tuple(columns),
            Column::Bitmap(column) => visitor.visit_bitmap(column),
            Column::Nullable(column) => visitor.visit_nullable(column),
            Column::Variant(column) => visitor.visit_variant(column),
            Column::Geometry(column) => visitor.visit_geometry(column),
            Column::Geography(column) => visitor.visit_geography(column),
            Column::Vector(column) => visitor.visit_vector(column),
            Column::Opaque(column) => visitor.visit_opaque(column),
        }
    }

    fn visit_simple_type<T: SimpleType>(
        &mut self,
        _: Buffer<T::Scalar>,
        _: &DataType,
    ) -> Result<Self::U, Self::Error> {
        unimplemented!()
    }
}
