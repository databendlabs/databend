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

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::OwnedMemoryUsageSize;
use databend_common_base::runtime::ThreadTracker;
use databend_common_expression::types::array::ArrayColumnBuilder;
use databend_common_expression::types::binary::BinaryColumn;
use databend_common_expression::types::decimal::DecimalColumn;
use databend_common_expression::types::decimal::DecimalScalar;
use databend_common_expression::types::nullable::NullableColumnBuilder;
use databend_common_expression::types::string::StringColumn;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::DateType;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::NumberColumn;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::StringType;
use databend_common_expression::Column;
use databend_common_expression::Scalar;
use ethnum::I256;
use ordered_float::OrderedFloat;

#[test]
fn test_null_column_owned_memory_usage() {
    for len in 0..100 {
        let mem_stat = MemStat::create("TEST".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.mem_stat = Some(mem_stat.clone());

        let _guard = ThreadTracker::tracking(payload);
        let mut column = Column::Null { len };

        drop(_guard);
        assert_eq!(
            mem_stat.get_memory_usage(),
            column.owned_memory_usage() as i64
        );
    }
}

#[test]
fn test_empty_array_column_owned_memory_usage() {
    for len in 0..100 {
        let mem_stat = MemStat::create("TEST".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.mem_stat = Some(mem_stat.clone());

        let _guard = ThreadTracker::tracking(payload);
        let mut column = Column::EmptyArray { len };

        drop(_guard);
        assert_eq!(
            mem_stat.get_memory_usage(),
            column.owned_memory_usage() as i64
        );
    }
}

#[test]
fn test_empty_map_column_owned_memory_usage() {
    for len in 0..100 {
        let mem_stat = MemStat::create("TEST".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.mem_stat = Some(mem_stat.clone());

        let _guard = ThreadTracker::tracking(payload);
        let mut column = Column::EmptyMap { len };

        drop(_guard);
        assert_eq!(
            mem_stat.get_memory_usage(),
            column.owned_memory_usage() as i64
        );
    }
}

#[test]
fn test_numbers_column_owned_memory_usage() {
    for len in 0..100 {
        let mem_stat = MemStat::create("TEST".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.mem_stat = Some(mem_stat.clone());

        let _guard = ThreadTracker::tracking(payload);
        let mut column = Column::Number(NumberColumn::Int8(Buffer::from(vec![0; len])));

        drop(_guard);
        assert_ne!(mem_stat.get_memory_usage(), 0);
        assert_eq!(
            mem_stat.get_memory_usage(),
            column.owned_memory_usage() as i64
        );
    }
}

#[test]
fn test_decimal_column_owned_memory_usage() {
    for len in 0..100 {
        let mem_stat = MemStat::create("TEST".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.mem_stat = Some(mem_stat.clone());

        let _guard = ThreadTracker::tracking(payload);
        let mut column = Column::Decimal(DecimalColumn::Decimal128(
            Buffer::from(vec![0_i128; len]),
            DecimalSize {
                scale: 0,
                precision: 0,
            },
        ));

        drop(_guard);
        assert_ne!(mem_stat.get_memory_usage(), 0);
        assert_eq!(
            mem_stat.get_memory_usage(),
            column.owned_memory_usage() as i64
        );
    }
}

#[test]
fn test_boolean_column_owned_memory_usage() {
    for len in 0..100 {
        let mem_stat = MemStat::create("TEST".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.mem_stat = Some(mem_stat.clone());

        let _guard = ThreadTracker::tracking(payload);
        let mut column = Column::Boolean(Bitmap::from(vec![true; len]));

        drop(_guard);
        assert_ne!(mem_stat.get_memory_usage(), 0);
        assert_eq!(
            mem_stat.get_memory_usage(),
            column.owned_memory_usage() as i64
        );
    }
}

#[test]
fn test_binary_column_owned_memory_usage() {
    for len in 0..100 {
        let mem_stat = MemStat::create("TEST".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.mem_stat = Some(mem_stat.clone());

        let _guard = ThreadTracker::tracking(payload);
        let mut column = Column::Binary(BinaryColumn::new(
            Buffer::from(vec![0; len * 3]),
            Buffer::from(vec![0; len]),
        ));

        drop(_guard);
        assert_ne!(mem_stat.get_memory_usage(), 0);
        assert_eq!(
            mem_stat.get_memory_usage(),
            column.owned_memory_usage() as i64
        );
    }
}

#[test]
fn test_string_column_owned_memory_usage() {
    for len in 0..100 {
        let mem_stat = MemStat::create("TEST".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.mem_stat = Some(mem_stat.clone());

        let _guard = ThreadTracker::tracking(payload);
        let mut column = Column::String(StringColumn::new(
            Buffer::from(vec![0; len * 3]),
            Buffer::from(vec![0; len]),
        ));

        drop(_guard);
        assert_ne!(mem_stat.get_memory_usage(), 0);
        assert_eq!(
            mem_stat.get_memory_usage(),
            column.owned_memory_usage() as i64
        );
    }
}

#[test]
fn test_timestamp_column_owned_memory_usage() {
    for len in 0..100 {
        let mem_stat = MemStat::create("TEST".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.mem_stat = Some(mem_stat.clone());

        let _guard = ThreadTracker::tracking(payload);
        let mut column = Column::Timestamp(Buffer::from(vec![0; len]));

        drop(_guard);
        assert_ne!(mem_stat.get_memory_usage(), 0);
        assert_eq!(
            mem_stat.get_memory_usage(),
            column.owned_memory_usage() as i64
        );
    }
}

#[test]
fn test_date_column_owned_memory_usage() {
    for len in 0..100 {
        let mem_stat = MemStat::create("TEST".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.mem_stat = Some(mem_stat.clone());

        let _guard = ThreadTracker::tracking(payload);
        let mut column = Column::Date(Buffer::from(vec![0; len]));

        drop(_guard);
        assert_ne!(mem_stat.get_memory_usage(), 0);
        assert_eq!(
            mem_stat.get_memory_usage(),
            column.owned_memory_usage() as i64
        );
    }
}

#[test]
fn test_date_array_column_owned_memory_usage() {
    for len in 0..100 {
        let mem_stat = MemStat::create("TEST".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.mem_stat = Some(mem_stat.clone());

        let _guard = ThreadTracker::tracking(payload);
        let mut column = {
            let column = Buffer::from(vec![0; len]);
            Column::Array(Box::new(
                ArrayColumnBuilder::<DateType>::repeat(&column, 10)
                    .build()
                    .upcast(),
            ))
        };

        drop(_guard);
        assert_ne!(mem_stat.get_memory_usage(), 0);
        assert_eq!(
            mem_stat.get_memory_usage(),
            column.owned_memory_usage() as i64
        );
    }
}

#[test]
fn test_string_array_column_owned_memory_usage() {
    for len in 0..100 {
        let mem_stat = MemStat::create("TEST".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.mem_stat = Some(mem_stat.clone());

        let _guard = ThreadTracker::tracking(payload);
        let mut column = {
            let column = {
                let mut builder = StringColumnBuilder::with_capacity(len, len * 3);
                for _ in 0..len {
                    builder.put_str("abc");
                }

                builder.build()
            };
            Column::Array(Box::new(
                ArrayColumnBuilder::<StringType>::repeat(&column, 10)
                    .build()
                    .upcast(),
            ))
        };

        drop(_guard);
        assert_ne!(mem_stat.get_memory_usage(), 0);
        assert_eq!(
            mem_stat.get_memory_usage(),
            column.owned_memory_usage() as i64
        );
    }
}

#[test]
fn test_bitmap_column_owned_memory_usage() {
    for len in 0..100 {
        let mem_stat = MemStat::create("TEST".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.mem_stat = Some(mem_stat.clone());

        let _guard = ThreadTracker::tracking(payload);
        let mut column = Column::Bitmap(BinaryColumn::new(
            Buffer::from(vec![0; len * 3]),
            Buffer::from(vec![0; len]),
        ));

        drop(_guard);
        assert_ne!(mem_stat.get_memory_usage(), 0);
        assert_eq!(
            mem_stat.get_memory_usage(),
            column.owned_memory_usage() as i64
        );
    }
}

#[test]
fn test_variant_column_owned_memory_usage() {
    for len in 0..100 {
        let mem_stat = MemStat::create("TEST".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.mem_stat = Some(mem_stat.clone());

        let _guard = ThreadTracker::tracking(payload);
        let mut column = Column::Variant(BinaryColumn::new(
            Buffer::from(vec![0; len * 3]),
            Buffer::from(vec![0; len]),
        ));

        drop(_guard);
        assert_ne!(mem_stat.get_memory_usage(), 0);
        assert_eq!(
            mem_stat.get_memory_usage(),
            column.owned_memory_usage() as i64
        );
    }
}

#[test]
fn test_geometry_column_owned_memory_usage() {
    for len in 0..100 {
        let mem_stat = MemStat::create("TEST".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.mem_stat = Some(mem_stat.clone());

        let _guard = ThreadTracker::tracking(payload);
        let mut column = Column::Geometry(BinaryColumn::new(
            Buffer::from(vec![0; len * 3]),
            Buffer::from(vec![0; len]),
        ));

        drop(_guard);
        assert_ne!(mem_stat.get_memory_usage(), 0);
        assert_eq!(
            mem_stat.get_memory_usage(),
            column.owned_memory_usage() as i64
        );
    }
}

#[test]
fn test_nullable_column_owned_memory_usage() {
    for len in 0..100 {
        let mem_stat = MemStat::create("TEST".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.mem_stat = Some(mem_stat.clone());

        let _guard = ThreadTracker::tracking(payload);

        let mut column = Column::Nullable(Box::new({
            let mut builder: NullableColumnBuilder<StringType> =
                NullableColumnBuilder::with_capacity(len, &[]);

            for index in 0..len {
                match index % 2 == 0 {
                    true => builder.push_null(),
                    false => builder.push("abc"),
                }
            }
            builder.build().upcast()
        }));

        drop(_guard);
        assert_ne!(mem_stat.get_memory_usage(), 0);
        assert_eq!(
            mem_stat.get_memory_usage(),
            column.owned_memory_usage() as i64
        );
    }
}

#[test]
fn test_tuple_column_owned_memory_usage() {
    for len in 0..100 {
        let mem_stat = MemStat::create("TEST".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.mem_stat = Some(mem_stat.clone());

        let _guard = ThreadTracker::tracking(payload);

        let mut column = Column::Tuple({
            let mut columns = Vec::with_capacity(100);

            columns.push(Column::Timestamp(Buffer::from(vec![0; len])));
            columns.push(Column::String(StringColumn::new(
                Buffer::from(vec![0; len * 3]),
                Buffer::from(vec![0; len]),
            )));

            columns
        });

        drop(_guard);
        assert_ne!(mem_stat.get_memory_usage(), 0);
        assert_eq!(
            mem_stat.get_memory_usage(),
            column.owned_memory_usage() as i64
        );
    }
}

#[test]
fn test_scalar_owned_memory_usage() {
    fn test_scalar<F: Fn() -> Scalar>(f: F) {
        let mem_stat = MemStat::create("TEST".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();

        payload.mem_stat = Some(mem_stat.clone());

        let _guard = ThreadTracker::tracking(payload);

        let mut scalar = f();

        drop(_guard);
        assert_eq!(
            mem_stat.get_memory_usage(),
            scalar.owned_memory_usage() as i64
        );
    }

    test_scalar(|| Scalar::Null);
    test_scalar(|| Scalar::EmptyArray);
    test_scalar(|| Scalar::EmptyMap);
    test_scalar(|| Scalar::Number(NumberScalar::Int8(0)));
    test_scalar(|| Scalar::Number(NumberScalar::Int16(0)));
    test_scalar(|| Scalar::Number(NumberScalar::Int32(0)));
    test_scalar(|| Scalar::Number(NumberScalar::Int64(0)));
    test_scalar(|| Scalar::Number(NumberScalar::UInt8(0)));
    test_scalar(|| Scalar::Number(NumberScalar::UInt16(0)));
    test_scalar(|| Scalar::Number(NumberScalar::UInt32(0)));
    test_scalar(|| Scalar::Number(NumberScalar::UInt64(0)));
    test_scalar(|| Scalar::Number(NumberScalar::Float32(OrderedFloat(0_f32))));
    test_scalar(|| Scalar::Number(NumberScalar::Float64(OrderedFloat(0_f64))));
    test_scalar(|| {
        Scalar::Decimal(DecimalScalar::Decimal128(0, DecimalSize {
            precision: 0,
            scale: 0,
        }))
    });
    test_scalar(|| {
        Scalar::Decimal(DecimalScalar::Decimal256(I256::new(0), DecimalSize {
            precision: 0,
            scale: 0,
        }))
    });
    test_scalar(|| Scalar::Timestamp(0));
    test_scalar(|| Scalar::Date(0));
    test_scalar(|| Scalar::Boolean(true));
    test_scalar(|| Scalar::Binary(Vec::with_capacity(1000)));
    test_scalar(|| Scalar::String(String::with_capacity(1000)));
    test_scalar(|| {
        Scalar::Array(Column::Number(NumberColumn::Int8(Buffer::from(vec![
            0_i8;
            1000
        ]))))
    });
    test_scalar(|| {
        Scalar::Map(Column::Number(NumberColumn::Int8(Buffer::from(vec![
            0_i8;
            1000
        ]))))
    });
    test_scalar(|| Scalar::Bitmap(Vec::with_capacity(1000)));
    test_scalar(|| {
        Scalar::Tuple({
            let mut scalars = Vec::with_capacity(1000);
            scalars.push(Scalar::Null);
            scalars.push(Scalar::Number(NumberScalar::Int8(0)));
            scalars.push(Scalar::String(String::with_capacity(1000)));
            scalars.push(Scalar::Array(Column::Number(NumberColumn::Int8(
                Buffer::from(vec![0_i8; 1000]),
            ))));
            scalars
        })
    });
    test_scalar(|| Scalar::Variant(Vec::with_capacity(1000)));
    test_scalar(|| Scalar::Geometry(Vec::with_capacity(1000)));
}
