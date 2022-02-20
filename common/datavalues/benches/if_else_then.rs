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

#[macro_use]
extern crate criterion;
use std::sync::Arc;

use common_arrow::arrow::array::*;
use common_arrow::arrow::compute::if_then_else::if_then_else;
use common_arrow::arrow::types::NativeType;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use criterion::Criterion;

fn add_benchmark(c: &mut Criterion) {
    let size = 1048576;
    let lhs: ArrayRef = Arc::new(create_primitive_array::<i32>(size, 0.2));
    let rhs: ArrayRef = Arc::new(create_primitive_array::<i32>(size, 0.3));
    let ifs: ArrayRef = Arc::new(create_boolean_array(size, 0.0, 0.3));

    c.bench_function("arrow2_if_else_then", |b| {
        b.iter(|| criterion::black_box(arrow2_if_else_then(&lhs, &rhs, &ifs)))
    });

    let lhs: ColumnRef = lhs.into_nullable_column();
    let rhs: ColumnRef = rhs.into_nullable_column();
    let ifs: ColumnRef = ifs.into_nullable_column();

    c.bench_function("databend_if_else_then", |b| {
        b.iter(|| criterion::black_box(databend_if_else_then(&lhs, &rhs, &ifs)))
    });
}

fn arrow2_if_else_then(lhs: &ArrayRef, rhs: &ArrayRef, ifs: &ArrayRef) -> Result<Box<dyn Array>> {
    let predicate = ifs.as_any().downcast_ref::<BooleanArray>().unwrap();
    Ok(if_then_else(predicate, lhs.as_ref(), rhs.as_ref()).unwrap())
}

fn databend_if_else_then(
    lhs: &ColumnRef,
    rhs: &ColumnRef,
    ifs: &ColumnRef,
) -> Result<Arc<dyn Column>> {
    let predicate: &NullableColumn = Series::check_get(ifs)?;
    let bool_c: &BooleanColumn = Series::check_get(predicate.inner())?;
    let bools = bool_c.values();
    let validity_predict = predicate.ensure_validity();

    if lhs.data_type() != rhs.data_type() {
        return Err(ErrorCode::BadDataValueType(
            "lhs and rhs must have the same data type".to_string(),
        ));
    }

    let physical_id = remove_nullable(&lhs.data_type())
        .data_type_id()
        .to_physical_type();

    macro_rules! with_match_physical_primitive_type {(
        $key_type:expr, | $_:tt $T:ident | $($body:tt)*
    ) => ({
        macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
        match $key_type {
             PhysicalTypeID::Int8 => __with_ty__! { i8 },
             PhysicalTypeID::Int16 => __with_ty__! { i16 },
             PhysicalTypeID::Int32 => __with_ty__! { i32 },
             PhysicalTypeID::Int64 => __with_ty__! { i64 },
             PhysicalTypeID::UInt8 => __with_ty__! { u8 },
             PhysicalTypeID::UInt16 => __with_ty__! { u16 },
             PhysicalTypeID::UInt32 => __with_ty__! { u32 },
             PhysicalTypeID::UInt64 => __with_ty__! { u64 },
             PhysicalTypeID::Float32 => __with_ty__! { f32 },
             PhysicalTypeID::Float64 => __with_ty__! { f64 },
             _ => unreachable!()
        }
    })}

    with_match_physical_primitive_type!(physical_id, |$T| {
        let lhs_wrapper = ColumnViewer::<$T>::try_create(lhs)?;
        let rhs_wrapper = ColumnViewer::<$T>::try_create(rhs)?;
        let size = lhs_wrapper.len();

        let mut builder = NullableColumnBuilder::<$T>::with_capacity(size);

        for row in 0..size {
            let valid = validity_predict.get_bit(row);
            if bools.get_bit(row) {
                builder.append(lhs_wrapper.value(row), valid & lhs_wrapper.valid_at(row));
            } else {
                builder.append(rhs_wrapper.value(row), valid & rhs_wrapper.valid_at(row));
            };
        }

        Ok(builder.build(size))
    })
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);

use rand::distributions::Distribution;
use rand::distributions::Standard;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
/// Returns fixed seedable RNG
pub fn seedable_rng() -> StdRng {
    StdRng::seed_from_u64(42)
}
/// Creates an random (but fixed-seeded) array of a given size and null density
pub fn create_primitive_array<T>(size: usize, null_density: f32) -> PrimitiveArray<T>
where
    T: NativeType,
    Standard: Distribution<T>,
{
    let mut rng = seedable_rng();

    (0..size)
        .map(|_| {
            if rng.gen::<f32>() < null_density {
                None
            } else {
                Some(rng.gen())
            }
        })
        .collect::<PrimitiveArray<T>>()
}

/// Creates a new [`PrimitiveArray`] from random values with a pre-set seed.
pub fn create_primitive_array_with_seed<T>(
    size: usize,
    null_density: f32,
    seed: u64,
) -> PrimitiveArray<T>
where
    T: NativeType,
    Standard: Distribution<T>,
{
    let mut rng = StdRng::seed_from_u64(seed);

    (0..size)
        .map(|_| {
            if rng.gen::<f32>() < null_density {
                None
            } else {
                Some(rng.gen())
            }
        })
        .collect::<PrimitiveArray<T>>()
}

/// Creates an random (but fixed-seeded) array of a given size and null density
pub fn create_boolean_array(size: usize, null_density: f32, true_density: f32) -> BooleanArray
where Standard: Distribution<bool> {
    let mut rng = seedable_rng();
    (0..size)
        .map(|_| {
            if rng.gen::<f32>() < null_density {
                None
            } else {
                let value = rng.gen::<f32>() < true_density;
                Some(value)
            }
        })
        .collect()
}
