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
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::arrow::compute::cast;
use common_arrow::arrow::compute::cast::CastOptions as ArrowOption;
use common_arrow::arrow::compute::comparison;
use common_arrow::arrow::compute::comparison::Simd8Lanes;
use common_arrow::arrow::compute::comparison::Simd8PartialEq;
use common_arrow::arrow::types::NativeType;
use common_datavalues::prelude::*;
use common_datavalues::type_coercion::compare_coercion;
use common_datavalues::with_match_physical_primitive_type_error;
use common_exception::Result;
use criterion::Criterion;

fn add_benchmark(c: &mut Criterion) {
    let size = 1048576;
    let lhs: ArrayRef = Arc::new(create_primitive_array::<i32>(size, 0.2));
    let rhs: ArrayRef = Arc::new(create_primitive_array::<i32>(size, 0.3));

    c.bench_function("arrow2_eq", |b| {
        b.iter(|| criterion::black_box(arrow2_eq(&lhs, &rhs)))
    });

    let lhs: ColumnRef = lhs.into_nullable_column();
    let rhs: ColumnRef = rhs.into_nullable_column();

    c.bench_function("databend_same_type_eq", |b| {
        b.iter(|| criterion::black_box(databend_eq(&lhs, &rhs)))
    });

    c.bench_function("databend_same_type_eq_simd", |b| {
        b.iter(|| criterion::black_box(databend_eq_simd(&lhs, &rhs)))
    });

    let rhs: ArrayRef = Arc::new(create_primitive_array::<u32>(size, 0.3));
    let rhs: ColumnRef = rhs.into_nullable_column();

    c.bench_function("databend_diff_type_eq", |b| {
        b.iter(|| criterion::black_box(databend_eq(&lhs, &rhs)))
    });

    c.bench_function("databend_diff_type_eq_simd", |b| {
        b.iter(|| criterion::black_box(databend_eq_simd(&lhs, &rhs)))
    });
}

fn arrow2_eq(lhs: &ArrayRef, rhs: &ArrayRef) -> BooleanArray {
    comparison::eq(lhs.as_ref(), rhs.as_ref())
}

fn databend_eq(lhs: &ColumnRef, rhs: &ColumnRef) -> Result<ColumnRef> {
    let mut validity: Option<Bitmap> = None;
    let (_, valid) = lhs.validity();
    validity = combine_validities_2(validity.clone(), valid.cloned());
    let lhs = Series::remove_nullable(lhs);

    let (_, valid) = rhs.validity();
    validity = combine_validities_2(validity.clone(), valid.cloned());
    let rhs = Series::remove_nullable(rhs);

    let lhs_type = remove_nullable(&lhs.data_type());
    let rhs_type = remove_nullable(&rhs.data_type());
    let lhs_id = lhs_type.data_type_id().to_physical_type();
    let rhs_id = rhs_type.data_type_id().to_physical_type();

    let col = with_match_physical_primitive_type_error!(lhs_id, |$L| {
        with_match_physical_primitive_type_error!(rhs_id, |$R| {
            let left: &<$L as Scalar>::ColumnType = unsafe { Series::static_cast(&lhs) };
            let right: &<$R as Scalar>::ColumnType = unsafe { Series::static_cast(&rhs) };

            let it = left.scalar_iter().zip(right.scalar_iter()).map(|(a, b)| {
                let a = a as <($L, $R) as ResultTypeOfBinary>::LeastSuper;
                let b = b as <($L, $R) as ResultTypeOfBinary>::LeastSuper;
                a.eq(&b)
            });
            Arc::new(BooleanColumn::from_owned_iterator(it))
        })
    });

    NullableColumn::wrap_inner(col, Some(validity))
}

fn databend_eq_simd(lhs: &ColumnRef, rhs: &ColumnRef) -> Result<ColumnRef> {
    let mut validity: Option<Bitmap> = None;
    let (_, valid) = lhs.validity();
    validity = combine_validities_2(validity.clone(), valid.cloned());
    let lhs = Series::remove_nullable(lhs);

    let (_, valid) = rhs.validity();
    validity = combine_validities_2(validity.clone(), valid.cloned());
    let rhs = Series::remove_nullable(rhs);

    let lhs_type = remove_nullable(&lhs.data_type());
    let rhs_type = remove_nullable(&rhs.data_type());
    let least_supertype = compare_coercion(&lhs_type, &rhs_type)?;

    let col0 = if lhs_type != least_supertype {
        cast(&lhs, &least_supertype)?
    } else {
        lhs
    };

    let col1 = if rhs_type != least_supertype {
        cast(&rhs, &least_supertype)?
    } else {
        rhs
    };

    let physical_id = least_supertype.data_type_id().to_physical_type();

    let col = with_match_physical_primitive_type_error!(physical_id, |$T| {
        let left: &<$T as Scalar>::ColumnType = unsafe { Series::static_cast(&col0) };
        let right: &<$T as Scalar>::ColumnType = unsafe { Series::static_cast(&col1) };
        Arc::new(eq::<$T>(&left, &right))
    });

    NullableColumn::wrap_inner(col, Some(validity))
}

fn cast(column: &ColumnRef, data_type: &DataTypePtr) -> Result<ColumnRef> {
    let arrow_array = column.as_arrow_array();
    let arrow_options = ArrowOption {
        wrapped: true,
        partial: false,
    };
    let result = cast::cast(arrow_array.as_ref(), &data_type.arrow_type(), arrow_options)?;
    let result: ArrayRef = Arc::from(result);
    Ok(result.into_column())
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

/// Perform `lhs == rhs` operation on two arrays.
pub fn eq<T>(lhs: &PrimitiveColumn<T>, rhs: &PrimitiveColumn<T>) -> BooleanColumn
where
    T: PrimitiveType + comparison::Simd8,
    T::Simd: comparison::Simd8PartialEq,
{
    compare_op(lhs, rhs, |a, b| a.eq(b))
}

/// Evaluate `op(lhs, rhs)` for [`PrimitiveArray`]s using a specified
/// comparison function.
fn compare_op<T, F>(lhs: &PrimitiveColumn<T>, rhs: &PrimitiveColumn<T>, op: F) -> BooleanColumn
where
    T: PrimitiveType + comparison::Simd8,
    F: Fn(T::Simd, T::Simd) -> u8,
{
    let values = compare_values_op(lhs.values(), rhs.values(), op);

    BooleanColumn::from_arrow_data(values.into())
}

pub(crate) fn compare_values_op<T, F>(lhs: &[T], rhs: &[T], op: F) -> MutableBitmap
where
    T: PrimitiveType + comparison::Simd8,
    F: Fn(T::Simd, T::Simd) -> u8,
{
    assert_eq!(lhs.len(), rhs.len());

    let lhs_chunks_iter = lhs.chunks_exact(8);
    let lhs_remainder = lhs_chunks_iter.remainder();
    let rhs_chunks_iter = rhs.chunks_exact(8);
    let rhs_remainder = rhs_chunks_iter.remainder();

    let mut values = Vec::with_capacity((lhs.len() + 7) / 8);
    let iterator = lhs_chunks_iter.zip(rhs_chunks_iter).map(|(lhs, rhs)| {
        let lhs = T::Simd::from_chunk(lhs);
        let rhs = T::Simd::from_chunk(rhs);
        op(lhs, rhs)
    });
    values.extend(iterator);

    if !lhs_remainder.is_empty() {
        let lhs = T::Simd::from_incomplete_chunk(lhs_remainder, T::default());
        let rhs = T::Simd::from_incomplete_chunk(rhs_remainder, T::default());
        values.push(op(lhs, rhs))
    };
    MutableBitmap::from_vec(values, lhs.len())
}
