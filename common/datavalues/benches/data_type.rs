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

use common_datavalues::prelude::*;
use common_exception::Result;
use criterion::Criterion;

fn add_benchmark(c: &mut Criterion) {
    let values = vec![
        DataValue::UInt64(3),
        DataValue::UInt64(4),
        DataValue::UInt64(5),
    ];

    let data_type = UInt64Type::arc();
    let data_type_enum = DataTypeImpl::UInt64(UInt64Type::new());

    c.bench_function("data_type_ptr_create", |b| {
        b.iter(|| criterion::black_box(data_type_ptr_create(&data_type, &values)))
    });

    c.bench_function("data_type_enum_create", |b| {
        b.iter(|| criterion::black_box(data_type_enum_create(&data_type_enum, &values)))
    });

    c.bench_function("data_type_ptr_dummy", |b| {
        b.iter(|| criterion::black_box(data_type.can_inside_nullable()))
    });

    c.bench_function("data_type_enum_dummy", |b| {
        b.iter(|| criterion::black_box(data_type_enum.can_inside_nullable()))
    });

    c.bench_function("data_type_ptr_dummy2", |b| {
        b.iter(|| criterion::black_box(data_type.is_null()))
    });

    c.bench_function("data_type_enum_dummy2", |b| {
        b.iter(|| criterion::black_box(data_type_enum.is_null()))
    });

    c.bench_function("data_type_ptr_dummy2_compiled", |b| {
        b.iter(|| data_type.is_null())
    });

    c.bench_function("data_type_enum_dummy2_compiled", |b| {
        b.iter(|| data_type_enum.is_null())
    });
}

fn data_type_ptr_create(ty: &DataTypePtr, values: &[DataValue]) -> Result<ColumnRef> {
    ty.create_column(values)
}

fn data_type_enum_create(ty: &DataTypeImpl, values: &[DataValue]) -> Result<ColumnRef> {
    ty.create_column(values)
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
