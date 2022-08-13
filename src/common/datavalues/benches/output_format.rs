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

use common_datavalues::ColumnRef;
use common_datavalues::DataType;
use common_datavalues::Series;
use common_datavalues::SeriesFrom;
use common_datavalues::TypeSerializer;
use common_io::prelude::FormatSettings;
use criterion::black_box;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use rand::distributions::Alphanumeric;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;

pub fn write_csv(cols: &[ColumnRef]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1000 * 1000);
    let mut ss = vec![];
    let rows = cols[0].len();

    for c in cols {
        let t = c.data_type();
        ss.push(t.create_serializer(c).unwrap())
    }
    let f = &FormatSettings::default();
    for row in 0..rows {
        for s in &ss {
            s.write_field(row, &mut buf, f);
        }
    }
    buf
}

fn add_benchmark(c: &mut Criterion) {
    let mut cols = vec![];
    let size = 4096;
    cols.push(create_primitive_array(size, None, 0));
    cols.push(create_string_array(size, None, 100));
    c.bench_function("not_nullable", |b| {
        b.iter(|| write_csv(black_box(&cols)));
    });
}

pub fn create_primitive_array(
    size: usize,
    null_density: Option<f32>,
    _item_size: usize,
) -> ColumnRef {
    let mut rng = StdRng::seed_from_u64(3);
    match null_density {
        None => {
            let v = (0..size).map(|_| rng.gen()).collect::<Vec<i32>>();
            Series::from_data(v)
        }
        Some(null_density) => {
            let v = (0..size)
                .map(|_| {
                    if rng.gen::<f32>() < null_density {
                        None
                    } else {
                        Some(rng.gen())
                    }
                })
                .collect::<Vec<Option<i32>>>();
            Series::from_data(v)
        }
    }
}
use std::string::String;
pub fn create_string_array(size: usize, null_density: Option<f32>, item_size: usize) -> ColumnRef {
    let mut rng = StdRng::seed_from_u64(3);
    match null_density {
        None => {
            let vec: Vec<String> = (0..size)
                .map(|_| {
                    (&mut rng)
                        .sample_iter(&Alphanumeric)
                        .take(item_size)
                        .map(char::from)
                        .collect::<String>()
                })
                .collect();
            Series::from_data(vec)
        }
        Some(null_density) => {
            let vec: Vec<_> = (0..item_size)
                .map(|_| {
                    if rng.gen::<f32>() < null_density {
                        None
                    } else {
                        let value = (&mut rng)
                            .sample_iter(&Alphanumeric)
                            .take(size)
                            .collect::<Vec<u8>>();
                        Some(value)
                    }
                })
                .collect();
            Series::from_data(vec)
        }
    }
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
