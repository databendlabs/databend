extern crate core;

use common_datavalues::serializations::formats::csv;
use common_datavalues::ColumnRef;
use common_datavalues::Series;
use common_datavalues::SeriesFrom;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use rand::distributions::Alphanumeric;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;

type ColumnCreator = fn(usize, Option<f32>, usize) -> ColumnRef;

fn bench_name(typ: &str, null_name: &str, f_name: &str) -> String {
    format!("{}({})_{}", typ, null_name, f_name)
}

fn add_group(c: &mut Criterion, typ: &str, creator: ColumnCreator, item_size: usize) {
    let mut group = c.benchmark_group(typ);
    //let range = (10..=14);
    let range = (10..=21);
    range.step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);
        for null_density in [None, Some(0.1)] {
            let null_name = match null_density {
                None => "not_nullable".to_string(),
                Some(f) => format!("null={}", f),
            };

            let col = creator(size, null_density, item_size);
            group.bench_with_input(
                BenchmarkId::new(bench_name(typ, &null_name, "index"), size),
                &log2_size,
                |b, _| {
                    b.iter(|| csv::write_by_row(&col));
                },
            );
            group.bench_with_input(
                BenchmarkId::new(bench_name(typ, &null_name, "iter"), size),
                &log2_size,
                |b, _| {
                    b.iter(|| csv::write_iterator(&col));
                },
            );
            group.bench_with_input(
                BenchmarkId::new(bench_name(typ, &null_name, "embedded"), size),
                &log2_size,
                |b, _| {
                    b.iter(|| csv::write_embedded(&col));
                },
            );
        }
    });
}

fn add_benchmark(c: &mut Criterion) {
    add_group(c, "i32", create_primitive_array, 1);
    for strlen in [10, 100, 1000] {
        let typ = format!("str[{}]", strlen);
        add_group(c, &typ, create_string_array, strlen);
    }
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

#[allow(dead_code)]
pub fn create_string_array(size: usize, null_density: Option<f32>, item_size: usize) -> ColumnRef {
    let mut rng = StdRng::seed_from_u64(3);
    match null_density {
        None => {
            let vec: Vec<String> = (0..item_size)
                .map(|_| {
                    (&mut rng)
                        .sample_iter(&Alphanumeric)
                        .take(size)
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
