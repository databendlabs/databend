extern crate core;

use common_datavalues::serializations::formats::csv;
use common_datavalues::ColumnRef;
use common_datavalues::Series;
use common_datavalues::SeriesFrom;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;

fn add_benchmark(c: &mut Criterion) {
    (10..=21).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);
        let col = create_primitive_array(size);
        c.bench_function(
            &format!("i32 2^{} not null, write_by_row", log2_size),
            |b| {
                b.iter(|| csv::write_by_row(&col));
            },
        );
        c.bench_function(
            &format!("i32 2^{} not null, write_iterator", log2_size),
            |b| {
                b.iter(|| csv::write_iterator(&col));
            },
        );
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);

pub fn create_primitive_array(size: usize) -> ColumnRef {
    let mut rng = seedable_rng();

    let v = (0..size).map(|_| rng.gen()).collect::<Vec<i32>>();
    Series::from_data(v)
}

pub fn seedable_rng() -> StdRng {
    StdRng::seed_from_u64(42)
}
