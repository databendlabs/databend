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

#[macro_use]
extern crate criterion;

use arrow_buffer::BooleanBuffer;
use arrow_buffer::ScalarBuffer;
use criterion::Criterion;
use databend_common_base::vec_ext::VecExt;
use databend_common_column::bitmap::Bitmap;
use databend_common_column::buffer::Buffer;
use databend_common_expression::arrow::deserialize_column;
use databend_common_expression::arrow::serialize_column;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::StringType;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Value;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_kernels");

    let mut rng = StdRng::seed_from_u64(0);
    // concats
    {
        for length in [12, 20, 500] {
            let (s, b) = generate_random_string_data(&mut rng, length);
            let bin_col = (0..5).map(|_| BinaryType::from_data(b.clone()));
            let str_col = (0..5).map(|_| StringType::from_data(s.clone()));

            group.bench_function(format!("concat_string_offset/{length}"), |b| {
                b.iter(|| Column::concat_columns(bin_col.clone()).unwrap())
            });

            group.bench_function(format!("concat_string_view/{length}"), |b| {
                b.iter(|| Column::concat_columns(str_col.clone()).unwrap())
            });

            let binary_col = BinaryType::column_from_iter(b.iter().cloned(), &[]);
            let mut c = 0;
            group.bench_function(format!("binary_sum_len/{length}"), |b| {
                b.iter(|| {
                    let mut sum = 0;
                    for i in 0..binary_col.len() {
                        sum += binary_col.value(i).len();
                    }

                    c += sum;
                })
            });

            group.bench_function(format!("binary_sum_len_unchecked/{length}"), |b| {
                b.iter(|| {
                    let mut sum = 0;
                    for i in 0..binary_col.len() {
                        sum += unsafe { binary_col.index_unchecked(i).len() };
                    }
                    c += sum;
                })
            });
        }
    }

    // take compact
    {
        for length in [12, 20, 500] {
            let (s, b) = generate_random_string_data(&mut rng, length);
            let block_bin = DataBlock::new_from_columns(vec![BinaryType::from_data(b.clone())]);
            let block_view = DataBlock::new_from_columns(vec![StringType::from_data(s.clone())]);

            let indices: Vec<(u32, u32)> = (0..s.len())
                .filter(|x| x % 10 == 0)
                .map(|x| (x as u32, 1000))
                .collect();
            let num_rows = indices.len() * 1000;

            group.bench_function(format!("take_compact_string_offset/{length}"), |b| {
                b.iter(|| {
                    block_bin
                        .take_compacted_indices(&indices, num_rows)
                        .unwrap()
                })
            });

            group.bench_function(format!("take_compact_string_view/{length}"), |b| {
                b.iter(|| {
                    block_view
                        .take_compacted_indices(&indices, num_rows)
                        .unwrap()
                })
            });
        }
    }

    // IPC
    //     bench_kernels/serialize_string_offset/12
    //                         time:   [183.25 µs 183.49 µs 183.93 µs]
    // Found 7 outliers among 100 measurements (7.00%)
    //   3 (3.00%) high mild
    //   4 (4.00%) high severe
    // bench_kernels/serialize_string_view/12
    //                         time:   [415.25 µs 415.36 µs 415.47 µs]
    // Found 6 outliers among 100 measurements (6.00%)
    //   3 (3.00%) high mild
    //   3 (3.00%) high severe
    // bench_kernels/serialize_string_offset/20
    //                         time:   [195.09 µs 195.15 µs 195.23 µs]
    // Found 6 outliers among 100 measurements (6.00%)
    //   6 (6.00%) high mild
    // bench_kernels/serialize_string_view/20
    //                         time:   [464.96 µs 465.08 µs 465.21 µs]
    // Found 4 outliers among 100 measurements (4.00%)
    //   4 (4.00%) high mild
    // bench_kernels/serialize_string_offset/500
    //                         time:   [3.3092 ms 3.3139 ms 3.3194 ms]
    // Found 2 outliers among 100 measurements (2.00%)
    //   1 (1.00%) high mild
    //   1 (1.00%) high severe
    // bench_kernels/serialize_string_view/500
    //                         time:   [3.9254 ms 3.9303 ms 3.9366 ms]
    // Found 9 outliers among 100 measurements (9.00%)
    //   4 (4.00%) high mild
    //   5 (5.00%) high severe

    {
        for length in [12, 20, 500] {
            let (s, b) = generate_random_string_data(&mut rng, length);
            let b_c = BinaryType::from_data(b.clone());
            let s_c = StringType::from_data(s.clone());

            group.bench_function(format!("serialize_string_offset/{length}"), |b| {
                b.iter(|| {
                    let bs = serialize_column(&b_c);
                    deserialize_column(&bs).unwrap();
                })
            });

            group.bench_function(format!("serialize_string_view/{length}"), |b| {
                b.iter(|| {
                    let bs = serialize_column(&s_c);
                    deserialize_column(&bs).unwrap();
                })
            });
        }
    }

    for length in [10240, 102400] {
        let (left, right) = generate_random_int_data(&mut rng, length);

        let left_scalar = ScalarBuffer::from_iter(left.iter().cloned());
        let right_scalar = ScalarBuffer::from_iter(right.iter().cloned());

        group.bench_function(format!("function_iterator_iterator_v1/{length}"), |b| {
            b.iter(|| {
                let left = left.clone();
                let right = right.clone();

                let _c = left
                    .into_iter()
                    .zip(right.into_iter())
                    .map(|(a, b)| a + b)
                    .collect::<Vec<i32>>();
            })
        });

        group.bench_function(format!("function_iterator_iterator_v2/{length}"), |b| {
            b.iter(|| {
                let _c = left
                    .iter()
                    .cloned()
                    .zip(right.iter().cloned())
                    .map(|(a, b)| a + b)
                    .collect::<Vec<i32>>();
            })
        });

        group.bench_function(
            format!("function_buffer_index_unchecked_iterator/{length}"),
            |b| {
                b.iter(|| {
                    let _c = (0..length)
                        .map(|i| unsafe { left.get_unchecked(i) + right.get_unchecked(i) })
                        .collect::<Vec<i32>>();
                })
            },
        );

        group.bench_function(
            format!("function_buffer_index_unchecked_push/{length}"),
            |b| {
                b.iter(|| {
                    let mut c = Vec::with_capacity(length);
                    for i in 0..length {
                        unsafe { c.push_unchecked(left.get_unchecked(i) + right.get_unchecked(i)) };
                    }
                })
            },
        );

        group.bench_function(
            format!("function_buffer_scalar_index_unchecked_iterator/{length}"),
            |b| {
                b.iter(|| {
                    let _c = (0..length)
                        .map(|i| unsafe {
                            left_scalar.get_unchecked(i) + right_scalar.get_unchecked(i)
                        })
                        .collect::<Vec<i32>>();
                })
            },
        );

        group.bench_function(
            format!("function_slice_index_unchecked_iterator/{length}"),
            |b| {
                b.iter(|| {
                    let left = left.as_slice();
                    let right = right.as_slice();

                    let _c = (0..length)
                        .map(|i| unsafe { left.get_unchecked(i) + right.get_unchecked(i) })
                        .collect::<Vec<i32>>();
                })
            },
        );

        let left_vec: Vec<_> = left.iter().cloned().collect();
        let right_vec: Vec<_> = right.iter().cloned().collect();

        group.bench_function(
            format!("function_vec_index_unchecked_iterator/{length}"),
            |b| {
                b.iter(|| {
                    let _c = (0..length)
                        .map(|i| unsafe { left_vec.get_unchecked(i) + right_vec.get_unchecked(i) })
                        .collect::<Vec<i32>>();
                })
            },
        );

        group.bench_function(format!("function_buffer_index_iterator/{length}"), |b| {
            b.iter(|| {
                let _c = (0..length)
                    .map(|i| left[i] + right[i])
                    .collect::<Vec<i32>>();
            })
        });

        let value1 = Value::<Int32Type>::Column(left.clone());
        let value2 = Value::<Int32Type>::Column(right.clone());

        group.bench_function(format!("register_new/{length}"), |b| {
            b.iter(|| {
                let iter = (0..length).map(|i| {
                    let a = unsafe { value1.index_unchecked(i) };
                    let b = unsafe { value2.index_unchecked(i) };
                    a + b
                });
                let _c = Int32Type::column_from_iter(iter, &[]);
            })
        });

        group.bench_function(format!("register_old/{length}"), |b| {
            b.iter(|| {
                let a = value1.as_column().unwrap();
                let b = value2.as_column().unwrap();
                let iter = a.iter().zip(b.iter()).map(|(a, b)| *a + b);
                let _c = Int32Type::column_from_iter(iter, &[]);
            })
        });

        group.bench_function(format!("bitmap_from_arrow1_collect_bool/{length}"), |b| {
            b.iter(|| {
                let buffer = collect_bool(length, false, |x| x % 2 == 0);
                assert!(buffer.count_set_bits() == length / 2);
            })
        });

        group.bench_function(format!("bitmap_from_arrow2_collect_bool/{length}"), |b| {
            b.iter(|| {
                let nulls = Bitmap::collect_bool(length, |x| x % 2 == 0);
                assert!(nulls.null_count() == length / 2);
            })
        });

        group.bench_function(format!("bitmap_from_arrow2/{length}"), |b| {
            b.iter(|| {
                let nulls = Bitmap::from_trusted_len_iter((0..length).map(|x| x % 2 == 0));
                assert!(nulls.null_count() == length / 2);
            })
        });
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);

fn generate_random_string_data(rng: &mut StdRng, length: usize) -> (Vec<String>, Vec<Vec<u8>>) {
    let iter_str: Vec<_> = (0..102400)
        .map(|_| {
            let random_string: String = (0..length)
                .map(|_| {
                    // Generate a random character (ASCII printable characters)
                    rng.gen_range(32..=126) as u8 as char
                })
                .collect();
            random_string
        })
        .collect();

    let iter_binary: Vec<_> = iter_str
        .iter()
        .map(|x| x.clone().as_bytes().to_vec())
        .collect();

    (iter_str, iter_binary)
}

fn generate_random_int_data(rng: &mut StdRng, length: usize) -> (Buffer<i32>, Buffer<i32>) {
    let s: Buffer<i32> = (0..length).map(|_| rng.gen_range(-1000..1000)).collect();
    let b: Buffer<i32> = (0..length).map(|_| rng.gen_range(-1000..1000)).collect();
    (s, b)
}

/// Invokes `f` with values `0..len` collecting the boolean results into a new `BooleanBuffer`
///
/// This is similar to [`MutableBuffer::collect_bool`] but with
/// the option to efficiently negate the result
fn collect_bool(len: usize, neg: bool, f: impl Fn(usize) -> bool) -> BooleanBuffer {
    let mut buffer = arrow_buffer::MutableBuffer::new(arrow_buffer::bit_util::ceil(len, 64) * 8);

    let chunks = len / 64;
    let remainder = len % 64;
    for chunk in 0..chunks {
        let mut packed = 0;
        for bit_idx in 0..64 {
            let i = bit_idx + chunk * 64;
            packed |= (f(i) as u64) << bit_idx;
        }
        if neg {
            packed = !packed
        }

        // SAFETY: Already allocated sufficient capacity
        unsafe { buffer.push_unchecked(packed) }
    }

    if remainder != 0 {
        let mut packed = 0;
        for bit_idx in 0..remainder {
            let i = bit_idx + chunks * 64;
            packed |= (f(i) as u64) << bit_idx;
        }
        if neg {
            packed = !packed
        }

        // SAFETY: Already allocated sufficient capacity
        unsafe { buffer.push_unchecked(packed) }
    }
    BooleanBuffer::new(buffer.into(), 0, len)
}
