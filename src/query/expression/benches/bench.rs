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

use arrow_buffer::BooleanBuffer;
use arrow_buffer::ScalarBuffer;
use databend_common_base::vec_ext::VecExt;
use databend_common_column::bitmap::Bitmap;
use databend_common_column::buffer::Buffer;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::arrow::deserialize_column;
use databend_common_expression::arrow::serialize_column;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::DecimalType;
use databend_common_expression::types::ReturnType;
use databend_common_expression::types::StringType;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;

fn main() {
    // Run registered benchmarks.
    divan::main();
}

// bench                    fastest       │ slowest       │ median        │ mean          │ samples │ iters
// ├─ concat_string_offset                │               │               │               │         │
// │  ├─ 12                 22.58 ms      │ 31.43 ms      │ 24.05 ms      │ 24.83 ms      │ 100     │ 100
// │  ├─ 20                 23.32 ms      │ 29.58 ms      │ 26.87 ms      │ 26.44 ms      │ 100     │ 100
// │  ╰─ 500                295.2 ms      │ 314.2 ms      │ 301.4 ms      │ 302 ms        │ 100     │ 100
// ╰─ concat_string_view                  │               │               │               │         │
//    ├─ 12                 23.68 ms      │ 25.96 ms      │ 24.42 ms      │ 24.46 ms      │ 100     │ 100
//    ├─ 20                 26.27 ms      │ 27.79 ms      │ 26.85 ms      │ 26.85 ms      │ 100     │ 100
//    ╰─ 500                118.8 ms      │ 247.2 ms      │ 121.5 ms      │ 123.3 ms      │ 100     │ 100
#[divan::bench(args = [12, 20, 500])]
fn concat_string_offset(bencher: divan::Bencher, length: usize) {
    let mut rng = StdRng::seed_from_u64(0);
    let (_s, b) = generate_random_string_data(&mut rng, length);
    let bin_col = (0..5).map(|_| BinaryType::from_data(b.clone()));

    bencher.bench(|| {
        Column::concat_columns(bin_col.clone()).unwrap();
    });
}

#[divan::bench(args = [12, 20, 500])]
fn concat_string_view(bencher: divan::Bencher, length: usize) {
    let mut rng = StdRng::seed_from_u64(0);
    let (s, _b) = generate_random_string_data(&mut rng, length);
    let str_col = (0..5).map(|_| StringType::from_data(s.clone()));
    bencher.bench(|| {
        Column::concat_columns(str_col.clone()).unwrap();
    });
}

#[divan::bench(args = [12, 20, 500])]
fn take_compact_string_offset(bencher: divan::Bencher, length: usize) {
    let mut rng = StdRng::seed_from_u64(0);
    let (s, b) = generate_random_string_data(&mut rng, length);
    let block_bin = DataBlock::new_from_columns(vec![BinaryType::from_data(b.clone())]);
    let indices: Vec<(u32, u32)> = (0..s.len())
        .filter(|x| x % 10 == 0)
        .map(|x| (x as u32, 1000))
        .collect();
    let num_rows = indices.len() * 1000;
    bencher.bench(|| {
        block_bin
            .take_compacted_indices(&indices, num_rows)
            .unwrap();
    });
}

#[divan::bench(args = [12, 20, 500])]
fn take_compact_string_view(bencher: divan::Bencher, length: usize) {
    let mut rng = StdRng::seed_from_u64(0);
    let (s, _b) = generate_random_string_data(&mut rng, length);
    let block_view = DataBlock::new_from_columns(vec![StringType::from_data(s.clone())]);
    let indices: Vec<(u32, u32)> = (0..s.len())
        .filter(|x| x % 10 == 0)
        .map(|x| (x as u32, 1000))
        .collect();
    let num_rows = indices.len() * 1000;
    bencher.bench(|| {
        block_view
            .take_compacted_indices(&indices, num_rows)
            .unwrap();
    });
}

// bench                       fastest       │ slowest       │ median        │ mean          │ samples │ iters
// ├─ serialize_string_offset                │               │               │               │         │
// │  ├─ 12                    3.057 ms      │ 4.628 ms      │ 3.194 ms      │ 3.265 ms      │ 100     │ 100
// │  ├─ 20                    4.651 ms      │ 6.266 ms      │ 4.857 ms      │ 4.911 ms      │ 100     │ 100
// │  ╰─ 500                   50.15 ms      │ 58.9 ms       │ 52.54 ms      │ 53 ms         │ 100     │ 100
// ╰─ serialize_string_view                  │               │               │               │         │
//    ├─ 12                    3.221 ms      │ 3.79 ms       │ 3.335 ms      │ 3.331 ms      │ 100     │ 100
//    ├─ 20                    3.838 ms      │ 4.502 ms      │ 3.932 ms      │ 3.977 ms      │ 100     │ 100
//    ╰─ 500                   69.78 ms      │ 74.67 ms      │ 70.88 ms      │ 71.05 ms      │ 100     │ 100
#[divan::bench(args = [12, 20, 500])]
fn serialize_string_offset(bencher: divan::Bencher, length: usize) {
    let mut rng = StdRng::seed_from_u64(0);
    let (_s, b) = generate_random_string_data(&mut rng, length);
    let b_c = BinaryType::from_data(b.clone());

    bencher.bench(|| {
        let bs = serialize_column(&b_c);
        deserialize_column(&bs).unwrap();
    });
}

#[divan::bench(args = [12, 20, 500])]
fn serialize_string_view(bencher: divan::Bencher, length: usize) {
    let mut rng = StdRng::seed_from_u64(0);
    let (s, _b) = generate_random_string_data(&mut rng, length);
    let s_c = StringType::from_data(s.clone());

    bencher.bench(|| {
        let bs = serialize_column(&s_c);
        deserialize_column(&bs).unwrap();
    });
}

// bench                                               fastest       │ slowest       │ median        │ mean          │ samples │ iters
// ├─ function_buffer_index_unchecked_iterator                       │               │               │               │         │
// │  ├─ 10240                                         18.17 µs      │ 76.9 µs       │ 18.54 µs      │ 19.24 µs      │ 100     │ 100
// │  ╰─ 102400                                        183.1 µs      │ 508.8 µs      │ 186.8 µs      │ 194.7 µs      │ 100     │ 100
// ├─ function_buffer_index_unchecked_push                           │               │               │               │         │
// │  ├─ 10240                                         18.52 µs      │ 20.83 µs      │ 18.55 µs      │ 18.64 µs      │ 100     │ 100
// │  ╰─ 102400                                        187.6 µs      │ 439.7 µs      │ 191.2 µs      │ 192.8 µs      │ 100     │ 100
// ├─ function_buffer_scalar_index_unchecked_iterator                │               │               │               │         │
// │  ├─ 10240                                         11.58 µs      │ 12.94 µs      │ 11.6 µs       │ 11.63 µs      │ 100     │ 100
// │  ╰─ 102400                                        115.9 µs      │ 492.3 µs      │ 118.3 µs      │ 122.3 µs      │ 100     │ 100
// ├─ function_iterator_iterator_ref                                 │               │               │               │         │
// │  ├─ 10240                                         6.301 µs      │ 6.859 µs      │ 6.318 µs      │ 6.325 µs      │ 100     │ 100
// │  ╰─ 102400                                        77.07 µs      │ 390.6 µs      │ 77.27 µs      │ 81.39 µs      │ 100     │ 100
// ├─ function_iterator_iterator_v1                                  │               │               │               │         │
// │  ├─ 10240                                         9.502 µs      │ 14.74 µs      │ 9.535 µs      │ 9.694 µs      │ 100     │ 100
// │  ╰─ 102400                                        100.9 µs      │ 344.6 µs      │ 101 µs        │ 103.9 µs      │ 100     │ 100
// ╰─ function_iterator_iterator_v2                                  │               │               │               │         │
//    ├─ 10240                                         6.307 µs      │ 6.447 µs      │ 6.322 µs      │ 6.324 µs      │ 100     │ 100
//    ╰─ 102400                                        77.49 µs      │ 317.6 µs      │ 77.73 µs      │ 80.28 µs      │ 100     │ 100
#[divan::bench(args = [10240, 102400])]
fn function_iterator_iterator_v1(bencher: divan::Bencher, length: usize) {
    let mut rng = StdRng::seed_from_u64(0);
    let (left, right) = generate_random_i128_data(&mut rng, length);

    bencher.bench(|| {
        let left = left.clone();
        let right = right.clone();

        divan::black_box(
            left.into_iter()
                .zip(right)
                .map(|(a, b)| a * b)
                .collect::<Vec<i128>>(),
        )
    });
}

#[divan::bench(args = [10240, 102400])]
fn function_iterator_iterator_ref(bencher: divan::Bencher, length: usize) {
    let mut rng = StdRng::seed_from_u64(0);
    let (left, right) = generate_random_i128_data(&mut rng, length);

    bencher.bench(|| {
        divan::black_box(
            left.iter()
                .zip(right.iter())
                .map(|(a, b)| *a * *b)
                .collect::<Vec<i128>>(),
        )
    });
}

#[divan::bench(args = [10240, 102400])]
fn function_iterator_iterator_v2(bencher: divan::Bencher, length: usize) {
    let mut rng = StdRng::seed_from_u64(0);
    let (left, right) = generate_random_i128_data(&mut rng, length);

    bencher.bench(|| {
        let iter = left
            .iter()
            .cloned()
            .zip(right.iter().cloned())
            .map(|(a, b)| a * b);
        divan::black_box(DecimalType::<i128>::column_from_iter(iter, &[]))
    });
}

#[divan::bench(args = [10240, 102400])]
fn function_buffer_index_unchecked_iterator(bencher: divan::Bencher, length: usize) {
    let mut rng = StdRng::seed_from_u64(0);
    let (left, right) = generate_random_i128_data(&mut rng, length);

    bencher.bench(|| {
        divan::black_box(
            (0..length)
                .map(|i| unsafe { left.get_unchecked(i) * right.get_unchecked(i) })
                .collect::<Vec<i128>>(),
        )
    });
}

#[divan::bench(args = [10240, 102400])]
fn function_buffer_index_unchecked_push(bencher: divan::Bencher, length: usize) {
    let mut rng = StdRng::seed_from_u64(0);
    let (left, right) = generate_random_i128_data(&mut rng, length);

    bencher.bench(|| {
        let mut c = Vec::with_capacity(length);
        for i in 0..length {
            unsafe { c.push_unchecked(left.get_unchecked(i) * right.get_unchecked(i)) };
        }
    });
}

#[divan::bench(args = [10240, 102400])]
fn function_buffer_scalar_index_unchecked_iterator(bencher: divan::Bencher, length: usize) {
    let mut rng = StdRng::seed_from_u64(0);
    let (left, right) = generate_random_i128_data(&mut rng, length);
    let left_scalar = ScalarBuffer::from_iter(left.iter().cloned());
    let right_scalar = ScalarBuffer::from_iter(right.iter().cloned());

    bencher.bench(|| {
        divan::black_box(
            (0..length)
                .map(|i| unsafe { left_scalar.get_unchecked(i) * right_scalar.get_unchecked(i) })
                .collect::<Vec<i128>>(),
        )
    });
}

// Timer precision: 10 ns
// bench                               fastest       │ slowest       │ median        │ mean          │ samples │ iters
// ├─ bitmap_from_arrow1_collect_bool                │               │               │               │         │
// │  ├─ 10240                         216.8 ns      │ 4.312 µs      │ 218.8 ns      │ 263.7 ns      │ 100     │ 100
// │  ╰─ 102400                        1.425 µs      │ 1.673 µs      │ 1.433 µs      │ 1.44 µs       │ 100     │ 100
// ├─ bitmap_from_arrow2                             │               │               │               │         │
// │  ├─ 10240                         4.427 µs      │ 6.18 µs       │ 4.572 µs      │ 4.855 µs      │ 100     │ 100
// │  ╰─ 102400                        43.84 µs      │ 62.38 µs      │ 54.28 µs      │ 53.52 µs      │ 100     │ 100
// ╰─ bitmap_from_arrow2_collect_bool                │               │               │               │         │
//    ├─ 10240                         175.6 ns      │ 195.2 ns      │ 179.8 ns      │ 180.3 ns      │ 100     │ 800
//    ╰─ 102400                        1.487 µs      │ 1.6 µs        │ 1.501 µs      │ 1.504 µs      │ 100     │ 100
#[divan::bench(args = [10240, 102400])]
fn bitmap_from_arrow1_collect_bool(bencher: divan::Bencher, length: usize) {
    bencher.bench(|| {
        let buffer = collect_bool(length, false, |x| x % 2 == 0);
        assert!(buffer.count_set_bits() == length / 2);
    });
}

#[divan::bench(args = [10240, 102400])]
fn bitmap_from_arrow2_collect_bool(bencher: divan::Bencher, length: usize) {
    bencher.bench(|| {
        let nulls = Bitmap::collect_bool(length, |x| x % 2 == 0);
        assert!(nulls.null_count() == length / 2);
    });
}

#[divan::bench(args = [10240, 102400])]
fn bitmap_from_arrow2(bencher: divan::Bencher, length: usize) {
    bencher.bench(|| {
        let nulls = Bitmap::from_trusted_len_iter((0..length).map(|x| x % 2 == 0));
        assert!(nulls.null_count() == length / 2);
    });
}

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

fn generate_random_i128_data(rng: &mut StdRng, length: usize) -> (Buffer<i128>, Buffer<i128>) {
    let s: Buffer<i128> = (0..length).map(|_| rng.gen_range(-1000..1000)).collect();
    let b: Buffer<i128> = (0..length).map(|_| rng.gen_range(-1000..1000)).collect();
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
