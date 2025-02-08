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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::hash::Hasher;
use std::io::Write;

/// Mask the least significant `num_bits` of `x`.
fn mask_bits(x: u64, num_bits: usize) -> u64 {
    x & ((1u64 << num_bits) - 1)
}

/// Apply Feistel network round to the least significant `num_bits` part of `x`.
fn feistel_round(x: u64, num_bits: usize, seed: u64, round: usize) -> u64 {
    let num_bits_left_half = num_bits / 2;
    let num_bits_right_half = num_bits - num_bits_left_half;

    let left_half = mask_bits(x >> num_bits_right_half, num_bits_left_half);
    let right_half = mask_bits(x, num_bits_right_half);

    let new_left_half = right_half;

    let mut state = std::hash::DefaultHasher::new();
    state.write_u64(right_half);
    state.write_u64(seed);
    state.write_usize(round);
    let new_right_half = left_half ^ mask_bits(state.finish(), num_bits_left_half);

    (new_left_half << num_bits_left_half) ^ new_right_half
}

/// Apply Feistel network with `num_rounds` to the least significant `num_bits` part of `x`.
fn feistel_network(x: u64, num_bits: usize, seed: u64, num_rounds: usize) -> u64 {
    let mut bits = mask_bits(x, num_bits);
    for i in 0..num_rounds {
        bits = feistel_round(bits, num_bits, seed, i);
    }
    (x & !((1u64 << num_bits) - 1)) ^ bits
}

/// Pseudorandom permutation within the set of numbers with the same log2(x).
pub fn transform(x: u64, seed: u64) -> u64 {
    // Keep 0 and 1 as is.
    if x == 0 || x == 1 {
        return x;
    }

    // Pseudorandom permutation of two elements.
    if x == 2 || x == 3 {
        return x ^ (seed & 1);
    }

    let num_leading_zeros = x.leading_zeros() as usize;
    feistel_network(x, 64 - num_leading_zeros - 1, seed, 4)
}

type CodePoint = u32;
type NGramHash = u32;

#[derive(Debug, Clone)]
pub struct MarkovModelParameters {
    pub order: usize,
    pub frequency_cutoff: usize,
    pub num_buckets_cutoff: usize,
    pub frequency_add: usize,
    pub frequency_desaturate: f64,
    pub determinator_sliding_window_size: usize,
}

impl Default for MarkovModelParameters {
    fn default() -> Self {
        Self {
            order: 5,
            frequency_cutoff: 5,
            num_buckets_cutoff: 0,
            frequency_add: 0,
            frequency_desaturate: 0.0,
            determinator_sliding_window_size: 8,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct Histogram {
    total: usize,
    count_end: usize,
    buckets: BTreeMap<CodePoint, usize>,
}

impl Histogram {
    fn add(&mut self, code: Option<CodePoint>) {
        if let Some(code) = code {
            self.total += 1;
            *self.buckets.entry(code).or_insert(0) += 1;
        } else {
            self.count_end += 1;
        }
    }

    fn sample(&self, random: u64, end_multiplier: f64) -> Option<CodePoint> {
        let range = self.total + (self.count_end as f64 * end_multiplier) as usize;
        if range == 0 {
            return None;
        }

        let mut random = random as usize % range;
        self.buckets
            .iter()
            .find(|(_, weighted)| {
                if random <= **weighted {
                    true
                } else {
                    random -= **weighted;
                    false
                }
            })
            .map(|(code, _)| *code)
    }

    fn is_empty(&self) -> bool {
        self.total + self.count_end == 0
    }

    fn frequency_cutoff(&mut self, limit: usize) {
        if self.total < limit {
            self.buckets.clear();
            self.total = 0;
            return;
        }

        self.buckets.retain(|_, count| *count >= limit);
        self.total = self.buckets.values().sum();
    }

    fn frequency_add(&mut self, n: usize) {
        if self.total == 0 {
            return;
        }
        self.count_end += n;
        for count in self.buckets.values_mut() {
            *count += n;
        }
        self.total += n * self.buckets.len();
    }

    fn frequency_desaturate(&mut self, p: f64) {
        if self.total == 0 {
            return;
        }

        let average = (self.total as f64 / self.buckets.len() as f64 * p) as usize;

        for count in self.buckets.values_mut() {
            *count = average + (*count as f64 * (1.0 - p)) as usize;
        }
        self.total = self.buckets.values().sum();
    }
}

#[derive(Debug, Clone)]
pub struct MarkovModel {
    params: MarkovModelParameters,
    table: HashMap<NGramHash, Histogram>,
}

impl MarkovModel {
    pub fn new(params: MarkovModelParameters) -> Self {
        MarkovModel {
            params,
            table: HashMap::new(),
        }
    }

    pub fn consume(&mut self, data: &[u8], code_points: &mut Vec<CodePoint>) {
        code_points.clear();

        let mut pos = 0;
        loop {
            let next_code_point = if pos < data.len() {
                let (code, n) = Self::read_code_point(&data[pos..]);
                pos += n;
                Some(code)
            } else {
                None
            };

            for context_size in 0..self.params.order {
                let context_hash = self.hash_context(context_size, code_points);

                let histogram = self.table.entry(context_hash).or_default();
                histogram.add(next_code_point);
            }

            if let Some(code) = next_code_point {
                code_points.push(code);
            } else {
                break;
            };
        }
    }

    fn read_code_point(data: &[u8]) -> (CodePoint, usize) {
        let length = utf8_char_width(data[0]).max(1).min(data.len());
        let mut buf = [0u8; 4];
        buf[..length].copy_from_slice(&data[..length]);
        (u32::from_le_bytes(buf), length)
    }

    pub fn finalize(&mut self) {
        if self.params.num_buckets_cutoff > 0 {
            self.table
                .retain(|_, histogram| histogram.buckets.len() >= self.params.num_buckets_cutoff);
        }

        if self.params.frequency_cutoff > 1 {
            for histogram in self.table.values_mut() {
                histogram.frequency_cutoff(self.params.frequency_cutoff);
            }
        }

        if self.params.frequency_add > 0 {
            for histogram in self.table.values_mut() {
                histogram.frequency_add(self.params.frequency_add);
            }
        }

        if self.params.frequency_desaturate > 0.0 {
            for histogram in self.table.values_mut() {
                histogram.frequency_desaturate(self.params.frequency_desaturate);
            }
        }

        self.table.retain(|_, histogram| !histogram.is_empty());
    }

    pub fn is_valid(&self) -> bool {
        !self.table.is_empty()
    }

    pub fn generate_writer(
        &self,
        mut writer: &mut [u8],
        desired_size: usize,
        seed: u64,
        determinator_data: &[u8],
        code_points: &mut Vec<CodePoint>,
    ) -> usize {
        code_points.clear();

        let determinator_size = determinator_data.len();
        let sliding_window_size = self
            .params
            .determinator_sliding_window_size
            .min(determinator_size);

        let mut written = 0;

        while !writer.is_empty() {
            let Some(histogram) = (1..=self.params.order).rev().find_map(|size| {
                let context_hash = self.hash_context(size, code_points);

                let histogram = self.table.get(&context_hash)?;
                if histogram.is_empty() {
                    None
                } else {
                    Some(histogram)
                }
            }) else {
                panic!("Logical error in markov model")
            };

            let mut hash = std::hash::DefaultHasher::new();
            hash.write_u64(seed);

            let sliding_window_overflow = if written + sliding_window_size > determinator_size {
                written + sliding_window_size - determinator_size
            } else {
                0
            };

            let start = written - sliding_window_overflow;
            let end = start + sliding_window_size;
            hash.write(&determinator_data[start..end]);
            hash.write_usize(sliding_window_overflow);
            let determinator = hash.finish();

            // If string is greater than desired_size, increase probability of end.
            let greater_than_desired_size = written > desired_size;
            let end_probability_multiplier = if greater_than_desired_size {
                1.25_f64.powf((written - desired_size) as f64)
            } else {
                0.0
            };

            let Some(code) = histogram.sample(determinator, end_probability_multiplier) else {
                return written;
            };

            // Heuristic: break at ASCII non-alnum code point.
            // This allows to be close to desired_size but not break natural looking words.
            if greater_than_desired_size && code < 128 && !is_alpha_numeric_ascii(code) {
                return written;
            }

            let length = utf8_char_width((code >> 24) as u8).max(1);
            if length > writer.len() {
                return written;
            }

            let _ = writer.write(&code.to_le_bytes()[..length]).unwrap();
            written += length;
            code_points.push(code);
        }

        written
    }

    pub fn generate(
        &self,
        max_size: usize,
        desired_size: usize,
        seed: u64,
        determinator_data: &[u8],
    ) -> Vec<u8> {
        let mut data = vec![0; max_size];
        let mut code_points = Vec::new();

        let n = self.generate_writer(
            &mut data,
            desired_size,
            seed,
            determinator_data,
            &mut code_points,
        );
        data.truncate(n);
        data
    }

    fn hash_context(&self, context_size: usize, code_points: &[CodePoint]) -> NGramHash {
        const BEGIN: CodePoint = CodePoint::MAX;
        std::iter::repeat_n(BEGIN, self.params.order)
            .chain(code_points.iter().copied())
            .skip(self.params.order + code_points.len() - context_size)
            .fold(crc32fast::Hasher::new(), |mut acc, code| {
                acc.write_u32(code);
                acc
            })
            .finalize()
    }
}

fn is_alpha_numeric_ascii(code: CodePoint) -> bool {
    (code >= b'0' as CodePoint && code <= b'9' as CodePoint)
        || (code >= b'A' as CodePoint && code <= b'Z' as CodePoint)
        || (code >= b'a' as CodePoint && code <= b'z' as CodePoint)
}

// https://tools.ietf.org/html/rfc3629
const UTF8_CHAR_WIDTH: &[u8; 256] = &[
    // 1  2  3  4  5  6  7  8  9  A  B  C  D  E  F
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 1
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 2
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 3
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 4
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 5
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 6
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 7
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 8
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 9
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // A
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // B
    0, 0, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, // C
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, // D
    3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, // E
    4, 4, 4, 4, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // F
];

const fn utf8_char_width(b: u8) -> usize {
    UTF8_CHAR_WIDTH[b as usize] as usize
}

#[test]
fn xxx2() {
    let mut model = MarkovModel::new(MarkovModelParameters {
        frequency_cutoff: 2,
        ..Default::default()
    });

    let mut buffer = Vec::new();

    model.consume(b"data", &mut buffer);
    model.consume(b"1431078573", &mut buffer);
    model.consume(b"1431076677", &mut buffer);
    model.consume(b"3466776677", &mut buffer);
    model.consume(b"count_end", &mut buffer);
    model.consume(b"buckets", &mut buffer);

    println!("{model:?}");

    model.finalize();

    println!("{model:?}");

    let desired_size = 15;
    let seed = 0;

    let got = model.generate(10, desired_size, seed, b"buckets");
    let data = String::from_utf8_lossy(&got);
    println!("{data}");

    let got = model.generate(10, desired_size, seed, b"buckets");
    let data = String::from_utf8_lossy(&got);
    println!("{data}");
}

#[test]
fn xxx() {
    let x = 123456789;
    let seed = 42;
    let transformed = transform(x, seed);
    println!("Transformed: {}", transformed);
}
