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

pub type CodePoint = u32;
pub type NGramHash = u32;

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

pub trait Histogram<'a> {
    fn sample(&self, random: u64, end_multiplier: f64) -> Option<CodePoint>;

    fn is_empty(&self) -> bool;
}

pub trait Table<'a, H: Histogram<'a>> {
    fn get(&'a self, context_hash: &NGramHash) -> Option<H>;
}

#[derive(Debug, Clone, Default)]
struct MapHistogram {
    total: usize,
    count_end: usize,
    buckets: BTreeMap<CodePoint, usize>,
}

impl MapHistogram {
    fn add(&mut self, code: Option<CodePoint>) {
        if let Some(code) = code {
            self.total += 1;
            *self.buckets.entry(code).or_insert(0) += 1;
        } else {
            self.count_end += 1;
        }
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

impl<'a> Histogram<'a> for &'a MapHistogram {
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
        self.total == 0 && self.count_end == 0
    }
}

#[derive(Debug, Clone)]
pub struct MarkovModel {
    params: MarkovModelParameters,
    table: HashMap<NGramHash, MapHistogram>,
}

impl MarkovModel {
    pub fn new(params: MarkovModelParameters) -> Self {
        MarkovModel {
            params,
            table: HashMap::new(),
        }
    }

    pub fn consume(&mut self, data: &[u8], code_points: &mut Vec<CodePoint>) {
        consume(
            self.params.order,
            data,
            |context_hash, code| {
                let histogram = self.table.entry(context_hash).or_default();
                histogram.add(code);
            },
            code_points,
        );
    }

    pub fn finalize(&mut self) {
        if self.params.num_buckets_cutoff > 0 {
            for histogram in self.table.values_mut() {
                if histogram.buckets.len() < self.params.num_buckets_cutoff {
                    histogram.buckets.clear();
                    histogram.total = 0;
                }
            }
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
    }

    pub fn generate_writer(
        &self,
        writer: &mut [u8],
        desired_size: usize,
        seed: u64,
        determinator_data: &[u8],
        code_points: &mut Vec<CodePoint>,
    ) -> Option<usize> {
        generate(
            self,
            self.params.order,
            seed,
            writer,
            desired_size,
            self.params.determinator_sliding_window_size,
            determinator_data,
            code_points,
        )
    }

    pub fn generate(
        &self,
        max_size: usize,
        desired_size: usize,
        seed: u64,
        determinator_data: &[u8],
    ) -> Option<Vec<u8>> {
        let mut data = vec![0; max_size];
        let mut code_points = Vec::new();

        let n = self.generate_writer(
            &mut data,
            desired_size,
            seed,
            determinator_data,
            &mut code_points,
        )?;
        data.truncate(n);
        Some(data)
    }
}

fn read_code_point(data: &[u8]) -> (CodePoint, usize) {
    let length = utf8_char_width(data[0]).max(1).min(data.len());
    let mut buf = [0u8; 4];
    buf[..length].copy_from_slice(&data[..length]);
    (u32::from_le_bytes(buf), length)
}

impl<'a> Table<'a, &'a MapHistogram> for MarkovModel {
    fn get(&'a self, context_hash: &NGramHash) -> Option<&'a MapHistogram> {
        self.table.get(context_hash)
    }
}

pub fn consume<F>(order: usize, data: &[u8], mut add_code: F, code_points: &mut Vec<CodePoint>)
where F: FnMut(NGramHash, Option<CodePoint>) {
    code_points.clear();

    let mut pos = 0;
    loop {
        let next_code_point = if pos < data.len() {
            let (code, n) = read_code_point(&data[pos..]);
            pos += n;
            Some(code)
        } else {
            None
        };

        for context_size in 0..order {
            let context_hash = hash_context(order, context_size, code_points);
            add_code(context_hash, next_code_point);
        }

        if let Some(code) = next_code_point {
            code_points.push(code);
        } else {
            break;
        };
    }
}

pub fn generate<'a, T, H>(
    table: &'a T,
    order: usize,
    seed: u64,
    mut writer: &mut [u8],
    desired_size: usize,
    determinator_sliding_window_size: usize,
    determinator_data: &[u8],
    code_points: &mut Vec<CodePoint>,
) -> Option<usize>
where
    T: Table<'a, H>,
    H: Histogram<'a>,
{
    use std::ops::ControlFlow;
    code_points.clear();

    let determinator_size = determinator_data.len();
    let sliding_window_size = determinator_sliding_window_size.min(determinator_size);

    let mut written = 0;

    while !writer.is_empty() {
        let reach_desired_size = written >= desired_size;
        let histogram = (1..=order).try_rfold(None, |prev, size| {
            let context_hash = hash_context(order, size, code_points);
            match table.get(&context_hash) {
                None => ControlFlow::Continue(prev),
                Some(v) if !reach_desired_size && v.is_empty() => ControlFlow::Continue(Some(v)),
                Some(v) => ControlFlow::Break(v),
            }
        });

        let histogram = match histogram {
            ControlFlow::Break(v) => v,
            ControlFlow::Continue(Some(v)) => v,
            ControlFlow::Continue(None) => return None, // logical error in markov model,
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
        let end_probability_multiplier = if reach_desired_size {
            1.25_f64.powf((written - desired_size) as f64)
        } else {
            0.0
        };

        let Some(code) = histogram.sample(determinator, end_probability_multiplier) else {
            return Some(written);
        };

        // Heuristic: break at ASCII non-alnum code point.
        // This allows to be close to desired_size but not break natural looking words.
        if reach_desired_size && code < 128 && !is_alpha_numeric_ascii(code) {
            return Some(written);
        }

        let length = utf8_char_width((code >> 24) as u8).max(1);
        if length > writer.len() {
            return Some(written);
        }

        let _ = writer.write(&code.to_le_bytes()[..length]).unwrap();
        written += length;
        code_points.push(code);
    }

    Some(written)
}

fn hash_context(order: usize, context_size: usize, code_points: &[CodePoint]) -> NGramHash {
    const BEGIN: CodePoint = CodePoint::MAX;
    std::iter::repeat_n(BEGIN, order)
        .chain(code_points.iter().copied())
        .skip(order + code_points.len() - context_size)
        .fold(crc32fast::Hasher::new(), |mut acc, code| {
            acc.write_u32(code);
            acc
        })
        .finalize()
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
