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

use databend_common_column::bitmap::MutableBitmap;
use databend_common_expression::Column;
use databend_common_expression::FromData;
use databend_common_expression::TopKSorter;
use databend_common_expression::types::StringType;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;

/// The top `limit` values seen so far, kept the way `TopKSorter` keeps them: a value is taken if
/// it does not rank after the worst value kept, and then replaces it.
struct Model {
    values: Vec<String>,
    limit: usize,
    asc: bool,
}

impl Model {
    fn worst(&self) -> Option<&String> {
        if self.asc {
            self.values.iter().max()
        } else {
            self.values.iter().min()
        }
    }

    fn push(&mut self, value: &str) -> bool {
        if self.values.len() < self.limit {
            self.values.push(value.to_string());
            return true;
        }
        let worst = self.worst().unwrap().clone();
        let taken = if self.asc {
            value <= worst.as_str()
        } else {
            value >= worst.as_str()
        };
        if taken {
            let pos = self.values.iter().position(|v| *v == worst).unwrap();
            self.values[pos] = value.to_string();
        }
        taken
    }

    /// Whether no value can be taken: all of them rank after the worst value kept.
    fn never_match_any(&self, values: &[String]) -> bool {
        if self.values.len() < self.limit {
            return false;
        }
        let worst = self.worst().unwrap();
        values
            .iter()
            .all(|v| if self.asc { v > worst } else { v < worst })
    }
}

/// Strings mixing inlined and long values that often share a prefix.
fn random_strings(rng: &mut StdRng, max_len: usize) -> Vec<String> {
    const PARTS: [&str; 5] = ["", "a", "ab\0", "https://www.example.com/", "\u{e9}"];
    (0..rng.gen_range(0..=max_len))
        .map(|_| {
            (0..rng.gen_range(0..=4))
                .map(|_| PARTS[rng.gen_range(0..PARTS.len())])
                .collect()
        })
        .collect()
}

#[test]
fn test_topk_string_push_column() {
    let mut rng = StdRng::seed_from_u64(0x70bc);
    for _ in 0..200 {
        let limit = rng.gen_range(1..=8);
        let asc = rng.gen_bool(0.5);
        let mut sorter = TopKSorter::new(limit, asc);
        let mut model = Model {
            values: vec![],
            limit,
            asc,
        };
        for _ in 0..rng.gen_range(1..=4) {
            let values = random_strings(&mut rng, 32);
            let column = StringType::from_data(values.clone());

            let probe = random_strings(&mut rng, 8);
            assert_eq!(
                sorter.never_match_any(&StringType::from_data(probe.clone())),
                model.never_match_any(&probe),
                "never_match_any({probe:?})"
            );

            let selected = (0..values.len())
                .map(|_| rng.gen_bool(0.8))
                .collect::<Vec<_>>();
            let mut bitmap = MutableBitmap::from_iter(selected.iter().copied());
            sorter.push_column(&column, &mut bitmap);
            let expected = values
                .iter()
                .zip(&selected)
                .map(|(value, selected)| *selected && model.push(value))
                .collect::<Vec<_>>();
            assert_eq!(bitmap.iter().collect::<Vec<_>>(), expected, "{values:?}");
        }
    }
}

#[test]
fn test_topk_string_push_column_with_selection() {
    let mut rng = StdRng::seed_from_u64(0x5e1c);
    for _ in 0..200 {
        let limit = rng.gen_range(1..=8);
        let asc = rng.gen_bool(0.5);
        let mut sorter = TopKSorter::new(limit, asc);
        let mut model = Model {
            values: vec![],
            limit,
            asc,
        };
        for _ in 0..rng.gen_range(1..=4) {
            let values = random_strings(&mut rng, 32);
            let column: Column = StringType::from_data(values.clone());

            let select_all = rng.gen_bool(0.5);
            let mut selection = (0..values.len() as u32)
                .filter(|_| select_all || rng.gen_bool(0.8))
                .collect::<Vec<_>>();
            let count = selection.len();
            let expected = selection
                .iter()
                .copied()
                .filter(|&row| model.push(&values[row as usize]))
                .collect::<Vec<_>>();
            let result_count = if select_all {
                sorter.push_column_with_selection::<true>(&column, &mut selection, count)
            } else {
                sorter.push_column_with_selection::<false>(&column, &mut selection, count)
            };
            assert_eq!(&selection[..result_count], expected, "{values:?}");
        }
    }
}
