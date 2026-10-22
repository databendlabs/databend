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

//! Lookup-only benchmark: includes index construction/destruction per object,
//! excludes parsing. Compare full named tuples and narrow projections.
//! cargo run --release -p databend-common-formats --example json_object_lookup

use std::collections::HashMap;
use std::hint::black_box;
use std::time::Duration;
use std::time::Instant;

use databend_common_formats::json::JsonObjectLookup;
use databend_common_formats::json::JsonValue;
use databend_common_formats::json::parse_sonic;

fn measure(mut lookup: impl FnMut()) -> f64 {
    let start = Instant::now();
    let mut iterations = 0u64;
    while start.elapsed() < Duration::from_millis(100) {
        lookup();
        iterations += 1;
    }
    start.elapsed().as_nanos() as f64 / iterations as f64
}

#[inline]
fn linear_lookup<'a, V: JsonValue>(value: &'a V, key: &str, case_sensitive: bool) -> Option<&'a V> {
    if case_sensitive {
        value.object_get(key)
    } else {
        let key = key.to_lowercase();
        value
            .object_iter()?
            .find_map(|(k, v)| (k.to_lowercase() == key).then_some(v))
    }
}

fn run<V: JsonValue>(
    backend: &str,
    value: &V,
    fields: usize,
    queries: &[String],
    case_sensitive: bool,
    row: bool,
) {
    let old = measure(|| {
        if row && !case_sensitive {
            // This is the existing COPY path, not a repeated linear scan.
            let index: HashMap<_, _> = black_box(value)
                .object_iter()
                .unwrap()
                .map(|(key, value)| (key.to_lowercase(), value))
                .collect();
            for key in queries {
                black_box(index.get(black_box(key).to_lowercase().as_str()));
            }
        } else {
            for key in queries {
                black_box(linear_lookup(
                    black_box(value),
                    black_box(key),
                    case_sensitive,
                ));
            }
        }
    });
    let indexed = measure(|| {
        let lookup = if row {
            JsonObjectLookup::for_row(black_box(value), case_sensitive, queries.len())
        } else {
            JsonObjectLookup::for_tuple(black_box(value), case_sensitive, queries.len())
        };
        for key in queries {
            black_box(lookup.get(black_box(key)));
        }
    });
    println!(
        "{},{backend},{fields},{},{case_sensitive},{old:.0},{indexed:.0},{:.2}",
        if row { "row" } else { "tuple" },
        queries.len(),
        old / indexed
    );
}

fn main() {
    println!("kind,backend,fields,lookups,case_sensitive,old_ns,new_ns,speedup");
    for fields in [4, 16, 32, 64, 256, 1024] {
        let text = format!(
            "{{{}}}",
            (0..fields)
                .rev()
                .map(|i| format!("\"K{i:04}\":{i}"))
                .collect::<Vec<_>>()
                .join(",")
        );
        let serde: serde_json::Value = serde_json::from_str(&text).unwrap();
        let sonic = parse_sonic(text.as_bytes()).unwrap();
        for case_sensitive in [true, false] {
            let queries: Vec<_> = (0..fields)
                .map(|i| {
                    if case_sensitive {
                        format!("K{i:04}")
                    } else {
                        format!("k{i:04}")
                    }
                })
                .collect();
            let counts = [fields.min(4), fields.min(8), fields.min(32), fields];
            for (i, &count) in counts.iter().enumerate() {
                if counts[..i].contains(&count) {
                    continue;
                }
                for row in [false, true] {
                    run(
                        "serde",
                        &serde,
                        fields,
                        &queries[..count],
                        case_sensitive,
                        row,
                    );
                    run(
                        "sonic",
                        &sonic,
                        fields,
                        &queries[..count],
                        case_sensitive,
                        row,
                    );
                }
            }
        }
    }
}
