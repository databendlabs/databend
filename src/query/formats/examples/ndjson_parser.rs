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

//! Parser-only benchmark, including validation, compatibility checks and drop.
//! cargo run --release -p databend-common-formats --example ndjson_parser
//! Add --variant to include direct jsonb conversion and encoding.
//! Append an NDJSON file path to measure a real workload instead of synthetic rows.

fn main() {
    use std::hint::black_box;
    use std::time::Duration;
    use std::time::Instant;

    use databend_common_formats::json::JsonValue;
    use databend_common_formats::json::parse_sonic;

    fn measure(mut parse: impl FnMut(), bytes: usize) -> f64 {
        let start = Instant::now();
        let mut iterations = 0;
        while start.elapsed() < Duration::from_millis(500) {
            parse();
            iterations += 1;
        }
        (bytes * iterations) as f64 / start.elapsed().as_secs_f64() / (1024.0 * 1024.0)
    }

    let mut args: Vec<_> = std::env::args().skip(1).collect();
    let encode_jsonb = args.iter().any(|arg| arg == "--variant");
    args.retain(|arg| arg != "--variant");
    let cases = if let Some(path) = args.first() {
        vec![(path.clone(), std::fs::read_to_string(path).unwrap())]
    } else {
        vec![
            ("short".to_string(), r#"{"id":1,"name":"hello","ok":true}"#.to_string()),
            ("long-string".to_string(), serde_json::json!({"id":1,"text":"abcdef 汉字 ".repeat(256)}).to_string()),
            ("wide-numeric".to_string(), format!("{{{}}}", (0..100).map(|i| format!("\"c{i}\":12345678901234.5678")).collect::<Vec<_>>().join(","))),
            ("nested".to_string(), r#"{"id":1,"a":[{"x":165.6263,"y":"hello"},{"x":-0.0003,"y":"world"}],"b":{"ok":true}}"#.to_string()),
            ("duplicate-native".to_string(), r#"{"id":1,"id":2,"text":"hello"}"#.to_string()),
            ("large-array".to_string(), serde_json::json!({"items": (0..100).map(|i| serde_json::json!({"id": i,"text":"abcdef 汉字 ".repeat(8),"value": i as f64 + 0.1234})).collect::<Vec<_>>()}).to_string()),
        ]
    };
    println!(
        "mode={}",
        if encode_jsonb { "parse+jsonb" } else { "parse" }
    );
    println!("case,serde_MiB/s,sonic_MiB/s,speedup,fallback_rows,total_rows");
    for (name, data) in cases {
        let rows: Vec<_> = data
            .lines()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();
        let bytes = rows.iter().map(|s| s.len()).sum();
        // Validate the workload once, outside timing.
        for row in &rows {
            let serde: serde_json::Value = serde_json::from_str(row).unwrap();
            if encode_jsonb && let Some(sonic) = parse_sonic(row.as_bytes()) {
                let mut expected = Vec::new();
                let mut actual = Vec::new();
                serde.write_jsonb(&mut expected).unwrap();
                sonic.write_jsonb(&mut actual).unwrap();
                assert_eq!(actual, expected);
            }
        }
        let fallback = rows
            .iter()
            .filter(|row| parse_sonic(row.as_bytes()).is_none())
            .count();
        let mut out = Vec::new();
        let serde = measure(
            || {
                for row in &rows {
                    let value =
                        serde_json::from_slice::<serde_json::Value>(black_box(row.as_bytes()))
                            .unwrap();
                    if encode_jsonb {
                        out.clear();
                        value.write_jsonb(&mut out).unwrap();
                        black_box(&out);
                    }
                    black_box(value);
                }
            },
            bytes,
        );
        let sonic = measure(
            || {
                for row in &rows {
                    match parse_sonic(black_box(row.as_bytes())) {
                        Some(value) => {
                            if encode_jsonb {
                                out.clear();
                                value.write_jsonb(&mut out).unwrap();
                                black_box(&out);
                            }
                            black_box(value);
                        }
                        None => {
                            let value = serde_json::from_slice::<serde_json::Value>(row.as_bytes())
                                .unwrap();
                            if encode_jsonb {
                                out.clear();
                                value.write_jsonb(&mut out).unwrap();
                                black_box(&out);
                            }
                            black_box(value);
                        }
                    }
                }
            },
            bytes,
        );
        println!(
            "{name},{serde:.1},{sonic:.1},{:.2},{fallback},{}",
            sonic / serde,
            rows.len()
        );
    }
}
