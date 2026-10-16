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

//! Inverted-index build benchmarks.
//!
//! Kept in a separate bench binary because it installs divan's allocation profiler, which would
//! otherwise add overhead to the other storage benchmarks. Run it on the commits being compared:
//!
//! ```text
//! cargo bench -p databend-common-storages-fuse --bench inverted_index
//! ```
//!
//! The output reports, per scenario, the time and the number/bytes of allocations.

use divan::AllocProfiler;

#[global_allocator]
static ALLOC: AllocProfiler = AllocProfiler::system();

fn main() {
    divan::main()
}

#[divan::bench_group(max_time = 5)]
mod inverted_index_writer {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::OnceLock;

    use databend_common_base::base::GlobalInstance;
    use databend_common_base::runtime::GlobalIORuntime;
    use databend_common_expression::BlockEntry;
    use databend_common_expression::DataBlock;
    use databend_common_expression::DataField;
    use databend_common_expression::DataSchema;
    use databend_common_expression::DataSchemaRef;
    use databend_common_expression::FromData;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::TableSchemaRef;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::StringType;
    use databend_common_expression::types::VariantType;
    use databend_common_storages_fuse::io::InvertedIndexWriter;
    use divan::counter::BytesCount;
    use opendal::Operator;
    use opendal::services::Memory;

    const WORDS: [&str; 8] = [
        "error", "request", "timeout", "user", "query", "storage", "alpha", "failed",
    ];

    struct Input {
        data_schema: DataSchemaRef,
        source_schema: TableSchemaRef,
        block: DataBlock,
        /// Bytes of indexed text (String values plus serialized JSONB), for throughput.
        bytes: usize,
        options: BTreeMap<String, String>,
        operator: Operator,
    }

    fn init_globals() {
        static INIT: OnceLock<()> = OnceLock::new();
        INIT.get_or_init(|| {
            // Streamed postings and `finalize()` write through the global IO runtime.
            GlobalInstance::init_production();
            GlobalIORuntime::init(1).unwrap();
        });
    }

    fn text_row(i: usize, words: usize) -> String {
        let mut text = format!("{i} ");
        for w in 0..words {
            text.push_str(WORDS[(i * 7 + w * 3) % WORDS.len()]);
            text.push(' ');
        }
        text
    }

    fn json_row(i: usize) -> Vec<u8> {
        let json = format!(
            r#"{{"level":"{}","status":{},"service":"svc-{}","latency":{}.5,"user":{{"id":{i},"name":"user-{}"}},"tags":["{}","{}"],"message":"{}"}}"#,
            ["info", "warn", "error"][i % 3],
            200 + i % 7 * 100,
            i % 31,
            i % 1000,
            i % 997,
            WORDS[i % 8],
            WORDS[(i + 3) % 8],
            text_row(i, 6).trim(),
        );
        jsonb::parse_value(json.as_bytes()).unwrap().to_vec()
    }

    fn string_column(rows: usize, words: usize) -> (BlockEntry, usize) {
        let values = (0..rows).map(|i| text_row(i, words)).collect::<Vec<_>>();
        let bytes = values.iter().map(String::len).sum();
        (StringType::from_data(values).into(), bytes)
    }

    fn variant_column(rows: usize) -> (BlockEntry, usize) {
        let values = (0..rows).map(json_row).collect::<Vec<_>>();
        let bytes = values.iter().map(Vec::len).sum();
        (VariantType::from_data(values).into(), bytes)
    }

    fn prepare(scenario: &str) -> Input {
        // `(name, is_variant, column, bytes)`
        let columns = match scenario {
            // Many short values: per-row document allocation dominates the copy savings.
            "short_text" => {
                let (column, bytes) = string_column(200_000, 4);
                vec![("body", false, column, bytes)]
            }
            // Few long values: tokenization dominates; copying full text is avoided.
            "long_text" => {
                let (column, bytes) = string_column(4_096, 400);
                vec![("body", false, column, bytes)]
            }
            // Only the CompactDoc re-encoding of the converted JSON value is avoided.
            "variant" => {
                let (column, bytes) = variant_column(50_000);
                vec![("payload", true, column, bytes)]
            }
            "mixed" => {
                let (text, text_bytes) = string_column(50_000, 12);
                let (json, json_bytes) = variant_column(50_000);
                vec![
                    ("body", false, text, text_bytes),
                    ("payload", true, json, json_bytes),
                ]
            }
            _ => unreachable!("unknown scenario {scenario}"),
        };

        let data_schema = Arc::new(DataSchema::new(
            columns
                .iter()
                .map(|(name, is_variant, ..)| {
                    let ty = if *is_variant {
                        DataType::Variant
                    } else {
                        DataType::String
                    };
                    DataField::new(name, ty)
                })
                .collect(),
        ));
        let source_schema = Arc::new(TableSchema::new(
            columns
                .iter()
                .map(|(name, is_variant, ..)| {
                    let ty = if *is_variant {
                        TableDataType::Variant
                    } else {
                        TableDataType::String
                    };
                    TableField::new(name, ty)
                })
                .collect(),
        ));
        let bytes = columns.iter().map(|(.., bytes)| bytes).sum();
        let num_rows = columns[0].2.len();
        let block = DataBlock::new(
            columns
                .into_iter()
                .map(|(_, _, column, _)| column)
                .collect(),
            num_rows,
        );

        Input {
            data_schema,
            source_schema,
            block,
            bytes,
            options: BTreeMap::from([("tokenizer".to_string(), "english".to_string())]),
            operator: Operator::new(Memory::default()).unwrap().finish(),
        }
    }

    fn writer(input: &Input) -> InvertedIndexWriter {
        init_globals();
        InvertedIndexWriter::try_create(
            input.data_schema.clone(),
            &input.options,
            input.operator.clone(),
            format!("bench/{}.index", uuid::Uuid::new_v4().simple()),
            None,
        )
        .unwrap()
    }

    /// Only `add_block()`: the code path changed by the column-backed document.
    #[divan::bench(args = ["short_text", "long_text", "variant", "mixed"], sample_count = 10)]
    fn add_block(bencher: divan::Bencher, scenario: &str) {
        let input = prepare(scenario);
        bencher
            .counter(BytesCount::new(input.bytes))
            .with_inputs(|| writer(&input))
            .bench_local_values(|mut writer| {
                writer
                    .add_block(&input.source_schema, &input.block)
                    .unwrap();
                // Dropped by divan outside the timed region.
                writer
            });
    }

    /// `add_block()` + `finalize()`: the end-to-end cost of building one block's index.
    #[divan::bench(args = ["short_text", "mixed"], sample_count = 10)]
    fn build_bundle(bencher: divan::Bencher, scenario: &str) {
        let input = prepare(scenario);
        bencher
            .counter(BytesCount::new(input.bytes))
            .with_inputs(|| writer(&input))
            .bench_local_values(|mut writer| {
                writer
                    .add_block(&input.source_schema, &input.block)
                    .unwrap();
                writer.finalize().unwrap()
            });
    }
}
