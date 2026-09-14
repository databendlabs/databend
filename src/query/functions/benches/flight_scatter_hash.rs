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

use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnRef;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::Value;
use databend_common_expression::group_hash_entry;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_functions::BUILTIN_FUNCTIONS;

const ROWS: usize = 1 << 16;
const PARTITIONS: usize = 8;

fn main() {
    divan::main();
}

fn column_expr(id: usize, data_type: DataType) -> Expr {
    Expr::ColumnRef(ColumnRef {
        span: None,
        id,
        data_type,
        display_name: format!("#{id}"),
    })
}

struct BenchmarkInput {
    block: DataBlock,
    keys: Vec<Expr>,
    legacy_hash_keys: Vec<Expr>,
}

fn benchmark_input(key_count: usize) -> BenchmarkInput {
    let text_keys = (0..ROWS)
        .map(|row| format!("key-{row:016x}"))
        .collect::<Vec<_>>();
    let tag_keys = (0..ROWS)
        .map(|row| format!("tag-{:08x}", row.wrapping_mul(17)))
        .collect::<Vec<_>>();
    let key_num_1 = (0..ROWS).map(|row| row as u64).collect::<Vec<_>>();
    let key_num_2 = (0..ROWS)
        .map(|row| row.wrapping_mul(31) as u64)
        .collect::<Vec<_>>();
    let key_num_3 = (0..ROWS).map(|row| (row % 2048) as u64).collect::<Vec<_>>();
    let block = DataBlock::new_from_columns(vec![
        StringType::from_data(text_keys),
        StringType::from_data(tag_keys),
        UInt64Type::from_data(key_num_1),
        UInt64Type::from_data(key_num_2),
        UInt64Type::from_data(key_num_3),
    ]);

    let text_key = column_expr(0, DataType::String);
    let tag_key = column_expr(1, DataType::String);
    let mut keys = vec![
        check_function(
            None,
            "lower",
            &[],
            std::slice::from_ref(&text_key),
            &BUILTIN_FUNCTIONS,
        )
        .unwrap(),
        check_function(
            None,
            "concat",
            &[],
            &[text_key, tag_key],
            &BUILTIN_FUNCTIONS,
        )
        .unwrap(),
        column_expr(2, DataType::Number(NumberDataType::UInt64)),
        column_expr(3, DataType::Number(NumberDataType::UInt64)),
        column_expr(4, DataType::Number(NumberDataType::UInt64)),
    ];
    keys.truncate(key_count);
    let legacy_hash_keys = keys
        .iter()
        .map(|key| {
            check_function(
                None,
                "siphash",
                &[],
                std::slice::from_ref(key),
                &BUILTIN_FUNCTIONS,
            )
            .unwrap()
        })
        .collect();
    BenchmarkInput {
        block,
        keys,
        legacy_hash_keys,
    }
}

fn legacy_scatter_indices(input: &BenchmarkInput) -> Vec<u64> {
    let func_ctx = FunctionContext::default();
    let evaluator = Evaluator::new(&input.block, &func_ctx, &BUILTIN_FUNCTIONS);
    let hash_keys = input
        .legacy_hash_keys
        .iter()
        .map(|expr| match evaluator.run(expr).unwrap() {
            Value::<AnyType>::Column(column) => {
                NumberType::<u64>::try_downcast_column(&column).unwrap()
            }
            Value::<AnyType>::Scalar(_) => unreachable!(),
        })
        .collect::<Vec<_>>();

    let mut hashes = vec![DefaultHasher::default(); input.block.num_rows()];
    for key in hash_keys {
        for (hash, value) in hashes.iter_mut().zip(key.iter()) {
            hash.write_u64(*value);
        }
    }
    hashes
        .into_iter()
        .map(|hash| hash.finish() % PARTITIONS as u64)
        .collect()
}

fn group_hash_scatter_indices(input: &BenchmarkInput) -> Vec<u64> {
    let func_ctx = FunctionContext::default();
    let evaluator = Evaluator::new(&input.block, &func_ctx, &BUILTIN_FUNCTIONS);
    let num_rows = input.block.num_rows();
    let mut hashes = vec![0; num_rows];

    for (index, expr) in input.keys.iter().enumerate() {
        let entry = BlockEntry::new(evaluator.run(expr).unwrap(), || {
            (expr.data_type().clone(), num_rows)
        });
        group_hash_entry(&entry, &mut hashes, index == 0);
    }

    for hash in &mut hashes {
        *hash %= PARTITIONS as u64;
    }
    hashes
}

#[divan::bench_group(max_time = 2)]
mod multi_key_scatter {
    use super::*;

    #[divan::bench(args = [2, 5])]
    fn legacy(bencher: divan::Bencher, key_count: usize) {
        let input = benchmark_input(key_count);
        assert_eq!(legacy_scatter_indices(&input).len(), ROWS);
        bencher.bench(|| {
            divan::black_box(legacy_scatter_indices(divan::black_box(&input)));
        });
    }

    #[divan::bench(args = [2, 5])]
    fn group_hash(bencher: divan::Bencher, key_count: usize) {
        let input = benchmark_input(key_count);
        assert_eq!(group_hash_scatter_indices(&input).len(), ROWS);
        bencher.bench(|| {
            divan::black_box(group_hash_scatter_indices(divan::black_box(&input)));
        });
    }
}
