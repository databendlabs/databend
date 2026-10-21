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

//! Group by on encoded columns gives the same result as on the decoded columns.

use std::sync::Arc;

use bumpalo::Bump;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::HashTableConfig;
use databend_common_expression::PayloadFlushState;
use databend_common_expression::ProbeState;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::aggregate_function::AggregateCallRef;
use databend_common_expression::aggregate_function::RawAggregateCall;
use databend_common_expression::block_debug::assert_block_value_sort_eq;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::DecimalColumn;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::DictColumn;
use databend_common_expression::types::EncodedColumn;
use databend_common_expression::types::ForColumn;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::StringType;
use databend_common_functions::aggregates::AGGR_REGISTRY;
use itertools::Itertools;

fn resolve_agg(name: &str, arg_type: DataType) -> AggregateCallRef {
    AGGR_REGISTRY
        .resolve(RawAggregateCall {
            name,
            params: &[],
            args_type: &[arg_type],
            distinct: false,
            order_by: &[],
        })
        .unwrap()
}

/// Plain group columns: String, Int64, Decimal(15, 2) and Date, with 5 * 3 * 2 * 7 groups.
/// The two decimal values are `decimal_step` apart, which sets the number of frame of reference
/// codes of the encoded decimal column.
fn plain_group_columns(n: usize, decimal_step: i64) -> Vec<Column> {
    let size = DecimalSize::new_unchecked(15, 2);
    vec![
        StringType::from_data((0..n).map(|x| format!("flag-{}", x % 5)).collect_vec()),
        Int64Type::from_data((0..n).map(|x| 1_000_000 + (x % 3) as i64).collect_vec()),
        Column::Decimal(DecimalColumn::Decimal64(
            (0..n)
                .map(|x| 9_000_00 + (x % 2) as i64 * decimal_step)
                .collect_vec()
                .into(),
            size,
        )),
        DateType::from_data((0..n).map(|x| 8_000 + (x % 7) as i32).collect_vec()),
    ]
}

/// The same columns encoded: dictionary for strings, frame of reference for the others.
fn encode(columns: &[Column]) -> Vec<Column> {
    columns
        .iter()
        .map(|column| {
            let encoded = match column {
                Column::String(_) => {
                    EncodedColumn::Dict(DictColumn::try_encode(column, 1024).unwrap())
                }
                _ => EncodedColumn::For(ForColumn::try_encode(column).unwrap()),
            };
            Column::Encoded(Box::new(encoded))
        })
        .collect()
}

/// Groups `batches` (group columns, aggregate argument) into one hash table with `count` and
/// `sum`, and returns the group columns followed by the aggregate results.
fn aggregate(batches: &[(Vec<Column>, Column)]) -> DataBlock {
    let group_types = batches[0].0.iter().map(|c| c.data_type()).collect_vec();
    let aggrs = vec![
        resolve_agg("count", Int64Type::data_type()),
        resolve_agg("sum", Int64Type::data_type()),
    ];
    let mut hashtable = AggregateHashTable::new(
        group_types,
        aggrs.clone(),
        HashTableConfig::default(),
        Arc::new(Bump::new()),
    );

    let mut state = ProbeState::default();
    for (group_columns, arg) in batches {
        let group_entries = group_columns
            .iter()
            .cloned()
            .map(BlockEntry::from)
            .collect_vec();
        let params = aggrs
            .iter()
            .map(|_| vec![BlockEntry::from(arg.clone())])
            .collect_vec();
        let params: Vec<ProjectedBlock> = params.iter().map(|v| v.into()).collect_vec();
        hashtable
            .add_groups(
                &mut state,
                (&group_entries).into(),
                &params,
                (&[]).into(),
                arg.len(),
            )
            .unwrap();
    }

    let mut merge_state = PayloadFlushState::default();
    let mut blocks = Vec::new();
    while hashtable.merge_result(&mut merge_state).unwrap() {
        let mut entries = merge_state.take_group_columns();
        entries.extend(merge_state.take_aggregate_results());
        let num_rows = entries[0].len();
        blocks.push(DataBlock::new(entries, num_rows));
    }
    DataBlock::concat(&blocks).unwrap()
}

// cargo test --package databend-common-functions --test it -- aggregates::agg_encoded --nocapture
fn check_group_by_encoded_columns(decimal_step: i64) {
    let n = 1000;
    let plain = plain_group_columns(n, decimal_step);
    let encoded = encode(&plain);
    let arg = Int64Type::from_data((0..n as i64).collect_vec());

    // Two batches, so the second one is matched against the rows of the first one.
    let expected = aggregate(&[(plain.clone(), arg.clone()), (plain.clone(), arg.clone())]);
    assert_eq!(expected.num_rows(), 5 * 3 * 2 * 7);

    let cases = [
        ("encoded, encoded", [&encoded, &encoded]),
        ("plain, encoded", [&plain, &encoded]),
        ("encoded, plain", [&encoded, &plain]),
    ];
    for (name, [first, second]) in cases {
        let block = aggregate(&[(first.clone(), arg.clone()), (second.clone(), arg.clone())]);
        // Group keys come back as regular columns of the group types.
        for (entry, column) in block.columns().iter().zip(plain.iter()) {
            assert_eq!(entry.data_type(), column.data_type(), "{name}");
            assert!(
                !matches!(entry, BlockEntry::Column(Column::Encoded(_))),
                "{name}"
            );
        }
        assert_block_value_sort_eq(&block, &expected);
    }
}

/// 5 * 3 * 38 * 7 code combinations for 1000 rows: probes every row.
#[test]
fn test_group_by_encoded_columns() {
    check_group_by_encoded_columns(37);
}

/// 5 * 3 * 2 * 7 code combinations for 1000 rows: probes once per combination.
#[test]
fn test_group_by_encoded_codes() {
    check_group_by_encoded_columns(1);
}
