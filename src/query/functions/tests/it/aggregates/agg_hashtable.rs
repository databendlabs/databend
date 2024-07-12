// Copyright 2022 Datafuse Labs.
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

use std::sync::Arc;

use bumpalo::Bump;
use databend_common_expression::block_debug::assert_block_value_sort_eq;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::Float32Type;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::Int16Type;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::Int8Type;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::F32;
use databend_common_expression::types::F64;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::HashTableConfig;
use databend_common_expression::PayloadFlushState;
use databend_common_expression::ProbeState;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use itertools::Itertools;

// cargo test --package databend-common-functions --test it -- aggregates::agg_hashtable::test_agg_hashtable --exact --nocapture
#[test]
fn test_agg_hashtable() {
    let factory = AggregateFunctionFactory::instance();
    let m: usize = 4;
    for n in [100, 1000, 10_000, 100_000] {
        let columns = vec![
            StringType::from_data((0..n).map(|x| format!("{}", x % m)).collect_vec()),
            Int64Type::from_data((0..n).map(|x| (x % m) as i64).collect_vec()),
            Int32Type::from_data((0..n).map(|x| (x % m) as i32).collect_vec()),
            Int16Type::from_data((0..n).map(|x| (x % m) as i16).collect_vec()),
            Int8Type::from_data((0..n).map(|x| (x % m) as i8).collect_vec()),
            Float32Type::from_data((0..n).map(|x| F32::from((x % m) as f32)).collect_vec()),
            Float64Type::from_data((0..n).map(|x| F64::from((x % m) as f64)).collect_vec()),
            BooleanType::from_data((0..n).map(|x| (x % m) != 0).collect_vec()),
        ];

        let group_columns = columns.clone();
        let group_types: Vec<_> = group_columns.iter().map(|c| c.data_type()).collect();

        let aggrs = vec![
            factory
                .get("min", vec![], vec![Int64Type::data_type()])
                .unwrap(),
            factory
                .get("max", vec![], vec![Int64Type::data_type()])
                .unwrap(),
            factory
                .get("sum", vec![], vec![Int64Type::data_type()])
                .unwrap(),
            factory
                .get("count", vec![], vec![Int64Type::data_type()])
                .unwrap(),
        ];

        let params: Vec<Vec<Column>> = aggrs.iter().map(|_| vec![columns[1].clone()]).collect();
        let params = params.iter().map(|v| v.into()).collect_vec();

        let config = HashTableConfig::default();
        let mut hashtable = AggregateHashTable::new(
            group_types.clone(),
            aggrs.clone(),
            config.clone(),
            Arc::new(Bump::new()),
        );

        let mut state = ProbeState::default();
        let _ = hashtable
            .add_groups(
                &mut state,
                (&group_columns).into(),
                &params,
                (&[]).into(),
                n,
            )
            .unwrap();

        let mut hashtable2 = AggregateHashTable::new(
            group_types.clone(),
            aggrs.clone(),
            config.clone(),
            Arc::new(Bump::new()),
        );

        let mut state2 = ProbeState::default();
        let _ = hashtable2
            .add_groups(
                &mut state2,
                (&group_columns).into(),
                &params,
                (&[]).into(),
                n,
            )
            .unwrap();

        let mut flush_state = PayloadFlushState::default();
        let _ = hashtable.combine(hashtable2, &mut flush_state);

        let mut merge_state = PayloadFlushState::default();

        let mut blocks = Vec::new();
        loop {
            match hashtable.merge_result(&mut merge_state) {
                Ok(true) => {
                    let mut columns = merge_state.take_group_columns();
                    columns.extend_from_slice(&merge_state.take_aggregate_results());

                    let block = DataBlock::new_from_columns(columns);
                    blocks.push(block);
                }
                Ok(false) => break,
                Err(err) => panic!("{}", err),
            }
        }
        let block = DataBlock::concat(&blocks).unwrap();

        assert_eq!(block.num_columns(), group_columns.len() + aggrs.len());
        assert_eq!(block.num_rows(), m);

        let validities = vec![true, true, true, true];

        let rows = n as i64;
        let urows = rows as u64;

        let mut expected_results: Vec<Column> =
            group_columns.iter().map(|c| c.slice(0..m)).collect();

        expected_results.extend_from_slice(&[
            Int64Type::from_data_with_validity(vec![0, 1, 2, 3], validities.clone()),
            Int64Type::from_data_with_validity(vec![0, 1, 2, 3], validities.clone()),
            Int64Type::from_data_with_validity(
                vec![0, rows / 2, rows, rows / 2 * 3],
                validities.clone(),
            ),
            UInt64Type::from_data(vec![urows / 2, urows / 2, urows / 2, urows / 2]),
        ]);

        let block_expected = DataBlock::new_from_columns(expected_results.clone());

        assert_block_value_sort_eq(&block, &block_expected);
    }
}
