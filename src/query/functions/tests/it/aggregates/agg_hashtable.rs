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

use std::alloc::Layout;
use std::sync::Arc;

use bumpalo::Bump;
use databend_common_expression::AggrStateType;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::HashTableConfig;
use databend_common_expression::PayloadFlushState;
use databend_common_expression::ProbeState;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::aggregate_function::AggregateCallRef;
use databend_common_expression::aggregate_function::RawAggregateCall;
use databend_common_expression::aggregate_function::get_states_layout;
use databend_common_expression::block_debug::assert_block_value_eq;
use databend_common_expression::block_debug::assert_block_value_sort_eq;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::F32;
use databend_common_expression::types::F64;
use databend_common_expression::types::Float32Type;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::Int8Type;
use databend_common_expression::types::Int16Type;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
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

// cargo test --package databend-common-functions --test it -- aggregates::agg_hashtable::test_agg_hashtable --exact --nocapture
#[test]
fn test_agg_hashtable() {
    let m: usize = 4;
    for n in [100, 1000, 10_000, 100_000] {
        let columns = [
            StringType::from_data((0..n).map(|x| format!("{}", x % m)).collect_vec()).into(),
            Int64Type::from_data((0..n).map(|x| (x % m) as i64).collect_vec()).into(),
            Int32Type::from_data((0..n).map(|x| (x % m) as i32).collect_vec()).into(),
            Int16Type::from_data((0..n).map(|x| (x % m) as i16).collect_vec()).into(),
            Int8Type::from_data((0..n).map(|x| (x % m) as i8).collect_vec()).into(),
            Float32Type::from_data((0..n).map(|x| F32::from((x % m) as f32)).collect_vec()).into(),
            Float64Type::from_data((0..n).map(|x| F64::from((x % m) as f64)).collect_vec()).into(),
            BooleanType::from_data((0..n).map(|x| (x % m) != 0).collect_vec()).into(),
            BlockEntry::new_const_column_arg::<StringType>("a".to_string(), n),
            BlockEntry::new_const_column_arg::<NullableType<StringType>>(None, n),
            BlockEntry::new_const_column_arg::<NullableType<Int8Type>>(Some(4), n),
        ];

        let group_columns = columns.to_vec();
        let group_types = columns.iter().map(|c| c.data_type()).collect::<Vec<_>>();

        let aggrs = vec![
            resolve_agg("min", Int64Type::data_type()),
            resolve_agg("max", Int64Type::data_type()),
            resolve_agg("sum", Int64Type::data_type()),
            resolve_agg("count", Int64Type::data_type()),
        ];

        let params = aggrs.iter().map(|_| vec![columns[1].clone()]).collect_vec();
        let params: Vec<ProjectedBlock> = params.iter().map(|v| v.into()).collect_vec();

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

        let _ = hashtable2
            .add_groups(
                &mut state,
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
                    let mut entries = merge_state.take_group_columns();
                    let agg_results = merge_state.take_aggregate_results();
                    entries.extend(agg_results);

                    let num_rows = entries[0].len();
                    blocks.push(DataBlock::new(entries, num_rows));
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

        let mut expected_results = group_columns
            .iter()
            .map(|c| c.slice(0..m))
            .collect::<Vec<_>>();

        expected_results.extend_from_slice(&[
            Int64Type::from_data_with_validity(vec![0, 1, 2, 3], validities.clone()).into(),
            Int64Type::from_data_with_validity(vec![0, 1, 2, 3], validities.clone()).into(),
            Int64Type::from_data_with_validity(
                vec![0, rows / 2, rows, rows / 2 * 3],
                validities.clone(),
            )
            .into(),
            UInt64Type::from_data(vec![urows / 2, urows / 2, urows / 2, urows / 2]).into(),
        ]);

        let block_expected = DataBlock::new(expected_results.clone(), expected_results[0].len());
        assert_block_value_sort_eq(&block, &block_expected);
    }
}

#[test]
fn test_layout() {
    let decimal_type = DataType::Decimal(DecimalSize::new_unchecked(20, 2));
    let aggrs = resolve_agg("sum", decimal_type);

    let states_layout = get_states_layout(std::slice::from_ref(&aggrs)).unwrap();

    assert_eq!(
        states_layout.layout,
        Layout::from_size_align(17, 8).unwrap()
    );
    let [AggrStateType::Custom(state_layout), AggrStateType::Bool] = aggrs.state().fields() else {
        panic!("unexpected state description: {:?}", aggrs.state().fields());
    };
    assert_eq!(*state_layout, Layout::from_size_align(16, 8).unwrap());
}

fn aggregate_into_block(
    group_columns: &[BlockEntry],
    params: &[Vec<BlockEntry>],
    aggrs: &[AggregateCallRef],
    num_rows: usize,
) -> DataBlock {
    let group_types = group_columns.iter().map(|c| c.data_type()).collect_vec();
    let params: Vec<ProjectedBlock> = params.iter().map(|v| v.into()).collect_vec();
    let mut hashtable = AggregateHashTable::new(
        group_types,
        aggrs.to_vec(),
        HashTableConfig::default(),
        Arc::new(Bump::new()),
    );
    let mut state = ProbeState::default();
    hashtable
        .add_groups(
            &mut state,
            group_columns.into(),
            &params,
            (&[]).into(),
            num_rows,
        )
        .unwrap();

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

#[test]
fn test_agg_hashtable_dictionary_across_batches() {
    use databend_common_column::bitmap::Bitmap;
    use databend_common_expression::Column;
    use databend_common_expression::types::DictColumn;
    use databend_common_expression::types::EncodedColumn;
    use databend_common_expression::types::ForColumn;
    use databend_common_expression::types::NarrowBuffer;

    let n = 8193;
    let args = vec![vec![
        Int64Type::from_data((0..n as i64).collect_vec()).into(),
    ]];
    let aggrs = vec![resolve_agg("sum", Int64Type::data_type())];
    // Exercise block-scoped code reuse, multi-key combinations above the old 512-code
    // batch limit, and the fallback for dictionaries too large for the whole block.
    for ndv in [4, 1021, 3001] {
        let mut values = (0..=ndv).map(|i| format!("key{}", i % ndv)).collect_vec();
        values.push("unused".to_string());
        let keys = NarrowBuffer::pack((0..n).map(|i| (i % (ndv + 1)) as u64), (ndv + 1) as u64);
        let encoded = Column::Encoded(Box::new(EncodedColumn::Dict(DictColumn::new(
            keys,
            StringType::from_data(values),
        ))));
        for nullable in [false, true] {
            let column = if nullable {
                encoded
                    .clone()
                    .wrap_nullable(Some(Bitmap::from_iter((0..n).map(|i| i % 5 != 0))))
            } else {
                encoded.clone()
            };
            let plain = column.clone().decode_owned();
            let number = Int64Type::from_data((0..n).map(|i| (i % 2) as i64).collect_vec());
            for second in [
                None,
                Some(number.clone()),
                Some(Column::Encoded(Box::new(EncodedColumn::For(
                    ForColumn::try_encode(&number).unwrap(),
                )))),
            ] {
                let mut encoded_keys = vec![column.clone().into()];
                let mut plain_keys = vec![plain.clone().into()];
                if let Some(second) = second {
                    encoded_keys.push(second.clone().into());
                    plain_keys.push(second.decode_owned().into());
                }
                assert_block_value_sort_eq(
                    &aggregate_into_block(&encoded_keys, &args, &aggrs, n),
                    &aggregate_into_block(&plain_keys, &args, &aggrs, n),
                );
            }
        }
    }
}

#[test]
fn test_agg_hashtable_shared_dictionary_keys() {
    use databend_common_expression::Column;
    use databend_common_expression::types::DictColumn;
    use databend_common_expression::types::EncodedColumn;
    use databend_common_expression::types::ForColumn;
    use databend_common_expression::types::NarrowBuffer;

    let n = 8193;
    let args = vec![vec![
        Int64Type::from_data((0..n as i64).collect_vec()).into(),
    ]];
    let aggrs = vec![resolve_agg("sum", Int64Type::data_type())];
    let dict = |keys, values| {
        Column::Encoded(Box::new(EncodedColumn::Dict(DictColumn::new(keys, values))))
    };
    for ndv in [64, 1021] {
        let all_keys =
            NarrowBuffer::pack((0..n + 1).map(|i| (i / 2 % ndv) as u64), (ndv - 1) as u64);
        let keys = all_keys.slice(0..n);
        let values = |delta| Int64Type::from_data((0..ndv).map(|i| i as i64 - delta).collect_vec());
        let raw = dict(keys.clone(), values(0));
        let mod2 = dict(
            keys.clone(),
            Int64Type::from_data((0..ndv).map(|i| (i % 2) as i64).collect_vec()),
        );
        let mod3 = dict(
            keys.clone(),
            Int64Type::from_data((0..ndv).map(|i| (i % 3) as i64).collect_vec()),
        );
        let independent = Column::Encoded(Box::new(EncodedColumn::For(
            ForColumn::try_encode(&Int64Type::from_data(
                (0..n).map(|i| (i % 2) as i64).collect_vec(),
            ))
            .unwrap(),
        )));
        for columns in [
            vec![
                raw.clone(),
                dict(keys.clone(), values(1)),
                dict(keys.clone(), values(2)),
                dict(keys.clone(), values(3)),
            ],
            vec![mod2.clone(), mod3.clone()],
            vec![mod2, mod3, independent],
            // Equal keys in independent storage are not proof of shared identity.
            vec![
                raw.clone(),
                dict(NarrowBuffer::pack(keys.iter(), (ndv - 1) as u64), values(1)),
            ],
            // The same allocation at a different offset is a different code dimension.
            vec![raw, dict(all_keys.slice(1..n + 1), values(0))],
        ] {
            let encoded = columns.iter().cloned().map(BlockEntry::from).collect_vec();
            let plain = columns
                .into_iter()
                .map(|c| BlockEntry::from(c.decode_owned()))
                .collect_vec();
            assert_block_value_sort_eq(
                &aggregate_into_block(&encoded, &args, &aggrs, n),
                &aggregate_into_block(&plain, &args, &aggrs, n),
            );
        }
    }
}

/// Group keys that are dictionary encoded (and arguments that are frame of reference encoded)
/// must aggregate exactly like their decoded columns.
#[test]
fn test_agg_hashtable_encoded_columns() {
    use databend_common_expression::Column;
    use databend_common_expression::types::DecimalColumn;
    use databend_common_expression::types::DictColumn;
    use databend_common_expression::types::EncodedColumn;
    use databend_common_expression::types::ForColumn;
    use databend_common_expression::types::NarrowBuffer;

    fn dict(values: &[&str], keys: &[u32]) -> (BlockEntry, BlockEntry) {
        let plain = StringType::from_data(keys.iter().map(|k| values[*k as usize]).collect_vec());
        let keys = NarrowBuffer::pack(keys.iter().map(|k| *k as u64), values.len() as u64 - 1);
        let encoded = Column::Encoded(Box::new(EncodedColumn::Dict(DictColumn::new(
            keys,
            StringType::from_data(values.to_vec()),
        ))));
        (encoded.into(), plain.into())
    }

    let size = DecimalSize::new_unchecked(15, 2);
    // Declared i128, values within i64 (like the result of a decimal multiplication).
    let wide_size = DecimalSize::new_unchecked(38, 6);
    // 10 rows takes the decoding fallback (too few rows per key combination).
    for n in [10usize, 2048, 5000] {
        let (flag_a, plain_a) = dict(
            &["A", "N", "R", "unused"],
            &(0..n).map(|i| (i * 7 % 3) as u32).collect_vec(),
        );
        let (flag_b, plain_b) = dict(&["F", "O"], &(0..n).map(|i| (i % 2) as u32).collect_vec());
        let values = (0..n as i64).map(|i| 100_000 + i * 13 % 5000).collect_vec();
        let plain_price = Column::Decimal(DecimalColumn::Decimal64(values.clone().into(), size));
        let price: BlockEntry = Column::Encoded(Box::new(EncodedColumn::For(
            ForColumn::try_encode(&plain_price).unwrap(),
        )))
        .into();
        let plain_price: BlockEntry = plain_price.into();
        let wide_values = values.iter().map(|v| *v as i128 * 1_000_000).collect_vec();
        let plain_wide: BlockEntry = Column::Decimal(DecimalColumn::Decimal128(
            wide_values.clone().into(),
            wide_size,
        ))
        .into();
        let min = *wide_values.iter().min().unwrap();
        let max = *wide_values.iter().max().unwrap();
        let deltas = NarrowBuffer::pack(
            wide_values.iter().map(|v| (v - min) as u64),
            (max - min) as u64,
        );
        let wide: BlockEntry = Column::Encoded(Box::new(EncodedColumn::For(ForColumn::new(
            DataType::Decimal(wide_size),
            min,
            (max - min) as u64,
            deltas,
        ))))
        .into();
        let number: BlockEntry =
            Int64Type::from_data((0..n as i64).map(|i| i % 4).collect_vec()).into();
        // numeric dictionary keys
        let codes = Int64Type::from_data((0..n).map(|i| [5i64, -1, 1 << 40][i % 3]).collect_vec());
        let code: BlockEntry = Column::Encoded(Box::new(EncodedColumn::Dict(
            DictColumn::try_encode(&codes, 16).unwrap(),
        )))
        .into();
        let plain_code: BlockEntry = codes.into();

        let aggrs = vec![
            resolve_agg("sum", DataType::Decimal(size)),
            resolve_agg("avg", DataType::Decimal(size)),
            resolve_agg("min", DataType::Decimal(size)),
            resolve_agg("count", DataType::Decimal(size)),
            resolve_agg("sum", DataType::Decimal(wide_size)),
            resolve_agg("avg", DataType::Decimal(wide_size)),
        ];
        let encoded_args = [&price, &price, &price, &price, &wide, &wide];
        let plain_args = [
            &plain_price,
            &plain_price,
            &plain_price,
            &plain_price,
            &plain_wide,
            &plain_wide,
        ];

        for (encoded_keys, plain_keys) in [
            (vec![flag_a.clone(), flag_b.clone()], vec![
                plain_a.clone(),
                plain_b.clone(),
            ]),
            (vec![code.clone(), flag_b.clone()], vec![
                plain_code.clone(),
                plain_b.clone(),
            ]),
            // mixed keys take the decoding fallback
            (vec![flag_a.clone(), number.clone()], vec![
                plain_a.clone(),
                number.clone(),
            ]),
        ] {
            let encoded_params = encoded_args
                .iter()
                .map(|arg| vec![(*arg).clone()])
                .collect_vec();
            let plain_params = plain_args
                .iter()
                .map(|arg| vec![(*arg).clone()])
                .collect_vec();
            let encoded = aggregate_into_block(&encoded_keys, &encoded_params, &aggrs, n);
            let plain = aggregate_into_block(&plain_keys, &plain_params, &aggrs, n);
            assert_block_value_sort_eq(&encoded, &plain);
        }
    }
}

// Flushing a payload with a reused `PayloadFlushState` must produce the same
// result as flushing it with a fresh state, including for empty payloads.
#[test]
fn test_payload_flush_all_with_reused_state() {
    let aggrs = vec![
        resolve_agg("sum", Int64Type::data_type()),
        resolve_agg("count", Int64Type::data_type()),
    ];

    // (distinct groups, radix bits): the first case has payloads larger than
    // one flush batch, the second one has mostly empty payloads.
    for (m, radix_bits) in [(5000_usize, 1_u64), (2, 4)] {
        let n = m * 2;
        let group_columns: Vec<BlockEntry> =
            vec![Int64Type::from_data((0..n).map(|x| (x % m) as i64).collect_vec()).into()];
        let group_types = group_columns
            .iter()
            .map(|c| c.data_type())
            .collect::<Vec<_>>();
        let args = vec![group_columns[0].clone()];
        let params: Vec<ProjectedBlock> = aggrs.iter().map(|_| (&args).into()).collect_vec();

        let config = HashTableConfig::default().with_initial_radix_bits(radix_bits);
        let mut hashtable = AggregateHashTable::new(
            group_types.clone(),
            aggrs.clone(),
            config,
            Arc::new(Bump::new()),
        );

        let mut state = ProbeState::default();
        hashtable
            .add_groups(
                &mut state,
                (&group_columns).into(),
                &params,
                (&[]).into(),
                n,
            )
            .unwrap();

        assert_eq!(hashtable.payload.partition_count(), 1 << radix_bits);

        let mut shared_state = PayloadFlushState::default();
        let mut total_rows = 0;
        let mut empty_payloads = 0;
        for (_, payload) in hashtable.payload.into_bucket_payloads() {
            let expected = payload.aggregate_flush_all().unwrap();
            let actual = payload
                .aggregate_flush_all_with_state(&mut shared_state)
                .unwrap();

            assert_eq!(expected.num_rows(), payload.len());
            assert_eq!(actual.num_rows(), payload.len());
            assert_eq!(expected.num_columns(), group_types.len() + aggrs.len());
            assert_eq!(actual.num_columns(), expected.num_columns());
            assert_block_value_eq(&expected, &actual);

            let group_by = payload.group_by_flush_all().unwrap();
            assert_eq!(group_by.num_rows(), payload.len());

            if payload.len() == 0 {
                empty_payloads += 1;
            }
            total_rows += actual.num_rows();
        }

        assert_eq!(total_rows, m);
        if m < (1 << radix_bits) {
            assert!(empty_payloads > 0);
        }
    }
}
