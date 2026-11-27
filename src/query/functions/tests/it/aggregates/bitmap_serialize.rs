// Copyright 2026 Datafuse Labs.
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

use std::collections::BTreeSet;
use std::ops::RangeInclusive;

use bumpalo::Bump;
use databend_common_column::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::StatesLayout;
use databend_common_expression::aggregate_function::AggregateCallRef;
use databend_common_expression::aggregate_function::AggregateStateOwner;
use databend_common_expression::aggregate_function::RawAggregateCall;
use databend_common_expression::aggregate_function::get_states_layout;
use databend_common_expression::types::BitmapType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::UInt64Type;
use databend_common_functions::aggregates::AGGR_REGISTRY;
use databend_common_io::HYBRID_KIND_LARGE;
use databend_common_io::HYBRID_KIND_SMALL;
use databend_common_io::HybridBitmap;

const BITMAP_AGGREGATES: [&str; 7] = [
    "bitmap_and_count",
    "bitmap_or_count",
    "bitmap_xor_count",
    "bitmap_not_count",
    "bitmap_intersect",
    "bitmap_union",
    "bitmap_xor_agg",
];

fn serialized_bitmap(values: &[u64]) -> Vec<u8> {
    let mut bitmap = HybridBitmap::new();
    for value in values {
        bitmap.insert(*value);
    }
    let mut data = Vec::new();
    bitmap.serialize_into(&mut data).unwrap();
    data
}

fn values_in_prefix(prefix: u64, range: RangeInclusive<u64>) -> Vec<u64> {
    range.map(|value| (prefix << 32) + value).collect()
}

struct BitmapOperand {
    name: &'static str,
    values: Vec<u64>,
    kind: u8,
}

fn bitmap_operands() -> Vec<BitmapOperand> {
    vec![
        BitmapOperand {
            name: "empty",
            values: vec![],
            kind: HYBRID_KIND_SMALL,
        },
        BitmapOperand {
            name: "small_32",
            values: values_in_prefix(0, 0..=31),
            kind: HYBRID_KIND_SMALL,
        },
        BitmapOperand {
            name: "multi_small_overlap",
            values: vec![31, (1u64 << 32) + 1],
            kind: HYBRID_KIND_SMALL,
        },
        BitmapOperand {
            name: "small_32_disjoint",
            values: values_in_prefix(0, 100..=131),
            kind: HYBRID_KIND_SMALL,
        },
        BitmapOperand {
            name: "large_33",
            values: values_in_prefix(0, 0..=32),
            kind: HYBRID_KIND_LARGE,
        },
        BitmapOperand {
            name: "large_array_4096",
            values: values_in_prefix(0, 0..=4095),
            kind: HYBRID_KIND_LARGE,
        },
        BitmapOperand {
            name: "large_bitmap_4097",
            values: values_in_prefix(0, 0..=4096),
            kind: HYBRID_KIND_LARGE,
        },
        BitmapOperand {
            name: "multi_large_mixed",
            values: values_in_prefix(0, 0..=4096)
                .into_iter()
                .chain(values_in_prefix(1, 0..=31))
                .collect(),
            kind: HYBRID_KIND_LARGE,
        },
        BitmapOperand {
            name: "multi_large_mixed_disjoint",
            values: values_in_prefix(2, 0..=4096)
                .into_iter()
                .chain(values_in_prefix(3, 0..=31))
                .collect(),
            kind: HYBRID_KIND_LARGE,
        },
    ]
}

fn bitmap_columns(lhs: &BitmapOperand, rhs: &BitmapOperand) -> [BlockEntry; 1] {
    let lhs_bitmap = serialized_bitmap(&lhs.values);
    let rhs_bitmap = serialized_bitmap(&rhs.values);
    let case_name = format!("{}_{}", lhs.name, rhs.name);
    assert_eq!(lhs_bitmap[3], lhs.kind, "{} lhs kind", case_name);
    assert_eq!(rhs_bitmap[3], rhs.kind, "{} rhs kind", case_name);
    [BitmapType::from_data(vec![lhs_bitmap, rhs_bitmap]).into()]
}

fn resolve(name: &str, params: &[Scalar], entries: &[BlockEntry]) -> Result<AggregateCallRef> {
    let args_type = entries
        .iter()
        .map(BlockEntry::data_type)
        .collect::<Vec<_>>();
    AGGR_REGISTRY.resolve(RawAggregateCall {
        name,
        params,
        args_type: &args_type,
        distinct: false,
        order_by: &[],
    })
}

fn eval_serialized_aggregate(
    name: &str,
    params: Vec<Scalar>,
    entries: &[BlockEntry],
    rows: usize,
    each_row: bool,
    with_serialize: bool,
) -> Result<(Column, DataType)> {
    let res1 = eval_serialized_aggregate_once(
        name,
        params.clone(),
        entries,
        rows,
        each_row,
        with_serialize,
    )?;
    let res2 = eval_serialized_aggregate_once(name, params, entries, rows, true, true)?;
    assert_eq!(res1, res2);
    Ok(res1)
}

fn eval_serialized_aggregate_once(
    name: &str,
    params: Vec<Scalar>,
    entries: &[BlockEntry],
    rows: usize,
    each_row: bool,
    with_serialize: bool,
) -> Result<(Column, DataType)> {
    let function = resolve(name, &params, entries)?;
    let data_type = function.signature().return_type.clone();
    let projected = function.input_layout().project(entries)?;
    let columns = projected.as_ref().into();
    let owner = AggregateStateOwner::new(vec![function.clone()])?;
    let state = owner.state(0);

    if each_row {
        for row in 0..rows {
            function.accumulate_row(state, columns, row)?;
        }
    } else {
        function.accumulate(state, columns)?;
    }

    if with_serialize {
        let state_type = function.state().data_type();
        let mut builder = ColumnBuilder::with_capacity(&state_type, 1);
        let builders = builder.as_tuple_mut().unwrap().as_mut_slice();
        function.serialize(owner.state_set(0), builders)?;
        function.init_state(state);
        let column = builder.build();
        function.merge_serialized(owner.state_set(0), &column.into())?;
    }
    let mut builder = ColumnBuilder::with_capacity(&data_type, 1);
    function.merge_result(state, &mut builder)?;
    Ok((builder.build(), data_type))
}

fn eval_merge_states(
    name: &str,
    entries: &[BlockEntry],
    rows: usize,
    split_at: usize,
) -> Result<(Column, DataType)> {
    let function = resolve(name, &[], entries)?;
    let data_type = function.signature().return_type.clone();
    let projected = function.input_layout().project(entries)?;
    let columns = projected.as_ref().into();
    let layout: StatesLayout = get_states_layout(std::slice::from_ref(&function))?;
    let loc = layout.states_loc[0].clone();
    let arena = Bump::new();
    let place = AggrState::new(arena.alloc_layout(layout.layout).into(), &loc);
    let rhs = AggrState::new(arena.alloc_layout(layout.layout).into(), &loc);
    function.init_state(place);
    function.init_state(rhs);
    for row in 0..split_at {
        function.accumulate_row(place, columns, row)?;
    }
    for row in split_at..rows {
        function.accumulate_row(rhs, columns, row)?;
    }
    function.merge_states(place, rhs)?;
    let mut builder = ColumnBuilder::with_capacity(&data_type, 1);
    function.merge_result(place, &mut builder)?;
    if function.state().need_manual_drop() {
        unsafe {
            function.drop_state(place);
            function.drop_state(rhs);
        }
    }
    Ok((builder.build(), data_type))
}

fn assert_malformed_with_options(
    validity: Option<Vec<bool>>,
    rows: Vec<Vec<u8>>,
    each_row: bool,
    with_serialize: bool,
) {
    let entry = match validity {
        Some(validity) => BitmapType::from_data_with_validity(rows, validity).into(),
        None => BitmapType::from_data(rows).into(),
    };
    let entries = [entry];
    for name in BITMAP_AGGREGATES {
        let error =
            eval_serialized_aggregate_once(name, vec![], &entries, 2, each_row, with_serialize)
                .unwrap_err();
        assert_eq!(error.code(), ErrorCode::BadBytes("").code(), "{name}");
    }
}

#[test]
fn test_fast_treemap_aggregate_matrix() -> Result<()> {
    let operands = bitmap_operands();
    let pairs = operands
        .iter()
        .flat_map(|lhs| operands.iter().map(move |rhs| (lhs, rhs)))
        .collect::<Vec<_>>();
    for (lhs, rhs) in pairs {
        let entries = bitmap_columns(lhs, rhs);
        let lhs_set = lhs.values.iter().copied().collect::<BTreeSet<_>>();
        let rhs_set = rhs.values.iter().copied().collect::<BTreeSet<_>>();
        let intersection = lhs_set.intersection(&rhs_set).copied().collect::<Vec<_>>();
        let union = lhs_set.union(&rhs_set).copied().collect::<Vec<_>>();
        let symmetric_difference = lhs_set
            .symmetric_difference(&rhs_set)
            .copied()
            .collect::<Vec<_>>();
        let difference = lhs_set.difference(&rhs_set).copied().collect::<Vec<_>>();
        for name in BITMAP_AGGREGATES {
            let (column, _) = eval_serialized_aggregate(name, vec![], &entries, 2, false, true)?;
            let expected = match name {
                "bitmap_and_count" => {
                    UInt64Type::from_data(vec![intersection.len() as u64]).wrap_nullable(None)
                }
                "bitmap_or_count" => {
                    UInt64Type::from_data(vec![union.len() as u64]).wrap_nullable(None)
                }
                "bitmap_xor_count" => {
                    UInt64Type::from_data(vec![symmetric_difference.len() as u64])
                        .wrap_nullable(None)
                }
                "bitmap_not_count" => {
                    UInt64Type::from_data(vec![difference.len() as u64]).wrap_nullable(None)
                }
                "bitmap_intersect" => BitmapType::from_data(vec![serialized_bitmap(&intersection)])
                    .wrap_nullable(None),
                "bitmap_union" => {
                    BitmapType::from_data(vec![serialized_bitmap(&union)]).wrap_nullable(None)
                }
                "bitmap_xor_agg" => {
                    BitmapType::from_data(vec![serialized_bitmap(&symmetric_difference)])
                        .wrap_nullable(None)
                }
                _ => unreachable!("{name}"),
            };
            assert_eq!(column, expected, "{name} {}_{}", lhs.name, rhs.name);
        }
    }
    Ok(())
}

#[test]
fn test_fast_treemap_empty_input() -> Result<()> {
    let entries = [BitmapType::from_data(Vec::<Vec<u8>>::new()).into()];
    for name in BITMAP_AGGREGATES {
        let (column, _) = eval_serialized_aggregate(name, vec![], &entries, 0, false, true)?;
        let validity = Some(Bitmap::new_constant(false, 1));
        let expected = match name {
            "bitmap_and_count" | "bitmap_or_count" | "bitmap_xor_count" | "bitmap_not_count" => {
                UInt64Type::from_data(vec![0u64]).wrap_nullable(validity)
            }
            "bitmap_intersect" | "bitmap_union" | "bitmap_xor_agg" => {
                BitmapType::from_data(vec![Vec::new()]).wrap_nullable(validity)
            }
            _ => unreachable!("{name}"),
        };
        assert_eq!(column, expected, "{name}");
    }
    Ok(())
}

#[test]
fn test_fast_treemap_merge_states_equivalence() -> Result<()> {
    let operands = bitmap_operands();
    let pairs = operands
        .iter()
        .flat_map(|lhs| operands.iter().map(move |rhs| (lhs, rhs)))
        .collect::<Vec<_>>();
    for (lhs, rhs) in pairs {
        let entries = bitmap_columns(lhs, rhs);
        for name in BITMAP_AGGREGATES {
            let standard = eval_serialized_aggregate(name, vec![], &entries, 2, false, true)?;
            let merged = eval_merge_states(name, &entries, 2, 1)?;
            assert_eq!(standard, merged, "{name} {}_{}", lhs.name, rhs.name);
        }
    }
    Ok(())
}

#[test]
fn test_fast_treemap_null_then_malformed() {
    let validity = Some(vec![false, true]);
    let rows = vec![vec![], vec![0xff]];
    assert_malformed_with_options(validity, rows, false, true);
}

#[test]
fn test_fast_treemap_valid_then_malformed() {
    let large = serialized_bitmap(
        &values_in_prefix(0, 0..=32)
            .into_iter()
            .chain(values_in_prefix(1, 0..=32))
            .collect::<Vec<_>>(),
    );
    assert_malformed_with_options(None, vec![large, vec![0xff]], true, false);
}
