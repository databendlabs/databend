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

use std::io::Write;

use bumpalo::Bump;
use comfy_table::Table;
use databend_common_base::runtime::drop_guard;
use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::FunctionContext;
use databend_common_expression::RawExpr;
use databend_common_expression::Scalar;
use databend_common_expression::StateAddr;
use databend_common_expression::StatesLayout;
use databend_common_expression::Value;
use databend_common_expression::aggregate::aggregate_function as v2;
use databend_common_expression::aggregate::aggregate_function::AggregateFunctionRequest;
use databend_common_expression::type_check;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_functions::aggregates::AGGR_REGISTRY;
use databend_common_functions::aggregates::AggregateFunctionSortDesc;
use databend_common_functions::aggregates::sort_descs_to_bound_order_by;
use itertools::Itertools;

use super::super::scalars::parser;
use super::aggregate_function_v2_support::assert_two_groups_group_by_v2_matches_legacy_result;

pub(super) trait AggregationSimulator = Fn(
        &str,
        Vec<Scalar>,
        &[BlockEntry],
        usize,
        Vec<AggregateFunctionSortDesc>,
    ) -> databend_common_exception::Result<(Column, DataType)>
    + Copy;

/// Run an aggregate expression against the given simulator and write its golden output.
pub(super) fn write_aggregate_expr_case(
    file: &mut impl Write,
    text: &str,
    entries: &[(&str, BlockEntry)],
    simulator: impl AggregationSimulator,
    sort_descs: Vec<AggregateFunctionSortDesc>,
) {
    let raw_expr = parser::parse_raw_expr(
        text,
        &entries
            .iter()
            .map(|(name, entry)| (*name, entry.data_type()))
            .collect::<Vec<_>>(),
        &BUILTIN_FUNCTIONS,
    );

    let num_rows = entries
        .iter()
        .map(|(_, entry)| entry.len())
        .max()
        .unwrap_or(0);
    let block = DataBlock::new(
        entries.iter().map(|(_, entry)| entry.clone()).collect(),
        num_rows,
    );

    let used_columns = raw_expr
        .column_refs()
        .keys()
        .copied()
        .sorted()
        .collect::<Vec<_>>();

    let args: databend_common_exception::Result<_> = try {
        // For test only, we just support agg function call here
        let databend_common_expression::RawExpr::FunctionCall {
            name, params, args, ..
        } = raw_expr
        else {
            unimplemented!()
        };

        let mut args: Vec<(Value<AnyType>, DataType)> = args
            .iter()
            .map(|raw_expr| evaluate_scalar_expr(raw_expr, &block))
            .collect::<Result<_>>()
            .unwrap();

        // Convert the delimiter of string_agg to params
        let params = if name.eq_ignore_ascii_case("string_agg") && args.len() == 2 {
            let val = args[1].0.as_scalar().unwrap();
            let params = vec![val.clone()];
            let _ = args.pop();
            params
        } else {
            params
        };

        // Convert the num_buckets of histogram to params
        let params = if name.eq_ignore_ascii_case("histogram") && args.len() == 2 {
            let val = args[1].0.as_scalar().unwrap();
            let params = vec![val.clone()];
            let _ = args.pop();
            params
        } else {
            params
        };

        let arg_columns = args
            .into_iter()
            .map(|(arg, ty)| BlockEntry::new(arg, || (ty, block.num_rows())))
            .collect::<Vec<_>>();

        (name, params, arg_columns)
    };

    let (name, params, arg_columns) = match args {
        Ok(x) => x,
        Err(e) => {
            writeln!(file, "error: {}\n", e.message()).unwrap();
            return;
        }
    };

    let column = match simulator(
        name.as_str(),
        params.clone(),
        &arg_columns,
        block.num_rows(),
        sort_descs.clone(),
    ) {
        Ok((column, _)) => column,
        Err(e) => {
            writeln!(file, "error: {}\n", e.message()).unwrap();
            return;
        }
    };

    let arg_columns_full = arg_columns
        .into_iter()
        .map(|entry| entry.to_column().into())
        .collect::<Vec<BlockEntry>>();
    match simulator(
        name.as_str(),
        params.clone(),
        &arg_columns_full,
        block.num_rows(),
        sort_descs.clone(),
    ) {
        Ok((column2, _)) => {
            assert_eq!(column, column2)
        }
        Err(e) => {
            writeln!(file, "error: {}\n", e.message()).unwrap();
            return;
        }
    };

    writeln!(file, "ast: {text}").unwrap();
    if !sort_descs.is_empty() {
        writeln!(file, "sort_descs: {sort_descs:?}").unwrap();
    }
    {
        let mut table = Table::new();
        table.load_preset("||--+-++|    ++++++");
        table.set_header(["Column", "Data"]);

        let ids = match used_columns.is_empty() {
            true => {
                if entries.is_empty() {
                    vec![]
                } else {
                    vec![0]
                }
            }
            false => used_columns,
        };

        for id in ids.iter() {
            let (name, entry) = &entries[*id];
            table.add_row(&[name.to_string(), format!("{entry:?}")]);
        }
        table.add_row(["Output".to_string(), format!("{column:?}")]);
        writeln!(file, "evaluation (internal):\n{table}").unwrap();
    }
    write!(file, "\n\n").unwrap();
}

fn evaluate_scalar_expr(
    raw_expr: &RawExpr,
    block: &DataBlock,
) -> Result<(Value<AnyType>, DataType)> {
    let expr = type_check::check(raw_expr, &BUILTIN_FUNCTIONS)?;
    let func_ctx = FunctionContext::default();
    let evaluator = Evaluator::new(block, &func_ctx, &BUILTIN_FUNCTIONS);
    let result = evaluator.run(&expr)?;
    Ok((result, expr.data_type().clone()))
}

/// Simulate group-by aggregation.
/// Rows are distributed into two group-by keys.
///
/// Example:
///
/// If the column is:
///
/// ```
/// let column = vec![1, 2, 3, 4, 5];
/// ```
///
/// then the groups are:
///
/// ```
/// let group1 = vec![1, 3, 5];
/// let group2 = vec![2, 4];
/// ```
pub(super) fn simulate_two_groups_group_by(
    name: &str,
    params: Vec<Scalar>,
    entries: &[BlockEntry],
    rows: usize,
    sort_descs: Vec<AggregateFunctionSortDesc>,
) -> databend_common_exception::Result<(Column, DataType)> {
    let arguments: Vec<DataType> = entries.iter().map(|c| c.data_type()).collect();
    let order_by = sort_descs_to_bound_order_by(&sort_descs)?;
    let func = AGGR_REGISTRY.resolve(AggregateFunctionRequest {
        name,
        params: &params,
        args_type: &arguments,
        distinct: false,
        order_by: &order_by,
    })?;
    let data_type = func.signature().return_type.clone();
    let states_layout = v2::get_states_layout(std::slice::from_ref(&func))?;
    let loc = states_layout.states_loc[0].clone();

    let arena = Bump::new();

    // init state for two groups
    let addr1 = arena.alloc_layout(states_layout.layout).into();
    let state1 = AggrState::new(addr1, &loc);
    func.init_state(state1);
    let addr2 = arena.alloc_layout(states_layout.layout).into();
    let state2 = AggrState::new(addr2, &loc);
    func.init_state(state2);

    let places = (0..rows)
        .map(|i| if i % 2 == 0 { addr1 } else { addr2 })
        .collect::<Vec<_>>();

    if entries.is_empty() {
        func.accumulate_row_count_keys(v2::AccumulateRowCountKeysInput {
            states: v2::AggregateStateSet::new(&places, &loc),
        })?;
    } else {
        func.accumulate_keys(v2::AccumulateKeysInput {
            states: v2::AggregateStateSet::new(&places, &loc),
            columns: entries.into(),
            order_by: &[],
        })?;
    }

    let mut builder = ColumnBuilder::with_capacity(&data_type, 1024);
    func.merge_result(v2::MergeResultInput {
        state: state1,
        builder: &mut builder,
    })?;
    func.merge_result(v2::MergeResultInput {
        state: state2,
        builder: &mut builder,
    })?;

    let res = (builder.build(), data_type);
    assert_two_groups_group_by_v2_matches_legacy_result(
        name, params, entries, rows, sort_descs, &res,
    )?;
    Ok(res)
}

pub(super) fn eval_legacy_aggregate_for_test(
    name: &str,
    params: Vec<Scalar>,
    entries: &[BlockEntry],
    rows: usize,
    each_row: bool,
    with_serialize: bool,
    sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<(Column, DataType)> {
    let arguments = entries
        .iter()
        .map(BlockEntry::data_type)
        .collect::<Vec<_>>();
    let order_by = sort_descs_to_bound_order_by(&sort_descs)?;
    let func = AGGR_REGISTRY.resolve(AggregateFunctionRequest {
        name,
        params: &params,
        args_type: &arguments,
        distinct: false,
        order_by: &order_by,
    })?;
    let data_type = func.signature().return_type.clone();

    let eval = EvalAggr::new(func.clone());
    let state = AggrState::new(eval.addr, &eval.state_layout.states_loc[0]);

    if each_row {
        for row in 0..rows {
            func.accumulate_row(v2::AccumulateRowInput {
                state,
                columns: entries.into(),
                row,
            })?;
        }
    } else if entries.is_empty() {
        func.accumulate_row_count(v2::AccumulateRowCountInput { state, rows })?;
    } else {
        func.accumulate(v2::AccumulateInput {
            state,
            columns: entries.into(),
            validity: None,
            order_by: &[],
        })?;
    }

    if with_serialize {
        let data_type = func.state_data_type();
        let mut builder = ColumnBuilder::with_capacity(&data_type, 1);
        let builders = builder.as_tuple_mut().unwrap().as_mut_slice();
        func.serialize(v2::SerializeInput {
            states: v2::AggregateStateSet::new(std::slice::from_ref(&eval.addr), state.loc),
            builders,
        })?;
        func.init_state(state);
        let column = builder.build();
        func.merge_serialized(v2::MergeSerializedInput {
            states: v2::AggregateStateSet::new(std::slice::from_ref(&eval.addr), state.loc),
            state: &column.into(),
            filter: None,
        })?;
    }
    let mut builder = ColumnBuilder::with_capacity(&data_type, 1024);
    func.merge_result(v2::MergeResultInput {
        state,
        builder: &mut builder,
    })?;
    Ok((builder.build(), data_type))
}

struct EvalAggr {
    addr: StateAddr,
    state_layout: StatesLayout,
    _arena: Bump,
    func: v2::AggregateFunctionRef,
}

impl EvalAggr {
    fn new(func: v2::AggregateFunctionRef) -> Self {
        let funcs = [func];
        let state_layout = v2::get_states_layout(&funcs).unwrap();
        let [func] = funcs;

        let _arena = Bump::new();
        let addr = _arena.alloc_layout(state_layout.layout).into();

        let state = AggrState::new(addr, &state_layout.states_loc[0]);
        func.init_state(state);

        Self {
            addr,
            state_layout,
            _arena,
            func,
        }
    }
}

impl Drop for EvalAggr {
    fn drop(&mut self) {
        drop_guard(move || {
            if self.func.state().need_manual_drop() {
                unsafe {
                    self.func
                        .drop_state(AggrState::new(self.addr, &self.state_layout.states_loc[0]));
                }
            }
        })
    }
}
