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

mod agg;
mod agg_hashtable;

use std::io::Write;

use bumpalo::Bump;
use comfy_table::Table;
use databend_common_exception::Result;
use databend_common_expression::type_check;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::FunctionContext;
use databend_common_expression::RawExpr;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use databend_common_functions::BUILTIN_FUNCTIONS;
use itertools::Itertools;

use super::scalars::parser;

pub trait AggregationSimulator = Fn(&str, Vec<Scalar>, &[Column], usize) -> databend_common_exception::Result<(Column, DataType)>
    + Copy;

/// run ast which is agg expr
pub fn run_agg_ast(
    file: &mut impl Write,
    text: &str,
    columns: &[(&str, Column)],
    simulator: impl AggregationSimulator,
) {
    let raw_expr = parser::parse_raw_expr(
        text,
        &columns
            .iter()
            .map(|(name, col)| (*name, col.data_type()))
            .collect::<Vec<_>>(),
    );

    let num_rows = columns.iter().map(|col| col.1.len()).max().unwrap_or(0);
    let block = DataBlock::new(
        columns
            .iter()
            .map(|(_, col)| BlockEntry::new(col.data_type(), Value::Column(col.clone())))
            .collect::<Vec<_>>(),
        num_rows,
    );

    let used_columns = raw_expr
        .column_refs()
        .keys()
        .cloned()
        .sorted()
        .collect::<Vec<_>>();

    // For test only, we just support agg function call here
    let result: databend_common_exception::Result<(Column, DataType)> = try {
        match raw_expr {
            databend_common_expression::RawExpr::FunctionCall {
                name, params, args, ..
            } => {
                let args: Vec<(Value<AnyType>, DataType)> = args
                    .iter()
                    .map(|raw_expr| run_scalar_expr(raw_expr, &block))
                    .collect::<Result<_>>()
                    .unwrap();

                // Convert the delimiter of string_agg to params
                let params = if name.eq_ignore_ascii_case("string_agg") && args.len() == 2 {
                    let val = args[1].0.as_scalar().unwrap();
                    vec![val.clone()]
                } else {
                    params
                };

                // Convert the num_buckets of histogram to params
                let params = if name.eq_ignore_ascii_case("histogram") && args.len() == 2 {
                    let val = args[1].0.as_scalar().unwrap();
                    vec![val.clone()]
                } else {
                    params
                };

                let arg_columns: Vec<Column> = args
                    .iter()
                    .map(|(arg, ty)| match arg {
                        Value::Scalar(s) => {
                            let builder = ColumnBuilder::repeat(&s.as_ref(), block.num_rows(), ty);
                            builder.build()
                        }
                        Value::Column(c) => c.clone(),
                    })
                    .collect();

                simulator(name.as_str(), params, &arg_columns, block.num_rows())?
            }
            _ => unimplemented!(),
        }
    };

    match result {
        Ok((column, _)) => {
            writeln!(file, "ast: {text}").unwrap();
            {
                let mut table = Table::new();
                table.load_preset("||--+-++|    ++++++");
                table.set_header(["Column", "Data"]);

                let ids = match used_columns.is_empty() {
                    true => {
                        if columns.is_empty() {
                            vec![]
                        } else {
                            vec![0]
                        }
                    }
                    false => used_columns,
                };

                for id in ids.iter() {
                    let (name, col) = &columns[*id];
                    table.add_row(&[name.to_string(), format!("{col:?}")]);
                }
                table.add_row(["Output".to_string(), format!("{column:?}")]);
                writeln!(file, "evaluation (internal):\n{table}").unwrap();
            }
            write!(file, "\n\n").unwrap();
        }
        Err(e) => {
            writeln!(file, "error: {}\n", e.message()).unwrap();
        }
    }
}

pub fn run_scalar_expr(
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
pub fn simulate_two_groups_group_by(
    name: &str,
    params: Vec<Scalar>,
    columns: &[Column],
    rows: usize,
) -> databend_common_exception::Result<(Column, DataType)> {
    let factory = AggregateFunctionFactory::instance();
    let arguments: Vec<DataType> = columns.iter().map(|c| c.data_type()).collect();

    let func = factory.get(name, params, arguments)?;
    let data_type = func.return_type()?;

    let arena = Bump::new();

    // init state for two groups
    let addr1 = arena.alloc_layout(func.state_layout());
    func.init_state(addr1.into());
    let addr2 = arena.alloc_layout(func.state_layout());
    func.init_state(addr2.into());

    let places = (0..rows)
        .map(|i| {
            if i % 2 == 0 {
                addr1.into()
            } else {
                addr2.into()
            }
        })
        .collect::<Vec<_>>();

    func.accumulate_keys(&places, 0, columns.into(), rows)?;

    let mut builder = ColumnBuilder::with_capacity(&data_type, 1024);
    func.merge_result(addr1.into(), &mut builder)?;
    func.merge_result(addr2.into(), &mut builder)?;

    Ok((builder.build(), data_type))
}
