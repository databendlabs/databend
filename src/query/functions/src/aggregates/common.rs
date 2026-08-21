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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Constant;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::Symbol;
use databend_common_expression::SymbolOrOffset;
use databend_common_expression::aggregate_function::AccumulateInput;
use databend_common_expression::aggregate_function::AccumulateRowCountInput;
use databend_common_expression::aggregate_function::AggregateBoundOrderByItem;
use databend_common_expression::aggregate_function::AggregateBoundOrderBySource;
use databend_common_expression::aggregate_function::AggregateFunctionRequest;
use databend_common_expression::aggregate_function::AggregateStateOwner;
use databend_common_expression::aggregate_function::MergeResultInput;
use databend_common_expression::type_check::check_number;
use databend_common_expression::types::DataType;
use databend_common_expression::types::F64;
use databend_common_expression::types::Number;

use super::registry::AGGR_REGISTRY;
use crate::BUILTIN_FUNCTIONS;

#[derive(Clone, PartialEq, Eq, Hash, Debug, serde::Serialize, serde::Deserialize)]
pub struct AggregateFunctionSortDesc {
    pub index: SymbolOrOffset,
    pub is_reuse_index: bool,
    pub data_type: DataType,
    pub nulls_first: bool,
    pub asc: bool,
}

pub fn sort_descs_to_bound_order_by(
    sort_descs: &[AggregateFunctionSortDesc],
) -> Result<Vec<AggregateBoundOrderByItem>> {
    sort_descs
        .iter()
        .map(|desc| {
            let (symbol, source) = match desc.index {
                SymbolOrOffset::Symbol(symbol) if desc.is_reuse_index => {
                    (symbol, AggregateBoundOrderBySource::Argument {
                        index: symbol.as_usize(),
                    })
                }
                SymbolOrOffset::Offset(offset) if desc.is_reuse_index => {
                    (Symbol::new(offset), AggregateBoundOrderBySource::Argument {
                        index: offset,
                    })
                }
                SymbolOrOffset::Symbol(symbol) => (symbol, AggregateBoundOrderBySource::Derived),
                SymbolOrOffset::Offset(offset) => {
                    (Symbol::new(offset), AggregateBoundOrderBySource::Derived)
                }
            };
            Ok(AggregateBoundOrderByItem {
                symbol,
                source,
                data_type: desc.data_type.clone(),
                asc: desc.asc,
                nulls_first: desc.nulls_first,
            })
        })
        .collect()
}

pub fn eval_aggr(
    name: &str,
    params: Vec<Scalar>,
    entries: &[BlockEntry],
    rows: usize,
    sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<(Column, DataType)> {
    let args_type = entries
        .iter()
        .map(BlockEntry::data_type)
        .collect::<Vec<_>>();
    let order_by = sort_descs_to_bound_order_by(&sort_descs)?;
    let function = AGGR_REGISTRY.resolve(AggregateFunctionRequest {
        name,
        params: &params,
        args_type: &args_type,
        distinct: false,
        order_by: &order_by,
    })?;
    let data_type = function.signature().return_type.clone();
    let owner = AggregateStateOwner::new(vec![function.clone()])?;

    if entries.is_empty() {
        function.accumulate_row_count(AccumulateRowCountInput {
            state: owner.state(0),
            rows,
        })?;
    } else {
        function.accumulate(AccumulateInput {
            state: owner.state(0),
            columns: entries.into(),
            validity: None,
            order_by: &[],
        })?;
    }

    let mut builder = ColumnBuilder::with_capacity(&data_type, 1);
    function.merge_result(MergeResultInput {
        state: owner.state(0),
        builder: &mut builder,
    })?;
    Ok((builder.build(), data_type))
}

pub(super) fn extract_number_param<T: Number>(param: Scalar) -> Result<T> {
    check_number::<T, usize>(
        None,
        &FunctionContext::default(),
        &Constant {
            span: None,
            data_type: param.as_ref().infer_data_type(),
            scalar: param,
        }
        .into(),
        &BUILTIN_FUNCTIONS,
    )
}

pub(super) fn get_levels(params: &[Scalar]) -> Result<Vec<f64>> {
    let levels = match params {
        [] => vec![0.5f64],
        [param] => {
            let level = extract_number_param::<F64>(param.clone())?.0;
            if !(0.0..=1.0).contains(&level) {
                return Err(ErrorCode::BadDataValueType(format!(
                    "level range between [0, 1], got: {:?}",
                    level
                )));
            }
            vec![level]
        }
        params => {
            let mut levels = Vec::with_capacity(params.len());
            for param in params {
                let level = extract_number_param::<F64>(param.clone())?.0;
                if !(0.0..=1.0).contains(&level) {
                    return Err(ErrorCode::BadDataValueType(format!(
                        "level range between [0, 1], got: {:?} in levels",
                        level
                    )));
                }
                levels.push(level);
            }
            levels
        }
    };
    Ok(levels)
}

#[cfg(test)]
mod tests {
    use databend_common_expression::FromData;
    use databend_common_expression::types::ArgType;
    use databend_common_expression::types::UInt64Type;

    use super::*;

    #[test]
    fn test_eval_aggr() -> Result<()> {
        let entries = [UInt64Type::from_data(vec![1, 2, 3, 4]).into()];
        let (sum, sum_type) = eval_aggr("sum0", vec![], &entries, 4, vec![])?;
        assert_eq!(sum, UInt64Type::from_data(vec![10]));
        assert_eq!(sum_type, UInt64Type::data_type());

        let (count, count_type) = eval_aggr("count", vec![], &[], 4, vec![])?;
        assert_eq!(count, UInt64Type::from_data(vec![4]));
        assert_eq!(count_type, UInt64Type::data_type());
        Ok(())
    }
}
