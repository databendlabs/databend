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

use std::sync::LazyLock;

use databend_common_exception::Result;
use databend_common_expression::Symbol;
use databend_common_expression::SymbolOrOffset;
pub use databend_common_expression::aggregate::aggregate_function::AggregateFunctionRegistry;
use databend_common_expression::aggregate_function::AggregateBoundOrderByItem;
use databend_common_expression::aggregate_function::AggregateBoundOrderBySource;

use super::AggregateFunctionSortDesc;
use super::aggregate_function_v2_impl;

pub static AGGR_REGISTRY: LazyLock<AggregateFunctionRegistry> = LazyLock::new(|| {
    let mut registry = AggregateFunctionRegistry::empty();
    aggregate_function_v2_impl::register_functions(&mut registry);
    registry
});

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
