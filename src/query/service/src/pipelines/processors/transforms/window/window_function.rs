// Copyright 2023 Datafuse Labs.
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

use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::DataBlock;
use common_expression::DataSchema;
use common_functions::aggregates::get_layout_offsets;
use common_functions::aggregates::AggregateFunction;
use common_functions::aggregates::AggregateFunctionFactory;
use common_functions::aggregates::StateAddr;
use common_sql::executor::WindowFunction;

use crate::pipelines::processors::transforms::group_by::Area;

#[derive(Clone)]
pub enum WindowFunctionInfo {
    Aggregate(Arc<dyn AggregateFunction>, Vec<usize>), // (func instance, argument offsets)
    RowNumber,
    Rank,
    DenseRank,
}

pub struct WindowFuncAggImpl {
    // Need to hold arena until `drop`.
    _arena: Area,
    agg: Arc<dyn AggregateFunction>,
    place: StateAddr,
    args: Vec<usize>,
}

impl WindowFuncAggImpl {
    #[inline]
    pub fn reset(&self) {
        self.agg.init_state(self.place);
    }

    #[inline]
    pub fn arg_columns(&self, data: &DataBlock) -> Vec<Column> {
        self.args
            .iter()
            .map(|index| {
                data.get_by_offset(*index)
                    .value
                    .as_column()
                    .cloned()
                    .unwrap()
            })
            .collect()
    }

    #[inline]
    pub fn accumulate_row(&self, args: &[Column], row: usize) -> Result<()> {
        self.agg.accumulate_row(self.place, args, row)
    }

    #[inline]
    pub fn merge_result(&self, builder: &mut ColumnBuilder) -> Result<()> {
        self.agg.merge_result(self.place, builder)
    }
}

impl Drop for WindowFuncAggImpl {
    fn drop(&mut self) {
        if self.agg.need_manual_drop_state() {
            unsafe {
                self.agg.drop_state(self.place);
            }
        }
    }
}

pub enum WindowFunctionImpl {
    Aggregate(WindowFuncAggImpl),
    RowNumber,
    Rank,
    DenseRank,
}

impl WindowFunctionInfo {
    pub fn try_create(window: &WindowFunction, schema: &DataSchema) -> Result<Self> {
        Ok(match window {
            WindowFunction::Aggregate(agg) => {
                let agg_func = AggregateFunctionFactory::instance().get(
                    agg.sig.name.as_str(),
                    agg.sig.params.clone(),
                    agg.sig.args.clone(),
                )?;
                let args = agg
                    .args
                    .iter()
                    .map(|p| {
                        let offset = schema.index_of(&p.to_string())?;
                        Ok(offset)
                    })
                    .collect::<Result<Vec<_>>>()?;
                Self::Aggregate(agg_func, args)
            }
            WindowFunction::RowNumber => Self::RowNumber,
            WindowFunction::Rank => Self::Rank,
            WindowFunction::DenseRank => Self::DenseRank,
        })
    }
}

impl WindowFunctionImpl {
    pub(crate) fn try_create(window: WindowFunctionInfo) -> Result<Self> {
        Ok(match window {
            WindowFunctionInfo::Aggregate(agg, args) => {
                let mut arena = Area::create();
                let mut state_offset = Vec::with_capacity(1);
                let layout = get_layout_offsets(&[agg.clone()], &mut state_offset)?;
                let place: StateAddr = arena.alloc_layout(layout).into();
                let place = place.next(state_offset[0]);
                let agg = WindowFuncAggImpl {
                    _arena: arena,
                    agg,
                    place,
                    args,
                };
                agg.reset();
                Self::Aggregate(agg)
            }
            WindowFunctionInfo::RowNumber => Self::RowNumber,
            WindowFunctionInfo::Rank => Self::Rank,
            WindowFunctionInfo::DenseRank => Self::DenseRank,
        })
    }

    pub fn return_type(&self) -> Result<DataType> {
        Ok(match self {
            Self::Aggregate(agg) => agg.agg.return_type()?,
            Self::RowNumber | Self::Rank | Self::DenseRank => {
                DataType::Number(NumberDataType::UInt64)
            }
        })
    }

    #[inline]
    pub fn reset(&self) {
        if let Self::Aggregate(agg) = self {
            agg.reset();
        }
    }
}
