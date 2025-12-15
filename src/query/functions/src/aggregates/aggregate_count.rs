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
use std::fmt;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::number::NumberColumnBuilder;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::UnaryType;
use databend_common_expression::types::ValueType;
use databend_common_expression::utils::column_merge_validity;
use databend_common_expression::AggrStateRegistry;
use databend_common_expression::AggrStateType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::StateSerdeItem;

use super::assert_params;
use super::assert_variadic_arguments;
use super::AggrState;
use super::AggrStateLoc;
use super::AggregateFunction;
use super::AggregateFunctionDescription;
use super::AggregateFunctionSortDesc;
use super::StateAddr;

struct AggregateCountState {
    count: u64,
}

#[derive(Clone)]
pub struct AggregateCountFunction {
    display_name: String,
}

impl AggregateCountFunction {
    pub fn try_create(
        display_name: &str,
        params: Vec<Scalar>,
        arguments: Vec<DataType>,
        _sort_descs: Vec<AggregateFunctionSortDesc>,
    ) -> Result<Arc<dyn AggregateFunction>> {
        assert_params(display_name, params.len(), 0)?;
        assert_variadic_arguments(display_name, arguments.len(), (0, 1))?;
        Ok(Arc::new(AggregateCountFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> AggregateFunctionDescription {
        let features = super::AggregateFunctionFeatures {
            returns_default_when_only_null: true,
            is_decomposable: true,
            ..Default::default()
        };
        AggregateFunctionDescription::creator_with_features(Box::new(Self::try_create), features)
    }
}

impl AggregateFunction for AggregateCountFunction {
    fn name(&self) -> &str {
        "AggregateCountFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(DataType::Number(NumberDataType::UInt64))
    }

    fn init_state(&self, place: AggrState) {
        place.write(|| AggregateCountState { count: 0 });
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        registry.register(AggrStateType::Custom(Layout::new::<AggregateCountState>()));
    }

    // columns may be nullable
    // if not we use validity as the null signs
    fn accumulate(
        &self,
        place: AggrState,
        block: ProjectedBlock,
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let state = place.get::<AggregateCountState>();
        let nulls = if block.is_empty() {
            validity.map(|v| v.null_count()).unwrap_or(0)
        } else {
            match &block[0] {
                BlockEntry::Const(Scalar::Null, DataType::Nullable(_), n) => *n,
                BlockEntry::Column(Column::Nullable(c)) => validity
                    .map(|v| v & (&c.validity))
                    .unwrap_or_else(|| c.validity.clone())
                    .null_count(),
                _ => validity.map(|v| v.null_count()).unwrap_or(0),
            }
        };
        state.count += (input_rows - nulls) as u64;
        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        columns: ProjectedBlock,
        _input_rows: usize,
    ) -> Result<()> {
        let validity = columns
            .iter()
            .fold(None, |acc, col| column_merge_validity(&col.clone(), acc));

        match validity {
            Some(v) => {
                // all nulls
                if v.null_count() == v.len() {
                    return Ok(());
                }
                for (valid, &place) in v.iter().zip(places.iter()) {
                    if valid {
                        let state = AggrState::new(place, loc).get::<AggregateCountState>();
                        state.count += 1;
                    }
                }
            }

            _ => {
                for place in places {
                    let state = AggrState::new(*place, loc).get::<AggregateCountState>();
                    state.count += 1;
                }
            }
        }

        Ok(())
    }

    fn accumulate_row(
        &self,
        place: AggrState,
        _columns: ProjectedBlock,
        _row: usize,
    ) -> Result<()> {
        let state = place.get::<AggregateCountState>();
        state.count += 1;
        Ok(())
    }

    fn serialize_type(&self) -> Vec<StateSerdeItem> {
        vec![StateSerdeItem::DataType(UInt64Type::data_type())]
    }

    fn batch_serialize(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        let mut builder = UInt64Type::downcast_builder(&mut builders[0]);
        for place in places {
            let state = AggrState::new(*place, loc).get::<AggregateCountState>();
            builder.push(state.count);
        }
        Ok(())
    }

    fn batch_merge(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        let view = state.downcast::<UnaryType<UInt64Type>>().unwrap();
        let iter = places.iter().zip(view.iter());
        if let Some(filter) = filter {
            for (place, other) in iter.zip(filter.iter()).filter_map(|(v, b)| b.then_some(v)) {
                let state = AggrState::new(*place, loc).get::<AggregateCountState>();
                state.count += other;
            }
        } else {
            for (place, other) in iter {
                let state = AggrState::new(*place, loc).get::<AggregateCountState>();
                state.count += other;
            }
        }
        Ok(())
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        let state = place.get::<AggregateCountState>();
        let other = rhs.get::<AggregateCountState>();
        state.count += other.count;
        Ok(())
    }

    fn batch_merge_result(
        &self,
        places: &[StateAddr],
        loc: Box<[AggrStateLoc]>,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        match builder {
            ColumnBuilder::Number(NumberColumnBuilder::UInt64(builder)) => {
                for place in places {
                    let state = AggrState::new(*place, &loc).get::<AggregateCountState>();
                    builder.push(state.count);
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn merge_result(
        &self,
        place: AggrState,
        _read_only: bool,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        match builder {
            ColumnBuilder::Number(NumberColumnBuilder::UInt64(builder)) => {
                let state = place.get::<AggregateCountState>();
                builder.push(state.count);
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn get_own_null_adaptor(
        &self,
        _nested_function: super::AggregateFunctionRef,
        _params: Vec<Scalar>,
        _arguments: Vec<DataType>,
    ) -> Result<Option<super::AggregateFunctionRef>> {
        Ok(Some(Arc::new(self.clone())))
    }
}

impl fmt::Display for AggregateCountFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
