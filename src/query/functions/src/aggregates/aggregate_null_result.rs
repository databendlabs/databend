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
use databend_common_expression::types::AnyType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::ValueType;
use databend_common_expression::AggrStateRegistry;
use databend_common_expression::AggrStateType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ProjectedBlock;

use super::aggregate_function::AggregateFunction;
use super::StateAddr;
use crate::aggregates::AggrState;
use crate::aggregates::AggrStateLoc;

#[derive(Clone)]
pub struct AggregateNullResultFunction {
    data_type: DataType,
}

impl AggregateNullResultFunction {
    pub fn try_create(data_type: DataType) -> Result<Arc<dyn AggregateFunction>> {
        Ok(Arc::new(AggregateNullResultFunction { data_type }))
    }
}

impl AggregateFunction for AggregateNullResultFunction {
    fn name(&self) -> &str {
        "AggregateNullResultFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.data_type.clone())
    }

    fn init_state(&self, _place: AggrState) {}

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        registry.register(AggrStateType::Custom(Layout::new::<u8>()));
    }

    fn accumulate(
        &self,
        _place: AggrState,
        _columns: ProjectedBlock,
        _validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        Ok(())
    }

    fn accumulate_keys(
        &self,
        _places: &[StateAddr],
        _loc: &[AggrStateLoc],
        _columns: ProjectedBlock,
        _input_rows: usize,
    ) -> Result<()> {
        Ok(())
    }

    fn accumulate_row(
        &self,
        _place: AggrState,
        _columns: ProjectedBlock,
        _row: usize,
    ) -> Result<()> {
        Ok(())
    }

    fn serialize(&self, _place: AggrState, _writer: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn merge(&self, _place: AggrState, _reader: &mut &[u8]) -> Result<()> {
        Ok(())
    }

    fn merge_states(&self, _place: AggrState, _rhs: AggrState) -> Result<()> {
        Ok(())
    }

    fn merge_result(&self, _place: AggrState, array: &mut ColumnBuilder) -> Result<()> {
        AnyType::push_default(array);
        Ok(())
    }
}

impl fmt::Display for AggregateNullResultFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}
