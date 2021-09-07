// Copyright 2020 Datafuse Labs.
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

use common_exception::Result;

use crate::prelude::DataColumn;
use crate::DataValue;
use crate::DataValueArithmeticOperator;
use crate::IntervalUnit;
use crate::NumberSerializer;
use crate::TypeSerializer;

pub struct IntervalSerializer {
    unit: IntervalUnit,
}

impl IntervalSerializer {
    pub fn new(unit: IntervalUnit) -> Self {
        Self { unit }
    }
}

impl TypeSerializer for IntervalSerializer {
    fn serialize_strings(&self, column: &DataColumn) -> Result<Vec<String>> {
        let seconds_per_unit = IntervalUnit::avg_seconds(self.unit.clone());
        let seconds = DataColumn::Constant(DataValue::Float64(Some(seconds_per_unit as f64)), 1);
        let interval = column.arithmetic(DataValueArithmeticOperator::Div, &seconds)?;
        NumberSerializer::<f64>::default().serialize_strings(&interval)
    }
}
