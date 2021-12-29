// Copyright 2021 Datafuse Labs.
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

use common_datavalues::prelude::*;
use common_exception::Result;
use common_functions::scalars::Function;

/// A helper function to evaluate function, not testing only.
#[allow(dead_code)]
pub fn eval_function(
    columns: &DataColumnsWithField,
    rows: usize,
    func: Box<dyn Function>,
) -> Result<DataColumn> {
    if func.passthrough_null() {
        let arg_column_validities = columns
            .iter()
            .map(|column_with_field| {
                let col = column_with_field.column();
                col.get_validity()
            })
            .collect::<Vec<_>>();

        let v = func.eval(columns, rows)?;
        let v = v.apply_validities(arg_column_validities.as_ref())?;
        Ok(v)
    } else {
        func.eval(columns, rows)
    }
}
