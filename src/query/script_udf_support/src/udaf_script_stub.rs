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

use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::types::DataType;
use databend_common_functions::aggregates::AggregateFunction;
use databend_common_sql::plans::UDFScriptCode;

pub fn create_udaf_script_function(
    _code: &UDFScriptCode,
    _name: String,
    _display_name: String,
    _state_fields: Vec<DataField>,
    _arguments: Vec<DataField>,
    _output_type: DataType,
) -> Result<Arc<dyn AggregateFunction>> {
    Err(ErrorCode::Unimplemented(
        "Script UDF runtime is disabled, rebuild with cargo feature 'script-udf'",
    ))
}
