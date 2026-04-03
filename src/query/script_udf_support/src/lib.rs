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

#![feature(box_patterns)]

#[cfg(feature = "script-udf")]
mod runtime_pool;
#[cfg(feature = "script-udf")]
mod transform_udf_script;
#[cfg(not(feature = "script-udf"))]
mod transform_udf_script_stub;
#[cfg(feature = "script-udf")]
mod udaf_script;
#[cfg(not(feature = "script-udf"))]
mod udaf_script_stub;

use std::collections::BTreeMap;

use databend_common_expression::types::DataType;
use databend_common_sql::IndexType;
use databend_common_sql::Symbol;
use databend_common_sql::plans::UDFType;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ScriptUdfFunctionDesc {
    pub name: String,
    pub func_name: String,
    pub output_column: Symbol,
    pub arg_indices: Vec<IndexType>,
    pub arg_exprs: Vec<String>,
    pub data_type: Box<DataType>,
    pub headers: BTreeMap<String, String>,
    pub udf_type: UDFType,
}

#[cfg(feature = "script-udf")]
pub use transform_udf_script::TransformUdfScript;
#[cfg(not(feature = "script-udf"))]
pub use transform_udf_script_stub::TransformUdfScript;
#[cfg(feature = "script-udf")]
pub use udaf_script::create_udaf_script_function;
#[cfg(not(feature = "script-udf"))]
pub use udaf_script_stub::create_udaf_script_function;
