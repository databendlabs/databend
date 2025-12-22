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

use databend_common_expression::types::DataType;

use crate::IndexType;
use crate::plans::AsyncFunctionArgument;

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AsyncFunctionDesc {
    pub func_name: String,
    pub display_name: String,
    pub output_column: IndexType,
    pub arg_indices: Vec<IndexType>,
    pub data_type: Box<DataType>,

    pub func_arg: AsyncFunctionArgument,
}
