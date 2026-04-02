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
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_pipeline_transforms::processors::Transform;

use crate::ScriptUdfFunctionDesc;

pub struct TransformUdfScript;

impl TransformUdfScript {
    pub fn new(
        _func_ctx: FunctionContext,
        _funcs: Vec<ScriptUdfFunctionDesc>,
        _script_runtimes: (),
    ) -> Self {
        Self
    }

    pub fn init_runtime(_funcs: &[ScriptUdfFunctionDesc]) -> Result<()> {
        Err(ErrorCode::Unimplemented(
            "Script UDF runtime is disabled, rebuild with cargo feature 'script-udf'",
        ))
    }
}

impl Transform for TransformUdfScript {
    const NAME: &'static str = "UDFScriptTransform";

    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    fn transform(&mut self, _data_block: DataBlock) -> Result<DataBlock> {
        Err(ErrorCode::Unimplemented(
            "Script UDF runtime is disabled, rebuild with cargo feature 'script-udf'",
        ))
    }
}
