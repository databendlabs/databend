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

use std::sync::Arc;

use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_functions::systems::FunctionFactory;

use crate::validate_function_arg;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct CallPlan {
    pub name: String,
    pub args: Vec<String>,
}

impl CallPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }

    pub fn validate(&self) -> Result<()> {
        let name = self.name.clone();
        let features = FunctionFactory::instance().get_features(&name)?;
        validate_function_arg(
            &name,
            self.args.len(),
            features.variadic_arguments,
            features.num_arguments,
        )?;
        Ok(())
    }
}
