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

use crate::aggregates::aggregate_function_factory::AggregateFunctionCreator;
use crate::aggregates::AggregateFunctionRef;

#[allow(dead_code)]
pub trait AggregateCombinator: Sync + Send {
    fn name() -> &'static str;

    fn transform_params(params: &[DataValue]) -> Result<Vec<DataValue>> {
        Ok(params.to_owned())
    }
    fn transform_arguments(arguments: &[DataField]) -> Result<Vec<DataField>> {
        Ok(arguments.to_owned())
    }

    fn transform_aggr_function(
        nested_name: &str,
        params: Vec<DataValue>,
        arguments: Vec<DataField>,
        nested_creator: &AggregateFunctionCreator,
    ) -> Result<AggregateFunctionRef>;
}
