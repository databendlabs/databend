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

use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::Scalar;

use super::aggregate_null_variadic_adaptor::AggregateNullVariadicAdaptor;
use super::AggregateNullUnaryAdaptor;
use crate::aggregates::aggregate_function_factory::AggregateFunctionFeatures;
use crate::aggregates::aggregate_null_result::AggregateNullResultFunction;
use crate::aggregates::AggregateFunctionRef;

#[derive(Clone)]
pub struct AggregateFunctionCombinatorNull {}

impl AggregateFunctionCombinatorNull {
    pub fn transform_arguments(arguments: &[DataType]) -> Result<Vec<DataType>> {
        let mut results = Vec::with_capacity(arguments.len());

        for arg in arguments.iter() {
            match arg {
                DataType::Nullable(box ty) => {
                    results.push(ty.clone());
                }
                _ => {
                    results.push(arg.clone());
                }
            }
        }
        Ok(results)
    }

    pub fn transform_params(params: &[Scalar]) -> Result<Vec<Scalar>> {
        Ok(params.to_owned())
    }

    pub fn try_create(
        _name: &str,
        params: Vec<Scalar>,
        arguments: Vec<DataType>,
        nested: AggregateFunctionRef,
        properties: AggregateFunctionFeatures,
    ) -> Result<AggregateFunctionRef> {
        // has_null_types
        if !arguments.is_empty() && arguments.iter().any(|f| f == &DataType::Null) {
            if properties.returns_default_when_only_null {
                return AggregateNullResultFunction::try_create(DataType::Number(
                    NumberDataType::UInt64,
                ));
            } else {
                return AggregateNullResultFunction::try_create(DataType::Null);
            }
        }
        let params = Self::transform_params(&params)?;
        let arguments = Self::transform_arguments(&arguments)?;
        let size = arguments.len();

        // Some functions may have their own null adaptor
        if let Some(null_adaptor) =
            nested.get_own_null_adaptor(nested.clone(), params, arguments)?
        {
            return Ok(null_adaptor);
        }

        let return_type = nested.return_type()?;
        let result_is_null =
            !properties.returns_default_when_only_null && return_type.can_inside_nullable();

        match size {
            1 => match result_is_null {
                true => Ok(AggregateNullUnaryAdaptor::<true>::create(nested)),
                false => Ok(AggregateNullUnaryAdaptor::<false>::create(nested)),
            },

            _ => match result_is_null {
                true => Ok(AggregateNullVariadicAdaptor::<true>::create(nested)),
                false => Ok(AggregateNullVariadicAdaptor::<false>::create(nested)),
            },
        }
    }
}
