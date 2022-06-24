// Copyright 2022 Datafuse Labs.
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

use common_datavalues::DataType;
use common_datavalues::DataTypeImpl;
use common_exception::Result;
use common_functions::scalars::CastFunction;
use common_functions::scalars::FunctionFactory;

use crate::common::evaluator::eval_node::EvalNode;
use crate::common::evaluator::Evaluator;
use crate::sql::exec::PhysicalScalar;

impl Evaluator {
    pub fn eval_physical_scalar<VectorID>(
        physical_scalar: &PhysicalScalar,
    ) -> Result<EvalNode<VectorID>>
    where VectorID: From<String> {
        match physical_scalar {
            PhysicalScalar::Variable { column_id, .. } => Ok(EvalNode::Variable {
                id: column_id.clone().into(),
            }),
            PhysicalScalar::Constant { value, data_type } => Ok(EvalNode::Constant {
                value: value.clone(),
                data_type: data_type.clone(),
            }),
            PhysicalScalar::Function { name, args, .. } => {
                let data_types: Vec<&DataTypeImpl> = args.iter().map(|(_, v)| v).collect();
                let func = FunctionFactory::instance().get(name, &data_types)?;
                let args = args
                    .iter()
                    .map(|(v, _)| Self::eval_physical_scalar(v))
                    .collect::<Result<_>>()?;
                Ok(EvalNode::Function { func, args })
            }
            PhysicalScalar::Cast { target, input } => {
                let from = input.data_type();
                let cast_func = CastFunction::create_try("", target.name().as_str(), from)?;
                Ok(EvalNode::Function {
                    func: cast_func,
                    args: vec![Self::eval_physical_scalar(input)?],
                })
            }
        }
    }
}
