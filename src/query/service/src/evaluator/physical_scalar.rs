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
use common_datavalues::DataValue;
use common_exception::Result;
use common_functions::scalars::in_evalutor;
use common_functions::scalars::CastFunction;
use common_functions::scalars::FunctionFactory;

use crate::evaluator::eval_node::EvalNode;
use crate::evaluator::Evaluator;
use crate::sql::executor::PhysicalScalar;

impl Evaluator {
    pub fn eval_physical_scalars(physical_scalars: &[PhysicalScalar]) -> Result<Vec<EvalNode>> {
        physical_scalars
            .iter()
            .map(Evaluator::eval_physical_scalar)
            .collect::<Result<_>>()
    }

    pub fn eval_physical_scalar(physical_scalar: &PhysicalScalar) -> Result<EvalNode> {
        match physical_scalar {
            PhysicalScalar::Variable { column_id, .. } => Ok(EvalNode::Variable {
                name: column_id.clone(),
            }),
            PhysicalScalar::Constant { value, data_type } => Ok(EvalNode::Constant {
                value: value.clone(),
                data_type: data_type.clone(),
            }),
            PhysicalScalar::Function { name, args, .. } => {
                let eval_args: Vec<EvalNode> = args
                    .iter()
                    .map(|(v, _)| Self::eval_physical_scalar(v))
                    .collect::<Result<_>>()?;

                // special case for in function
                let name_lower = name.to_lowercase();
                if name_lower.as_str() == "in" || name_lower.as_str() == "not_in" {
                    if let EvalNode::Constant {
                        value: DataValue::Struct(vs),
                        ..
                    } = &eval_args[1]
                    {
                        let func = if name_lower.as_str() == "not_in" {
                            in_evalutor::create_by_values::<true>(args[0].1.clone(), vs.clone())
                        } else {
                            in_evalutor::create_by_values::<false>(args[0].1.clone(), vs.clone())
                        }?;

                        return Ok(EvalNode::Function {
                            func,
                            args: vec![eval_args[0].clone()],
                        });
                    }
                }

                let data_types: Vec<&DataTypeImpl> = args.iter().map(|(_, v)| v).collect();
                let func = FunctionFactory::instance().get(name, &data_types)?;
                Ok(EvalNode::Function {
                    func,
                    args: eval_args,
                })
            }
            PhysicalScalar::Cast { target, input } => {
                let from = input.data_type();
                let cast_func = if target.is_nullable() {
                    CastFunction::create_try("", target.name().as_str(), from)?
                } else {
                    CastFunction::create("", target.name().as_str(), from)?
                };
                Ok(EvalNode::Function {
                    func: cast_func,
                    args: vec![Self::eval_physical_scalar(input)?],
                })
            }
            PhysicalScalar::IndexedVariable { index, .. } => {
                Ok(EvalNode::IndexedVariable { index: *index })
            }
        }
    }
}
