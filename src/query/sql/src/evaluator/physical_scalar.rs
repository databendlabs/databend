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
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::in_evaluator;
use common_functions::scalars::CastFunction;
use common_functions::scalars::FunctionFactory;
use common_planner::PhysicalScalar;

use crate::evaluator::eval_node::EvalNode;
use crate::evaluator::Evaluator;

pub trait PhysicalScalarOp {
    fn binary_op(&self, name: &str, other: &Self) -> Result<PhysicalScalar>;

    fn and(&self, other: &Self) -> Result<PhysicalScalar> {
        self.binary_op("and", other)
    }

    fn or(&self, other: &Self) -> Result<PhysicalScalar> {
        self.binary_op("or", other)
    }

    fn eq(&self, other: &Self) -> Result<PhysicalScalar> {
        self.binary_op("=", other)
    }

    fn not_eq(&self, other: &Self) -> Result<PhysicalScalar> {
        self.binary_op("!=", other)
    }

    fn gt_eq(&self, other: &Self) -> Result<PhysicalScalar> {
        self.binary_op(">=", other)
    }

    fn gt(&self, other: &Self) -> Result<PhysicalScalar> {
        self.binary_op(">", other)
    }

    fn lt_eq(&self, other: &Self) -> Result<PhysicalScalar> {
        self.binary_op("<=", other)
    }

    fn lt(&self, other: &Self) -> Result<PhysicalScalar> {
        self.binary_op("=", other)
    }
}

impl PhysicalScalarOp for PhysicalScalar {
    fn binary_op(&self, name: &str, other: &PhysicalScalar) -> Result<PhysicalScalar> {
        let func =
            FunctionFactory::instance().get(name, &[&self.data_type(), &other.data_type()])?;

        Ok(PhysicalScalar::Function {
            name: name.to_owned(),
            args: vec![self.clone(), other.clone()],
            return_type: func.return_type(),
        })
    }
}

impl Evaluator {
    pub fn eval_physical_scalars(physical_scalars: &[PhysicalScalar]) -> Result<Vec<EvalNode>> {
        physical_scalars
            .iter()
            .map(Evaluator::eval_physical_scalar)
            .collect::<Result<_>>()
    }

    pub fn eval_physical_scalar(physical_scalar: &PhysicalScalar) -> Result<EvalNode> {
        match physical_scalar {
            PhysicalScalar::Constant { value, data_type } => Ok(EvalNode::Constant {
                value: value.clone(),
                data_type: data_type.clone(),
            }),
            PhysicalScalar::Function { name, args, .. } => {
                let eval_args: Vec<EvalNode> = args
                    .iter()
                    .map(Self::eval_physical_scalar)
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
                            in_evaluator::create_by_values::<true>(args[0].data_type(), vs.clone())
                        } else {
                            in_evaluator::create_by_values::<false>(args[0].data_type(), vs.clone())
                        }?;

                        return Ok(EvalNode::Function {
                            func,
                            args: vec![eval_args[0].clone()],
                        });
                    } else {
                        return Err(ErrorCode::SyntaxException(
                            "IN expression must have a literal array or subquery as the second argument",
                        ));
                    }
                }

                let data_types: Vec<DataTypeImpl> = args.iter().map(|v| v.data_type()).collect();
                let data_types: Vec<&DataTypeImpl> = data_types.iter().map(|v| v).collect();

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
