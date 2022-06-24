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

use std::fmt::Debug;

use common_datavalues::ColumnWithField;
use common_datavalues::DataField;
use common_datavalues::DataTypeImpl;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::Function;
use common_functions::scalars::FunctionContext;

use crate::common::evaluator::eval_context::EmptyEvalContext;
use crate::common::evaluator::TypedVector;
use crate::common::EvalContext;

/// A intermediate representation of a evaluable scalar expression, with configurable
/// EvalContext.
#[derive(Clone)]
pub enum EvalNode<VectorID> {
    Function {
        func: Box<dyn Function>,
        args: Vec<Self>,
    },
    Constant {
        value: DataValue,
        // TODO(leiysky): remove this `data_type` in the future, which should can
        // be inferred from the `value`
        data_type: DataTypeImpl,
    },
    Variable {
        id: VectorID,
    },
}

impl<VectorID> EvalNode<VectorID>
where VectorID: PartialEq + Eq + Clone + Debug
{
    /// Evaluate with given context, which is typically a `DataBlock`
    pub fn eval(
        &self,
        func_ctx: &FunctionContext,
        eval_ctx: &impl EvalContext<VectorID = VectorID>,
    ) -> Result<TypedVector> {
        match &self {
            EvalNode::Function { func, args } => {
                let args = args
                    .iter()
                    .map(|arg| {
                        let vector = arg.eval(func_ctx, eval_ctx)?;
                        Ok(ColumnWithField::new(
                            vector.vector,
                            DataField::new("", vector.logical_type),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(TypedVector::new(
                    func.eval(func_ctx.clone(), &args, eval_ctx.tuple_count())?,
                    func.return_type(),
                ))
            }
            EvalNode::Constant { value, data_type } => {
                let vector = value.as_const_column(data_type, eval_ctx.tuple_count())?;
                Ok(TypedVector::new(vector, data_type.clone()))
            }
            EvalNode::Variable { id } => eval_ctx.get_vector(id),
        }
    }

    /// Try to evaluate as a constant expression
    pub fn try_eval_const(&self, func_ctx: &FunctionContext) -> Result<(DataValue, DataTypeImpl)> {
        let eval_ctx = EmptyEvalContext::<VectorID>::new();
        let vector = self.eval(func_ctx, &eval_ctx)?;
        if vector.vector.is_const() {
            Ok((vector.vector.get(0), vector.logical_type))
        } else {
            Err(ErrorCode::LogicalError(
                "Non-constant column can not be evaluated by try_eval_const",
            ))
        }
    }
}
