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

use common_datablocks::DataBlock;
use common_datavalues::ColumnWithField;
use common_datavalues::DataField;
use common_datavalues::DataTypeImpl;
use common_datavalues::DataValue;
use common_exception::Result;
use common_functions::scalars::Function;
use common_functions::scalars::FunctionContext;

use crate::evaluator::TypedVector;

/// A intermediate representation of a evaluable scalar expression, with configurable
/// EvalContext.
#[derive(Clone)]
pub enum EvalNode {
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
        name: String,
    },
    IndexedVariable {
        index: usize,
    },
}

impl EvalNode {
    pub fn eval(&self, func_ctx: &FunctionContext, data_block: &DataBlock) -> Result<TypedVector> {
        match &self {
            EvalNode::Function { func, args } => {
                let args = args
                    .iter()
                    .map(|arg| {
                        let vector = arg.eval(func_ctx, data_block)?;
                        Ok(ColumnWithField::new(
                            vector.vector,
                            DataField::new("", vector.logical_type),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(TypedVector::new(
                    func.eval(func_ctx.clone(), &args, data_block.num_rows())?,
                    func.return_type(),
                ))
            }
            EvalNode::Constant { value, data_type } => {
                let vector = value.as_const_column(data_type, data_block.num_rows())?;
                Ok(TypedVector::new(vector, data_type.clone()))
            }
            EvalNode::Variable { name } => {
                let column = data_block.try_column_by_name(name)?;
                let data_type = data_block
                    .schema()
                    .field_with_name(name)?
                    .data_type()
                    .clone();
                Ok(TypedVector {
                    vector: column.clone(),
                    logical_type: data_type,
                })
            }
            EvalNode::IndexedVariable { index } => {
                let column = data_block.column(*index);
                let data_type = data_block.schema().field(*index).data_type().clone();
                Ok(TypedVector {
                    vector: column.clone(),
                    logical_type: data_type,
                })
            }
        }
    }

    /// Try to evaluate as a constant expression
    pub fn try_eval_const(&self, func_ctx: &FunctionContext) -> Result<(DataValue, DataTypeImpl)> {
        let dummy_data_block = DataBlock::empty();
        let vector = self.eval(func_ctx, &dummy_data_block)?;
        debug_assert!(vector.vector.len() == 1);
        Ok((vector.vector.get(0), vector.logical_type))
    }
}
