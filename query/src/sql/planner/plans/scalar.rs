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

use common_ast::ast::BinaryOperator;
use common_datavalues::BooleanType;
use common_datavalues::DataTypeImpl;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use enum_dispatch::enum_dispatch;

use crate::sql::binder::ColumnBinding;
use crate::sql::optimizer::ColumnSet;
use crate::sql::optimizer::SExpr;
use crate::sql::BindContext;

#[enum_dispatch]
pub trait ScalarExpr {
    /// Get return type and nullability
    fn data_type(&self) -> DataTypeImpl;

    fn used_columns(&self) -> ColumnSet;

    // TODO: implement this in the future
    // fn outer_columns(&self) -> ColumnSet;

    // fn contains_aggregate(&self) -> bool;

    // fn contains_subquery(&self) -> bool;
}

#[derive(Clone, PartialEq, Debug)]
#[enum_dispatch(ScalarExpr)]
pub enum Scalar {
    BoundColumnRef(BoundColumnRef),
    ConstantExpr(ConstantExpr),
    AndExpr(AndExpr),
    OrExpr(OrExpr),
    ComparisonExpr(ComparisonExpr),
    AggregateFunction(AggregateFunction),
    FunctionCall(FunctionCall),
    Cast(CastExpr),
    SubqueryExpr(SubqueryExpr),
}

#[derive(Clone, PartialEq, Debug)]
pub struct BoundColumnRef {
    pub column: ColumnBinding,
}

impl ScalarExpr for BoundColumnRef {
    fn data_type(&self) -> DataTypeImpl {
        self.column.data_type.clone()
    }

    fn used_columns(&self) -> ColumnSet {
        ColumnSet::from([self.column.index])
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct ConstantExpr {
    pub value: DataValue,
}

impl ScalarExpr for ConstantExpr {
    fn data_type(&self) -> DataTypeImpl {
        self.value.data_type()
    }

    fn used_columns(&self) -> ColumnSet {
        ColumnSet::new()
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct AndExpr {
    pub left: Box<Scalar>,
    pub right: Box<Scalar>,
}

impl ScalarExpr for AndExpr {
    fn data_type(&self) -> DataTypeImpl {
        BooleanType::new_impl()
    }

    fn used_columns(&self) -> ColumnSet {
        let left: ColumnSet = self.left.used_columns();
        let right: ColumnSet = self.right.used_columns();
        left.union(&right).cloned().collect()
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct OrExpr {
    pub left: Box<Scalar>,
    pub right: Box<Scalar>,
}

impl ScalarExpr for OrExpr {
    fn data_type(&self) -> DataTypeImpl {
        BooleanType::new_impl()
    }

    fn used_columns(&self) -> ColumnSet {
        let left: ColumnSet = self.left.used_columns();
        let right: ColumnSet = self.right.used_columns();
        left.union(&right).cloned().collect()
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum ComparisonOp {
    Equal,
    NotEqual,
    // Greater ">"
    GT,
    // Less "<"
    LT,
    // Greater or equal ">="
    GTE,
    // Less or equal "<="
    LTE,
}

impl ComparisonOp {
    pub fn try_from_binary_op(op: &BinaryOperator) -> Result<Self> {
        match op {
            BinaryOperator::Gt => Ok(Self::GT),
            BinaryOperator::Lt => Ok(Self::LT),
            BinaryOperator::Gte => Ok(Self::GTE),
            BinaryOperator::Lte => Ok(Self::LTE),
            BinaryOperator::Eq => Ok(Self::Equal),
            BinaryOperator::NotEq => Ok(Self::NotEqual),
            _ => Err(ErrorCode::SemanticError(format!(
                "Unsupported comparison operator {}",
                op
            ))),
        }
    }

    pub fn to_func_name(&self) -> String {
        match &self {
            ComparisonOp::Equal => "=",
            ComparisonOp::NotEqual => "<>",
            ComparisonOp::GT => ">",
            ComparisonOp::LT => "<",
            ComparisonOp::GTE => ">=",
            ComparisonOp::LTE => "<=",
        }
        .to_string()
    }
}

impl<'a> TryFrom<&'a BinaryOperator> for ComparisonOp {
    type Error = ErrorCode;

    fn try_from(value: &'a BinaryOperator) -> Result<Self> {
        Self::try_from_binary_op(value)
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct ComparisonExpr {
    pub op: ComparisonOp,
    pub left: Box<Scalar>,
    pub right: Box<Scalar>,
}

impl ScalarExpr for ComparisonExpr {
    fn data_type(&self) -> DataTypeImpl {
        BooleanType::new_impl()
    }

    fn used_columns(&self) -> ColumnSet {
        let left: ColumnSet = self.left.used_columns();
        let right: ColumnSet = self.right.used_columns();
        left.union(&right).cloned().collect()
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct AggregateFunction {
    pub func_name: String,
    pub distinct: bool,
    pub params: Vec<DataValue>,
    pub args: Vec<Scalar>,
    pub return_type: DataTypeImpl,
}

impl ScalarExpr for AggregateFunction {
    fn data_type(&self) -> DataTypeImpl {
        self.return_type.clone()
    }

    fn used_columns(&self) -> ColumnSet {
        let mut result = ColumnSet::new();
        for scalar in self.args.iter() {
            result = result.union(&scalar.used_columns()).cloned().collect();
        }
        result
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct FunctionCall {
    pub arguments: Vec<Scalar>,

    pub func_name: String,
    pub arg_types: Vec<DataTypeImpl>,
    pub return_type: DataTypeImpl,
}

impl ScalarExpr for FunctionCall {
    fn data_type(&self) -> DataTypeImpl {
        self.return_type.clone()
    }

    fn used_columns(&self) -> ColumnSet {
        let mut result = ColumnSet::new();
        for scalar in self.arguments.iter() {
            result = result.union(&scalar.used_columns()).cloned().collect();
        }
        result
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct CastExpr {
    pub argument: Box<Scalar>,
    pub from_type: DataTypeImpl,
    pub target_type: DataTypeImpl,
}

impl ScalarExpr for CastExpr {
    fn data_type(&self) -> DataTypeImpl {
        self.target_type.clone()
    }

    fn used_columns(&self) -> ColumnSet {
        self.argument.used_columns()
    }
}

#[derive(Clone, Debug)]
pub struct SubqueryExpr {
    pub subquery: SExpr,
    pub data_type: DataTypeImpl,
    pub allow_multi_rows: bool,
    pub output_context: Box<BindContext>,
}

impl ScalarExpr for SubqueryExpr {
    fn data_type(&self) -> DataTypeImpl {
        self.data_type.clone()
    }

    fn used_columns(&self) -> ColumnSet {
        todo!()
    }
}

impl PartialEq for SubqueryExpr {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}
