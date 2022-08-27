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

use std::hash::Hash;

use common_ast::ast::BinaryOperator;
use common_datavalues::BooleanType;
use common_datavalues::DataTypeImpl;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::FunctionFactory;

use crate::sql::binder::ColumnBinding;
use crate::sql::optimizer::ColumnSet;
use crate::sql::optimizer::SExpr;
use crate::sql::IndexType;

pub trait ScalarExpr {
    /// Get return type and nullability
    fn data_type(&self) -> DataTypeImpl;

    fn used_columns(&self) -> ColumnSet;

    fn is_deterministic(&self) -> bool;

    // TODO: implement this in the future
    // fn outer_columns(&self) -> ColumnSet;

    // fn contains_aggregate(&self) -> bool;

    // fn contains_subquery(&self) -> bool;
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Scalar {
    BoundColumnRef(BoundColumnRef),
    ConstantExpr(ConstantExpr),
    AndExpr(AndExpr),
    OrExpr(OrExpr),
    ComparisonExpr(ComparisonExpr),
    AggregateFunction(AggregateFunction),
    FunctionCall(FunctionCall),
    // TODO(leiysky): maybe we don't need this variant any more
    // after making functions static typed?
    CastExpr(CastExpr),
    SubqueryExpr(SubqueryExpr),
}

impl ScalarExpr for Scalar {
    fn data_type(&self) -> DataTypeImpl {
        match self {
            Scalar::BoundColumnRef(scalar) => scalar.data_type(),
            Scalar::ConstantExpr(scalar) => scalar.data_type(),
            Scalar::AndExpr(scalar) => scalar.data_type(),
            Scalar::OrExpr(scalar) => scalar.data_type(),
            Scalar::ComparisonExpr(scalar) => scalar.data_type(),
            Scalar::AggregateFunction(scalar) => scalar.data_type(),
            Scalar::FunctionCall(scalar) => scalar.data_type(),
            Scalar::CastExpr(scalar) => scalar.data_type(),
            Scalar::SubqueryExpr(scalar) => scalar.data_type(),
        }
    }

    fn used_columns(&self) -> ColumnSet {
        match self {
            Scalar::BoundColumnRef(scalar) => scalar.used_columns(),
            Scalar::ConstantExpr(scalar) => scalar.used_columns(),
            Scalar::AndExpr(scalar) => scalar.used_columns(),
            Scalar::OrExpr(scalar) => scalar.used_columns(),
            Scalar::ComparisonExpr(scalar) => scalar.used_columns(),
            Scalar::AggregateFunction(scalar) => scalar.used_columns(),
            Scalar::FunctionCall(scalar) => scalar.used_columns(),
            Scalar::CastExpr(scalar) => scalar.used_columns(),
            Scalar::SubqueryExpr(scalar) => scalar.used_columns(),
        }
    }

    fn is_deterministic(&self) -> bool {
        match self {
            Scalar::BoundColumnRef(scalar) => scalar.is_deterministic(),
            Scalar::ConstantExpr(scalar) => scalar.is_deterministic(),
            Scalar::AndExpr(scalar) => scalar.is_deterministic(),
            Scalar::OrExpr(scalar) => scalar.is_deterministic(),
            Scalar::ComparisonExpr(scalar) => scalar.is_deterministic(),
            Scalar::AggregateFunction(scalar) => scalar.is_deterministic(),
            Scalar::FunctionCall(scalar) => scalar.is_deterministic(),
            Scalar::CastExpr(scalar) => scalar.is_deterministic(),
            Scalar::SubqueryExpr(scalar) => scalar.is_deterministic(),
        }
    }
}

impl From<BoundColumnRef> for Scalar {
    fn from(v: BoundColumnRef) -> Self {
        Self::BoundColumnRef(v)
    }
}

impl TryFrom<Scalar> for BoundColumnRef {
    type Error = ErrorCode;
    fn try_from(value: Scalar) -> Result<Self> {
        if let Scalar::BoundColumnRef(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::LogicalError(
                "Cannot downcast Scalar to BoundColumnRef",
            ))
        }
    }
}

impl From<ConstantExpr> for Scalar {
    fn from(v: ConstantExpr) -> Self {
        Self::ConstantExpr(v)
    }
}

impl TryFrom<Scalar> for ConstantExpr {
    type Error = ErrorCode;
    fn try_from(value: Scalar) -> Result<Self> {
        if let Scalar::ConstantExpr(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::LogicalError(
                "Cannot downcast Scalar to ConstantExpr",
            ))
        }
    }
}

impl From<AndExpr> for Scalar {
    fn from(v: AndExpr) -> Self {
        Self::AndExpr(v)
    }
}

impl TryFrom<Scalar> for AndExpr {
    type Error = ErrorCode;
    fn try_from(value: Scalar) -> Result<Self> {
        if let Scalar::AndExpr(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::LogicalError("Cannot downcast Scalar to AndExpr"))
        }
    }
}

impl From<OrExpr> for Scalar {
    fn from(v: OrExpr) -> Self {
        Self::OrExpr(v)
    }
}

impl TryFrom<Scalar> for OrExpr {
    type Error = ErrorCode;
    fn try_from(value: Scalar) -> Result<Self> {
        if let Scalar::OrExpr(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::LogicalError("Cannot downcast Scalar to OrExpr"))
        }
    }
}

impl From<ComparisonExpr> for Scalar {
    fn from(v: ComparisonExpr) -> Self {
        Self::ComparisonExpr(v)
    }
}

impl TryFrom<Scalar> for ComparisonExpr {
    type Error = ErrorCode;
    fn try_from(value: Scalar) -> Result<Self> {
        if let Scalar::ComparisonExpr(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::LogicalError(
                "Cannot downcast Scalar to ComparisonExpr",
            ))
        }
    }
}

impl From<AggregateFunction> for Scalar {
    fn from(v: AggregateFunction) -> Self {
        Self::AggregateFunction(v)
    }
}

impl TryFrom<Scalar> for AggregateFunction {
    type Error = ErrorCode;
    fn try_from(value: Scalar) -> Result<Self> {
        if let Scalar::AggregateFunction(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::LogicalError(
                "Cannot downcast Scalar to AggregateFunction",
            ))
        }
    }
}

impl From<FunctionCall> for Scalar {
    fn from(v: FunctionCall) -> Self {
        Self::FunctionCall(v)
    }
}

impl TryFrom<Scalar> for FunctionCall {
    type Error = ErrorCode;
    fn try_from(value: Scalar) -> Result<Self> {
        if let Scalar::FunctionCall(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::LogicalError(
                "Cannot downcast Scalar to FunctionCall",
            ))
        }
    }
}

impl From<CastExpr> for Scalar {
    fn from(v: CastExpr) -> Self {
        Self::CastExpr(v)
    }
}

impl TryFrom<Scalar> for CastExpr {
    type Error = ErrorCode;
    fn try_from(value: Scalar) -> Result<Self> {
        if let Scalar::CastExpr(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::LogicalError(
                "Cannot downcast Scalar to CastExpr",
            ))
        }
    }
}

impl From<SubqueryExpr> for Scalar {
    fn from(v: SubqueryExpr) -> Self {
        Self::SubqueryExpr(v)
    }
}

impl TryFrom<Scalar> for SubqueryExpr {
    type Error = ErrorCode;
    fn try_from(value: Scalar) -> Result<Self> {
        if let Scalar::SubqueryExpr(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::LogicalError(
                "Cannot downcast Scalar to SubqueryExpr",
            ))
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct BoundColumnRef {
    pub column: ColumnBinding,
}

impl ScalarExpr for BoundColumnRef {
    fn data_type(&self) -> DataTypeImpl {
        *self.column.data_type.clone()
    }

    fn used_columns(&self) -> ColumnSet {
        ColumnSet::from([self.column.index])
    }

    fn is_deterministic(&self) -> bool {
        true
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct ConstantExpr {
    pub value: DataValue,

    pub data_type: Box<DataTypeImpl>,
}

impl ScalarExpr for ConstantExpr {
    fn data_type(&self) -> DataTypeImpl {
        *self.data_type.clone()
    }

    fn used_columns(&self) -> ColumnSet {
        ColumnSet::new()
    }

    fn is_deterministic(&self) -> bool {
        true
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct AndExpr {
    pub left: Box<Scalar>,
    pub right: Box<Scalar>,
    pub return_type: Box<DataTypeImpl>,
}

impl ScalarExpr for AndExpr {
    fn data_type(&self) -> DataTypeImpl {
        *self.return_type.clone()
    }

    fn used_columns(&self) -> ColumnSet {
        let left: ColumnSet = self.left.used_columns();
        let right: ColumnSet = self.right.used_columns();
        left.union(&right).cloned().collect()
    }

    fn is_deterministic(&self) -> bool {
        self.left.is_deterministic() && self.right.is_deterministic()
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct OrExpr {
    pub left: Box<Scalar>,
    pub right: Box<Scalar>,
    pub return_type: Box<DataTypeImpl>,
}

impl ScalarExpr for OrExpr {
    fn data_type(&self) -> DataTypeImpl {
        *self.return_type.clone()
    }

    fn used_columns(&self) -> ColumnSet {
        let left: ColumnSet = self.left.used_columns();
        let right: ColumnSet = self.right.used_columns();
        left.union(&right).cloned().collect()
    }

    fn is_deterministic(&self) -> bool {
        self.left.is_deterministic() && self.right.is_deterministic()
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
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

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct ComparisonExpr {
    pub op: ComparisonOp,
    pub left: Box<Scalar>,
    pub right: Box<Scalar>,
    pub return_type: Box<DataTypeImpl>,
}

impl ScalarExpr for ComparisonExpr {
    fn data_type(&self) -> DataTypeImpl {
        *self.return_type.clone()
    }

    fn used_columns(&self) -> ColumnSet {
        let left: ColumnSet = self.left.used_columns();
        let right: ColumnSet = self.right.used_columns();
        left.union(&right).cloned().collect()
    }

    fn is_deterministic(&self) -> bool {
        FunctionFactory::instance()
            .get_features(self.op.to_func_name())
            .unwrap()
            .is_deterministic
            && self.left.is_deterministic()
            && self.right.is_deterministic()
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct AggregateFunction {
    pub display_name: String,

    pub func_name: String,
    pub distinct: bool,
    pub params: Vec<DataValue>,
    pub args: Vec<Scalar>,
    pub return_type: Box<DataTypeImpl>,
}

impl ScalarExpr for AggregateFunction {
    fn data_type(&self) -> DataTypeImpl {
        *self.return_type.clone()
    }

    fn used_columns(&self) -> ColumnSet {
        let mut result = ColumnSet::new();
        for scalar in self.args.iter() {
            result = result.union(&scalar.used_columns()).cloned().collect();
        }
        result
    }

    fn is_deterministic(&self) -> bool {
        false
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct FunctionCall {
    pub arguments: Vec<Scalar>,

    pub func_name: String,
    pub arg_types: Vec<DataTypeImpl>,
    pub return_type: Box<DataTypeImpl>,
}

impl ScalarExpr for FunctionCall {
    fn data_type(&self) -> DataTypeImpl {
        *self.return_type.clone()
    }

    fn used_columns(&self) -> ColumnSet {
        let mut result = ColumnSet::new();
        for scalar in self.arguments.iter() {
            result = result.union(&scalar.used_columns()).cloned().collect();
        }
        result
    }

    fn is_deterministic(&self) -> bool {
        FunctionFactory::instance()
            .get_features(&self.func_name)
            .map_or(false, |feature| feature.is_deterministic)
            && self.arguments.iter().all(|arg| arg.is_deterministic())
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct CastExpr {
    pub argument: Box<Scalar>,
    pub from_type: Box<DataTypeImpl>,
    pub target_type: Box<DataTypeImpl>,
}

impl ScalarExpr for CastExpr {
    fn data_type(&self) -> DataTypeImpl {
        *self.target_type.clone()
    }

    fn used_columns(&self) -> ColumnSet {
        self.argument.used_columns()
    }

    fn is_deterministic(&self) -> bool {
        true
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum SubqueryType {
    Any,
    All,
    Scalar,
    Exists,
    NotExists,
}

#[derive(Clone, Debug)]
pub struct SubqueryExpr {
    pub typ: SubqueryType,
    pub subquery: Box<SExpr>,
    // The expr that is used to compare the result of the subquery (IN/ANY/ALL), such as `t1.a in (select t2.a from t2)`, t1.a is `child_expr`.
    pub child_expr: Option<Box<Scalar>>,
    // Comparison operator for Any/All, such as t1.a = Any (...), `compare_op` is `=`.
    pub compare_op: Option<ComparisonOp>,
    pub index: Option<IndexType>,
    pub data_type: Box<DataTypeImpl>,
    pub allow_multi_rows: bool,
    pub outer_columns: ColumnSet,
}

impl PartialEq for SubqueryExpr {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

impl Eq for SubqueryExpr {}

impl Hash for SubqueryExpr {
    fn hash<H: std::hash::Hasher>(&self, _state: &mut H) {
        unreachable!()
    }
}

impl ScalarExpr for SubqueryExpr {
    fn data_type(&self) -> DataTypeImpl {
        match &self.typ {
            SubqueryType::Scalar => *self.data_type.clone(),

            SubqueryType::Any
            | SubqueryType::All
            | SubqueryType::Exists
            | SubqueryType::NotExists => BooleanType::new_impl(),
        }
    }

    fn used_columns(&self) -> ColumnSet {
        self.outer_columns.clone()
    }

    fn is_deterministic(&self) -> bool {
        false
    }
}
