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

use std::hash::Hash;
use std::hash::Hasher;

use common_ast::ast::BinaryOperator;
use common_exception::ErrorCode;
use common_exception::Range;
use common_exception::Result;
use common_exception::Span;
use common_expression::types::DataType;
use common_expression::Scalar;
use educe::Educe;
use itertools::Itertools;

use super::WindowFuncFrame;
use super::WindowFuncType;
use crate::binder::ColumnBinding;
use crate::optimizer::ColumnSet;
use crate::optimizer::SExpr;
use crate::IndexType;
use crate::MetadataRef;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ScalarExpr {
    BoundColumnRef(BoundColumnRef),
    ConstantExpr(ConstantExpr),
    WindowFunction(WindowFunc),
    AggregateFunction(AggregateFunction),
    FunctionCall(FunctionCall),
    CastExpr(CastExpr),
    SubqueryExpr(SubqueryExpr),
}

impl ScalarExpr {
    pub fn data_type(&self) -> Result<DataType> {
        Ok(self.as_expr()?.data_type().clone())
    }

    pub fn used_columns(&self) -> ColumnSet {
        match self {
            ScalarExpr::BoundColumnRef(scalar) => ColumnSet::from([scalar.column.index]),
            ScalarExpr::ConstantExpr(_) => ColumnSet::new(),
            ScalarExpr::WindowFunction(scalar) => {
                let mut result = scalar.func.used_columns();
                for scalar in &scalar.partition_by {
                    result = result.union(&scalar.used_columns()).cloned().collect();
                }
                for order in &scalar.order_by {
                    result = result.union(&order.expr.used_columns()).cloned().collect();
                }
                result
            }
            ScalarExpr::AggregateFunction(scalar) => {
                let mut result = ColumnSet::new();
                for scalar in &scalar.args {
                    result = result.union(&scalar.used_columns()).cloned().collect();
                }
                result
            }
            ScalarExpr::FunctionCall(scalar) => {
                let mut result = ColumnSet::new();
                for scalar in &scalar.arguments {
                    result = result.union(&scalar.used_columns()).cloned().collect();
                }
                result
            }
            ScalarExpr::CastExpr(scalar) => scalar.argument.used_columns(),
            ScalarExpr::SubqueryExpr(scalar) => scalar.outer_columns.clone(),
        }
    }

    // Get used tables in ScalarExpr
    pub fn used_tables(&self, metadata: MetadataRef) -> Result<Vec<IndexType>> {
        match self {
            ScalarExpr::BoundColumnRef(scalar) => {
                let mut tables = vec![];
                if let Some(table_index) = scalar.column.table_index {
                    tables = vec![table_index];
                }
                Ok(tables)
            }
            ScalarExpr::ConstantExpr(_) => Ok(vec![]),
            ScalarExpr::AggregateFunction(scalar) => {
                let mut result = vec![];
                for scalar in &scalar.args {
                    result.append(&mut scalar.used_tables(metadata.clone())?);
                }
                Ok(result)
            }
            ScalarExpr::FunctionCall(scalar) => {
                let mut result = vec![];
                for scalar in &scalar.arguments {
                    result.append(&mut scalar.used_tables(metadata.clone())?);
                }
                Ok(result)
            }
            ScalarExpr::CastExpr(scalar) => scalar.argument.used_tables(metadata),
            ScalarExpr::WindowFunction(_) | ScalarExpr::SubqueryExpr(_) => {
                Err(ErrorCode::Unimplemented(
                    "SubqueryExpr/WindowFunction doesn't support used_tables method".to_string(),
                ))
            }
        }
    }

    pub fn span(&self) -> Span {
        match self {
            ScalarExpr::BoundColumnRef(expr) => expr.span,
            ScalarExpr::ConstantExpr(expr) => expr.span,
            ScalarExpr::FunctionCall(expr) => expr.span.or_else(|| {
                let (start, end) = expr
                    .arguments
                    .iter()
                    .filter_map(|x| x.span())
                    .flat_map(|span| [span.start, span.end])
                    .minmax()
                    .into_option()?;
                Some(Range { start, end })
            }),
            ScalarExpr::CastExpr(expr) => expr.span.or(expr.argument.span()),
            ScalarExpr::SubqueryExpr(expr) => expr.span,
            _ => None,
        }
    }
}

impl From<BoundColumnRef> for ScalarExpr {
    fn from(v: BoundColumnRef) -> Self {
        Self::BoundColumnRef(v)
    }
}

impl TryFrom<ScalarExpr> for BoundColumnRef {
    type Error = ErrorCode;
    fn try_from(value: ScalarExpr) -> Result<Self> {
        if let ScalarExpr::BoundColumnRef(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal(
                "Cannot downcast Scalar to BoundColumnRef",
            ))
        }
    }
}

impl From<ConstantExpr> for ScalarExpr {
    fn from(v: ConstantExpr) -> Self {
        Self::ConstantExpr(v)
    }
}

impl TryFrom<ScalarExpr> for ConstantExpr {
    type Error = ErrorCode;
    fn try_from(value: ScalarExpr) -> Result<Self> {
        if let ScalarExpr::ConstantExpr(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal(
                "Cannot downcast Scalar to ConstantExpr",
            ))
        }
    }
}

impl From<AggregateFunction> for ScalarExpr {
    fn from(v: AggregateFunction) -> Self {
        Self::AggregateFunction(v)
    }
}

impl TryFrom<ScalarExpr> for AggregateFunction {
    type Error = ErrorCode;
    fn try_from(value: ScalarExpr) -> Result<Self> {
        if let ScalarExpr::AggregateFunction(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal(
                "Cannot downcast Scalar to AggregateFunction",
            ))
        }
    }
}

impl From<WindowFunc> for ScalarExpr {
    fn from(v: WindowFunc) -> Self {
        Self::WindowFunction(v)
    }
}

impl TryFrom<ScalarExpr> for WindowFunc {
    type Error = ErrorCode;

    fn try_from(value: ScalarExpr) -> Result<Self> {
        if let ScalarExpr::WindowFunction(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal("Cannot downcast Scalar to WindowFunc"))
        }
    }
}

impl From<FunctionCall> for ScalarExpr {
    fn from(v: FunctionCall) -> Self {
        Self::FunctionCall(v)
    }
}

impl TryFrom<ScalarExpr> for FunctionCall {
    type Error = ErrorCode;
    fn try_from(value: ScalarExpr) -> Result<Self> {
        if let ScalarExpr::FunctionCall(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal(
                "Cannot downcast Scalar to FunctionCall",
            ))
        }
    }
}

impl From<CastExpr> for ScalarExpr {
    fn from(v: CastExpr) -> Self {
        Self::CastExpr(v)
    }
}

impl TryFrom<ScalarExpr> for CastExpr {
    type Error = ErrorCode;
    fn try_from(value: ScalarExpr) -> Result<Self> {
        if let ScalarExpr::CastExpr(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal("Cannot downcast Scalar to CastExpr"))
        }
    }
}

impl From<SubqueryExpr> for ScalarExpr {
    fn from(v: SubqueryExpr) -> Self {
        Self::SubqueryExpr(v)
    }
}

impl TryFrom<ScalarExpr> for SubqueryExpr {
    type Error = ErrorCode;
    fn try_from(value: ScalarExpr) -> Result<Self> {
        if let ScalarExpr::SubqueryExpr(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal(
                "Cannot downcast Scalar to SubqueryExpr",
            ))
        }
    }
}

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct BoundColumnRef {
    #[educe(Hash(ignore), PartialEq(ignore), Eq(ignore))]
    pub span: Span,
    pub column: ColumnBinding,
}

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct ConstantExpr {
    #[educe(Hash(ignore), PartialEq(ignore), Eq(ignore))]
    pub span: Span,
    pub value: Scalar,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
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
    pub fn try_from_func_name(name: &str) -> Option<Self> {
        match name {
            "eq" => Some(Self::Equal),
            "noteq" => Some(Self::NotEqual),
            "gt" => Some(Self::GT),
            "lt" => Some(Self::LT),
            "gte" => Some(Self::GTE),
            "lte" => Some(Self::LTE),
            _ => None,
        }
    }

    pub fn to_func_name(&self) -> &'static str {
        match &self {
            ComparisonOp::Equal => "eq",
            ComparisonOp::NotEqual => "noteq",
            ComparisonOp::GT => "gt",
            ComparisonOp::LT => "lt",
            ComparisonOp::GTE => "gte",
            ComparisonOp::LTE => "lte",
        }
    }
}

impl<'a> TryFrom<&'a BinaryOperator> for ComparisonOp {
    type Error = ErrorCode;

    fn try_from(op: &'a BinaryOperator) -> Result<Self> {
        match op {
            BinaryOperator::Gt => Ok(Self::GT),
            BinaryOperator::Lt => Ok(Self::LT),
            BinaryOperator::Gte => Ok(Self::GTE),
            BinaryOperator::Lte => Ok(Self::LTE),
            BinaryOperator::Eq => Ok(Self::Equal),
            BinaryOperator::NotEq => Ok(Self::NotEqual),
            _ => Err(ErrorCode::SemanticError(format!(
                "Unsupported comparison operator {op}"
            ))),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct AggregateFunction {
    pub func_name: String,
    pub distinct: bool,
    pub params: Vec<Scalar>,
    pub args: Vec<ScalarExpr>,
    pub return_type: Box<DataType>,

    pub display_name: String,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct LagLeadFunction {
    /// Is `lag` or `lead`.
    pub is_lag: bool,
    pub arg: Box<ScalarExpr>,
    pub offset: u64,
    pub default: Option<Box<ScalarExpr>>,
    pub return_type: Box<DataType>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct NthValueFunction {
    /// The nth row of the window frame (counting from 1).
    ///
    /// - Some(1): `first_value`
    /// - Some(n): `nth_value`
    /// - None: `last_value`
    pub n: Option<u64>,
    pub arg: Box<ScalarExpr>,
    pub return_type: Box<DataType>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct NtileFunction {
    pub n: u64,
    pub return_type: Box<DataType>,
}

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct WindowFunc {
    #[educe(PartialEq(ignore), Eq(ignore), Hash(ignore))]
    pub span: Span,
    pub display_name: String,
    pub partition_by: Vec<ScalarExpr>,
    pub func: WindowFuncType,
    pub order_by: Vec<WindowOrderBy>,
    pub frame: WindowFuncFrame,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct WindowOrderBy {
    pub expr: ScalarExpr,
    // Optional `ASC` or `DESC`
    pub asc: Option<bool>,
    // Optional `NULLS FIRST` or `NULLS LAST`
    pub nulls_first: Option<bool>,
}

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct FunctionCall {
    #[educe(Hash(ignore), PartialEq(ignore), Eq(ignore))]
    pub span: Span,
    pub func_name: String,
    pub params: Vec<usize>,
    pub arguments: Vec<ScalarExpr>,
}

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct CastExpr {
    #[educe(Hash(ignore), PartialEq(ignore), Eq(ignore))]
    pub span: Span,
    pub is_try: bool,
    pub argument: Box<ScalarExpr>,
    pub target_type: Box<DataType>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum SubqueryType {
    Any,
    All,
    Scalar,
    Exists,
    NotExists,
}

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct SubqueryExpr {
    #[educe(Hash(ignore), PartialEq(ignore), Eq(ignore))]
    pub span: Span,
    pub typ: SubqueryType,
    pub subquery: Box<SExpr>,
    // The expr that is used to compare the result of the subquery (IN/ANY/ALL), such as `t1.a in (select t2.a from t2)`, t1.a is `child_expr`.
    pub child_expr: Option<Box<ScalarExpr>>,
    // Comparison operator for Any/All, such as t1.a = Any (...), `compare_op` is `=`.
    pub compare_op: Option<ComparisonOp>,
    // Output column of Any/All and scalar subqueries.
    pub output_column: ColumnBinding,
    pub projection_index: Option<IndexType>,
    pub(crate) data_type: Box<DataType>,
    #[educe(Hash(method = "hash_column_set"))]
    pub outer_columns: ColumnSet,
}

impl SubqueryExpr {
    pub fn data_type(&self) -> DataType {
        match &self.typ {
            SubqueryType::Scalar => (*self.data_type).clone(),
            SubqueryType::Any
            | SubqueryType::All
            | SubqueryType::Exists
            | SubqueryType::NotExists => DataType::Nullable(Box::new(DataType::Boolean)),
        }
    }
}

fn hash_column_set<H: Hasher>(columns: &ColumnSet, state: &mut H) {
    columns.iter().for_each(|c| c.hash(state));
}
