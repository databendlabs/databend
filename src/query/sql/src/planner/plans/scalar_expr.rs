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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::str::FromStr;
use std::sync::Arc;

use databend_common_ast::Range;
use databend_common_ast::Span;
use databend_common_ast::ast::BinaryOperator;
use databend_common_catalog::catalog::Catalog;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AutoIncrementExpr;
use databend_common_expression::FunctionKind;
use databend_common_expression::RemoteExpr;
use databend_common_expression::SEARCH_MATCHED_COL_NAME;
use databend_common_expression::SEARCH_SCORE_COL_NAME;
use databend_common_expression::Scalar;
use databend_common_expression::VECTOR_SCORE_COL_NAME;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_functions::aggregates::AggregateFunctionSortDesc;
use databend_common_meta_app::principal::AutoIncrementKey;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::schema::GetAutoIncrementNextValueReq;
use databend_common_meta_app::schema::GetSequenceNextValueReq;
use databend_common_meta_app::schema::SequenceIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::GrantObjectVisibilityChecker;
use educe::Educe;
use enum_as_inner::EnumAsInner;
use itertools::Itertools;

use super::WindowFuncFrame;
use super::WindowFuncType;
use crate::ColumnSet;
use crate::IndexType;
use crate::MetadataRef;
use crate::binder::ColumnBinding;
use crate::optimizer::ir::SExpr;

#[derive(Debug)]
pub enum ScalarExpr {
    BoundColumnRef(BoundColumnRef),
    ConstantExpr(ConstantExpr),
    // A special case of constant expr that has been type checked before
    TypedConstantExpr(ConstantExpr, DataType),
    WindowFunction(WindowFunc),
    AggregateFunction(AggregateFunction),
    LambdaFunction(LambdaFunc),
    FunctionCall(FunctionCall),
    CastExpr(CastExpr),
    SubqueryExpr(SubqueryExpr),
    UDFCall(UDFCall),
    UDAFCall(UDAFCall),
    UDFLambdaCall(UDFLambdaCall),
    AsyncFunctionCall(AsyncFunctionCall),
}

impl Clone for ScalarExpr {
    #[recursive::recursive]
    fn clone(&self) -> Self {
        match self {
            ScalarExpr::BoundColumnRef(v) => ScalarExpr::BoundColumnRef(v.clone()),
            ScalarExpr::ConstantExpr(v) => ScalarExpr::ConstantExpr(v.clone()),
            ScalarExpr::TypedConstantExpr(v, t) => {
                ScalarExpr::TypedConstantExpr(v.clone(), t.clone())
            }
            ScalarExpr::WindowFunction(v) => ScalarExpr::WindowFunction(v.clone()),
            ScalarExpr::AggregateFunction(v) => ScalarExpr::AggregateFunction(v.clone()),
            ScalarExpr::LambdaFunction(v) => ScalarExpr::LambdaFunction(v.clone()),
            ScalarExpr::FunctionCall(v) => ScalarExpr::FunctionCall(v.clone()),
            ScalarExpr::CastExpr(v) => ScalarExpr::CastExpr(v.clone()),
            ScalarExpr::SubqueryExpr(v) => ScalarExpr::SubqueryExpr(v.clone()),
            ScalarExpr::UDFCall(v) => ScalarExpr::UDFCall(v.clone()),
            ScalarExpr::UDFLambdaCall(v) => ScalarExpr::UDFLambdaCall(v.clone()),
            ScalarExpr::UDAFCall(v) => ScalarExpr::UDAFCall(v.clone()),
            ScalarExpr::AsyncFunctionCall(v) => ScalarExpr::AsyncFunctionCall(v.clone()),
        }
    }
}

impl Eq for ScalarExpr {}

impl PartialEq for ScalarExpr {
    #[recursive::recursive]
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ScalarExpr::BoundColumnRef(l), ScalarExpr::BoundColumnRef(r)) => {
                l.column.index == r.column.index && l.column.table_index == r.column.table_index
            }
            (ScalarExpr::ConstantExpr(l), ScalarExpr::ConstantExpr(r)) => l.eq(r),
            (ScalarExpr::WindowFunction(l), ScalarExpr::WindowFunction(r)) => l.eq(r),
            (ScalarExpr::AggregateFunction(l), ScalarExpr::AggregateFunction(r)) => l.eq(r),
            (ScalarExpr::LambdaFunction(l), ScalarExpr::LambdaFunction(r)) => l.eq(r),
            (ScalarExpr::FunctionCall(l), ScalarExpr::FunctionCall(r)) => l.eq(r),
            (ScalarExpr::CastExpr(l), ScalarExpr::CastExpr(r)) => l.eq(r),
            (ScalarExpr::SubqueryExpr(l), ScalarExpr::SubqueryExpr(r)) => l.eq(r),
            (ScalarExpr::UDFCall(l), ScalarExpr::UDFCall(r)) => l.eq(r),
            (ScalarExpr::UDFLambdaCall(l), ScalarExpr::UDFLambdaCall(r)) => l.eq(r),
            (ScalarExpr::UDAFCall(l), ScalarExpr::UDAFCall(r)) => l.eq(r),
            (ScalarExpr::AsyncFunctionCall(l), ScalarExpr::AsyncFunctionCall(r)) => l.eq(r),
            _ => false,
        }
    }
}

impl Hash for ScalarExpr {
    #[recursive::recursive]
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            ScalarExpr::BoundColumnRef(v) => {
                v.column.index.hash(state);
                v.column.table_index.hash(state);
            }
            ScalarExpr::ConstantExpr(v) => v.hash(state),
            ScalarExpr::TypedConstantExpr(v, _) => v.hash(state),
            ScalarExpr::WindowFunction(v) => v.hash(state),
            ScalarExpr::AggregateFunction(v) => v.hash(state),
            ScalarExpr::LambdaFunction(v) => v.hash(state),
            ScalarExpr::FunctionCall(v) => v.hash(state),
            ScalarExpr::CastExpr(v) => v.hash(state),
            ScalarExpr::SubqueryExpr(v) => v.hash(state),
            ScalarExpr::UDFCall(v) => v.hash(state),
            ScalarExpr::UDFLambdaCall(v) => v.hash(state),
            ScalarExpr::UDAFCall(v) => v.hash(state),
            ScalarExpr::AsyncFunctionCall(v) => v.hash(state),
        }
    }
}

impl ScalarExpr {
    pub fn data_type(&self) -> Result<DataType> {
        Ok(self.as_expr()?.data_type().clone())
    }

    pub fn used_columns(&self) -> ColumnSet {
        struct UsedColumnsVisitor {
            columns: ColumnSet,
        }

        impl<'a> Visitor<'a> for UsedColumnsVisitor {
            fn visit_bound_column_ref(&mut self, col: &'a BoundColumnRef) -> Result<()> {
                self.columns.insert(col.column.index);
                Ok(())
            }

            fn visit_subquery(&mut self, subquery: &'a SubqueryExpr) -> Result<()> {
                for idx in subquery.outer_columns.iter() {
                    self.columns.insert(*idx);
                }
                if let Some(child_expr) = subquery.child_expr.as_ref() {
                    self.visit(child_expr)?;
                }
                Ok(())
            }
        }

        let mut visitor = UsedColumnsVisitor {
            columns: ColumnSet::new(),
        };
        visitor.visit(self).unwrap();
        visitor.columns
    }

    // Get used tables in ScalarExpr
    pub fn used_tables(&self) -> Result<Vec<IndexType>> {
        struct UsedTablesVisitor {
            tables: Vec<IndexType>,
        }

        impl<'a> Visitor<'a> for UsedTablesVisitor {
            fn visit_bound_column_ref(&mut self, col: &'a BoundColumnRef) -> Result<()> {
                if let Some(table_index) = col.column.table_index {
                    self.tables.push(table_index);
                }
                Ok(())
            }
        }

        let mut visitor = UsedTablesVisitor { tables: vec![] };
        visitor.visit(self)?;
        Ok(visitor.tables)
    }

    pub fn span(&self) -> Span {
        match self {
            ScalarExpr::BoundColumnRef(expr) => expr.span,
            ScalarExpr::ConstantExpr(expr) => expr.span,
            ScalarExpr::TypedConstantExpr(expr, _) => expr.span,
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
            ScalarExpr::WindowFunction(expr) => expr.span,
            ScalarExpr::AggregateFunction(expr) => expr.span,
            ScalarExpr::LambdaFunction(expr) => expr.span,
            ScalarExpr::CastExpr(expr) => expr.span.or(expr.argument.span()),
            ScalarExpr::SubqueryExpr(expr) => expr.span,
            ScalarExpr::UDFCall(expr) => expr.span,
            ScalarExpr::UDFLambdaCall(expr) => expr.span,
            ScalarExpr::UDAFCall(expr) => expr.span,
            ScalarExpr::AsyncFunctionCall(expr) => expr.span,
        }
    }

    /// Returns true if the expression can be evaluated from a row of data.
    pub fn evaluable(&self) -> bool {
        let mut visitor = EvaluableVisitor {
            evaluable: true,
            has_nextval: false,
        };
        visitor.visit(self).unwrap();
        visitor.evaluable && !visitor.has_nextval
    }

    /// Returns if the expression can be evaluated as default value
    /// and whether contains `nextval` async function.
    pub fn default_value_evaluable(&self) -> (bool, bool) {
        let mut visitor = EvaluableVisitor {
            evaluable: true,
            has_nextval: false,
        };
        visitor.visit(self).unwrap();
        (visitor.evaluable, visitor.has_nextval)
    }

    pub fn replace_column(&mut self, old: IndexType, new: IndexType) -> Result<()> {
        struct ReplaceColumnVisitor {
            old: IndexType,
            new: IndexType,
        }

        impl VisitorMut<'_> for ReplaceColumnVisitor {
            fn visit_bound_column_ref(&mut self, col: &mut BoundColumnRef) -> Result<()> {
                if col.column.index == self.old {
                    col.column.index = self.new;
                }
                Ok(())
            }
        }

        let mut visitor = ReplaceColumnVisitor { old, new };
        visitor.visit(self)?;
        Ok(())
    }

    pub fn replace_column_binding(
        &mut self,
        old: IndexType,
        new_column: &ColumnBinding,
    ) -> Result<()> {
        struct ReplaceColumnBindingVisitor<'a> {
            old: IndexType,
            new_column: &'a ColumnBinding,
        }

        impl VisitorMut<'_> for ReplaceColumnBindingVisitor<'_> {
            fn visit_bound_column_ref(&mut self, col: &mut BoundColumnRef) -> Result<()> {
                if col.column.index == self.old {
                    col.column = self.new_column.clone();
                }
                Ok(())
            }
        }

        let mut visitor = ReplaceColumnBindingVisitor { old, new_column };
        visitor.visit(self)?;
        Ok(())
    }

    pub fn replace_sub_scalar(
        &mut self,
        from_scalar: ScalarExpr,
        to_scalar: ScalarExpr,
    ) -> Result<()> {
        struct ReplaceColumnVisitor {
            from_scalar: ScalarExpr,
            to_scalar: ScalarExpr,
        }

        impl VisitorMut<'_> for ReplaceColumnVisitor {
            fn visit(&mut self, expr: &mut ScalarExpr) -> Result<()> {
                if expr == &self.from_scalar {
                    *expr = self.to_scalar.clone();
                    return Ok(());
                }
                walk_expr_mut(self, expr)
            }
        }

        let mut visitor = ReplaceColumnVisitor {
            from_scalar,
            to_scalar,
        };
        visitor.visit(self)?;
        Ok(())
    }

    pub fn columns_and_data_types(&self, metadata: MetadataRef) -> HashMap<usize, DataType> {
        struct UsedColumnsVisitor {
            columns: HashMap<IndexType, DataType>,
            metadata: MetadataRef,
        }

        impl<'a> Visitor<'a> for UsedColumnsVisitor {
            fn visit_bound_column_ref(&mut self, col: &'a BoundColumnRef) -> Result<()> {
                self.columns
                    .insert(col.column.index, *col.column.data_type.clone());
                Ok(())
            }

            fn visit_subquery(&mut self, subquery: &'a SubqueryExpr) -> Result<()> {
                for idx in subquery.outer_columns.iter() {
                    self.columns
                        .insert(*idx, self.metadata.read().column(*idx).data_type());
                }
                if let Some(child_expr) = subquery.child_expr.as_ref() {
                    self.visit(child_expr)?;
                }
                Ok(())
            }
        }

        let mut visitor = UsedColumnsVisitor {
            columns: HashMap::new(),
            metadata,
        };
        visitor.visit(self).unwrap();
        visitor.columns
    }

    pub fn has_one_column_ref(&self) -> bool {
        struct BoundColumnRefVisitor {
            has_column_ref: bool,
            num_column_ref: usize,
        }

        impl<'a> Visitor<'a> for BoundColumnRefVisitor {
            fn visit_bound_column_ref(&mut self, _col: &'a BoundColumnRef) -> Result<()> {
                self.has_column_ref = true;
                self.num_column_ref += 1;
                Ok(())
            }
        }

        let mut visitor = BoundColumnRefVisitor {
            has_column_ref: false,
            num_column_ref: 0,
        };
        visitor.visit(self).unwrap();
        visitor.has_column_ref && visitor.num_column_ref == 1
    }

    pub fn unify_to_data_type(self, data_type: &DataType) -> Self {
        match self {
            ScalarExpr::ConstantExpr(ref constant_expr) => {
                let infer_type = constant_expr.value.as_ref().infer_data_type();
                if &infer_type != data_type {
                    let cast_expr = CastExpr {
                        span: None,
                        is_try: false,
                        argument: Box::new(self),
                        target_type: Box::new(data_type.clone()),
                    };
                    ScalarExpr::CastExpr(cast_expr)
                } else {
                    self
                }
            }
            other => ScalarExpr::CastExpr(CastExpr {
                span: None,
                is_try: false,
                argument: Box::new(other),
                target_type: Box::new(data_type.clone()),
            }),
        }
    }

    pub fn get_udf_names(&self) -> Result<HashSet<&String>> {
        struct FindUdfNamesVisitor<'a> {
            udfs: HashSet<&'a String>,
        }

        impl<'a> Visitor<'a> for FindUdfNamesVisitor<'a> {
            fn visit_udf_call(&mut self, udf: &'a UDFCall) -> Result<()> {
                for expr in &udf.arguments {
                    self.visit(expr)?;
                }

                self.udfs.insert(&udf.name);
                Ok(())
            }

            fn visit_udf_lambda_call(&mut self, udf: &'a UDFLambdaCall) -> Result<()> {
                self.visit(&udf.scalar)?;
                self.udfs.insert(&udf.func_name);
                Ok(())
            }
        }

        let mut find_udfs = FindUdfNamesVisitor {
            udfs: HashSet::new(),
        };
        find_udfs.visit(self)?;
        Ok(find_udfs.udfs)
    }

    pub fn has_subquery(&self) -> bool {
        struct HasSubqueryVisitor {
            has_subquery: bool,
        }

        impl<'a> Visitor<'a> for HasSubqueryVisitor {
            fn visit_subquery(&mut self, _: &'a SubqueryExpr) -> Result<()> {
                self.has_subquery = true;
                Ok(())
            }
        }

        let mut has_subquery = HasSubqueryVisitor {
            has_subquery: false,
        };
        has_subquery.visit(self).unwrap();
        has_subquery.has_subquery
    }

    pub fn collect_subquery(&self, result: &mut Vec<SubqueryExpr>) {
        struct CollectSubQuery<'a> {
            subquerys: &'a mut Vec<SubqueryExpr>,
        }

        impl<'a> Visitor<'a> for CollectSubQuery<'a> {
            fn visit_subquery(&mut self, subquery: &'a SubqueryExpr) -> Result<()> {
                self.subquerys.push(subquery.clone());
                Ok(())
            }
        }

        let mut visitor = CollectSubQuery { subquerys: result };
        visitor.visit(self).unwrap();
    }
}

struct EvaluableVisitor {
    evaluable: bool,
    has_nextval: bool,
}

impl<'a> Visitor<'a> for EvaluableVisitor {
    fn visit_function_call(&mut self, func: &'a FunctionCall) -> Result<()> {
        if BUILTIN_FUNCTIONS
            .get_property(&func.func_name)
            .map(|property| property.kind == FunctionKind::SRF)
            .unwrap_or(false)
        {
            self.evaluable = false;
        } else {
            for expr in &func.arguments {
                self.visit(expr)?;
            }
        }
        Ok(())
    }
    fn visit_window_function(&mut self, _: &'a WindowFunc) -> Result<()> {
        self.evaluable = false;
        Ok(())
    }
    fn visit_aggregate_function(&mut self, _: &'a AggregateFunction) -> Result<()> {
        self.evaluable = false;
        Ok(())
    }
    fn visit_subquery(&mut self, _: &'a SubqueryExpr) -> Result<()> {
        self.evaluable = false;
        Ok(())
    }
    fn visit_udf_call(&mut self, _: &'a UDFCall) -> Result<()> {
        self.evaluable = false;
        Ok(())
    }
    fn visit_udaf_call(&mut self, _: &'a UDAFCall) -> Result<()> {
        self.evaluable = false;
        Ok(())
    }
    fn visit_udf_lambda_call(&mut self, _: &'a UDFLambdaCall) -> Result<()> {
        self.evaluable = false;
        Ok(())
    }
    fn visit_async_function_call(&mut self, func: &'a AsyncFunctionCall) -> Result<()> {
        if func.func_name == "nextval" {
            self.has_nextval = true;
        } else {
            self.evaluable = false;
        }
        Ok(())
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

impl From<LambdaFunc> for ScalarExpr {
    fn from(v: LambdaFunc) -> Self {
        Self::LambdaFunction(v)
    }
}

impl TryFrom<ScalarExpr> for LambdaFunc {
    type Error = ErrorCode;

    fn try_from(value: ScalarExpr) -> Result<Self> {
        if let ScalarExpr::LambdaFunction(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal("Cannot downcast Scalar to LambdaFunc"))
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

impl From<UDFCall> for ScalarExpr {
    fn from(v: UDFCall) -> Self {
        Self::UDFCall(v)
    }
}

impl TryFrom<ScalarExpr> for UDFCall {
    type Error = ErrorCode;
    fn try_from(value: ScalarExpr) -> Result<Self> {
        if let ScalarExpr::UDFCall(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal("Cannot downcast Scalar to UDFCall"))
        }
    }
}

impl From<UDFLambdaCall> for ScalarExpr {
    fn from(v: UDFLambdaCall) -> Self {
        Self::UDFLambdaCall(v)
    }
}

impl TryFrom<ScalarExpr> for UDFLambdaCall {
    type Error = ErrorCode;
    fn try_from(value: ScalarExpr) -> Result<Self> {
        if let ScalarExpr::UDFLambdaCall(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal(
                "Cannot downcast Scalar to UDFLambdaCall",
            ))
        }
    }
}

impl From<UDAFCall> for ScalarExpr {
    fn from(v: UDAFCall) -> Self {
        Self::UDAFCall(v)
    }
}

impl TryFrom<ScalarExpr> for UDAFCall {
    type Error = ErrorCode;
    fn try_from(value: ScalarExpr) -> Result<Self> {
        if let ScalarExpr::UDAFCall(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal("Cannot downcast Scalar to UDAFCall"))
        }
    }
}

impl From<AsyncFunctionCall> for ScalarExpr {
    fn from(v: AsyncFunctionCall) -> Self {
        Self::AsyncFunctionCall(v)
    }
}

impl TryFrom<ScalarExpr> for AsyncFunctionCall {
    type Error = ErrorCode;
    fn try_from(value: ScalarExpr) -> Result<Self> {
        if let ScalarExpr::AsyncFunctionCall(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal(
                "Cannot downcast Scalar to AsyncFunctionCall",
            ))
        }
    }
}

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct BoundColumnRef {
    #[educe(Hash(ignore), PartialEq(ignore))]
    pub span: Span,
    pub column: ColumnBinding,
}

#[derive(Clone, Debug, Educe, Ord, PartialOrd)]
#[educe(PartialEq, Eq, Hash)]
pub struct ConstantExpr {
    #[educe(Hash(ignore), PartialEq(ignore))]
    pub span: Span,
    pub value: Scalar,
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
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

    pub fn reverse(&self) -> Self {
        match &self {
            ComparisonOp::Equal => ComparisonOp::Equal,
            ComparisonOp::NotEqual => ComparisonOp::NotEqual,
            ComparisonOp::GT => ComparisonOp::LT,
            ComparisonOp::LT => ComparisonOp::GT,
            ComparisonOp::GTE => ComparisonOp::LTE,
            ComparisonOp::LTE => ComparisonOp::GTE,
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

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum SubqueryComparisonOp {
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
    // Like operator for pattern matching
    Like(Option<String>),
}

impl SubqueryComparisonOp {
    fn to_func_name(&self) -> &'static str {
        match &self {
            SubqueryComparisonOp::Equal => "eq",
            SubqueryComparisonOp::NotEqual => "noteq",
            SubqueryComparisonOp::GT => "gt",
            SubqueryComparisonOp::LT => "lt",
            SubqueryComparisonOp::GTE => "gte",
            SubqueryComparisonOp::LTE => "lte",
            SubqueryComparisonOp::Like(_) => "like",
        }
    }

    pub fn to_func_call(&self, span: Span, left: ScalarExpr, right: ScalarExpr) -> FunctionCall {
        if let SubqueryComparisonOp::Like(escape) = self {
            let mut arguments = vec![left, right];
            if let Some(escape) = escape {
                arguments.push(ScalarExpr::ConstantExpr(ConstantExpr {
                    span,
                    value: Scalar::String(escape.clone()),
                }))
            }
            return FunctionCall {
                span,
                func_name: "like".to_string(),
                params: vec![],
                arguments,
            };
        }
        FunctionCall {
            span,
            func_name: self.to_func_name().to_string(),
            params: vec![],
            arguments: vec![left, right],
        }
    }
}

impl<'a> TryFrom<&'a BinaryOperator> for SubqueryComparisonOp {
    type Error = ErrorCode;

    fn try_from(op: &'a BinaryOperator) -> Result<Self> {
        match op {
            BinaryOperator::Gt => Ok(Self::GT),
            BinaryOperator::Lt => Ok(Self::LT),
            BinaryOperator::Gte => Ok(Self::GTE),
            BinaryOperator::Lte => Ok(Self::LTE),
            BinaryOperator::Eq => Ok(Self::Equal),
            BinaryOperator::NotEq => Ok(Self::NotEqual),
            BinaryOperator::Like(escape) => Ok(Self::Like(escape.clone())),
            _ => Err(ErrorCode::SemanticError(format!(
                "Unsupported subquery comparison operator {op}"
            ))),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct AggregateFunctionScalarSortDesc {
    pub expr: ScalarExpr,
    pub is_reuse_index: bool,
    pub nulls_first: bool,
    pub asc: bool,
}

impl TryInto<AggregateFunctionSortDesc> for &AggregateFunctionScalarSortDesc {
    type Error = ErrorCode;

    fn try_into(self) -> std::result::Result<AggregateFunctionSortDesc, Self::Error> {
        let expr = &self.expr;
        let ScalarExpr::BoundColumnRef(col) = expr else {
            return Err(ErrorCode::Internal(
                "Aggregate function sort description must be a BoundColumnRef".to_string(),
            ));
        };

        Ok(AggregateFunctionSortDesc {
            index: col.column.index,
            is_reuse_index: self.is_reuse_index,
            data_type: expr.data_type()?,
            nulls_first: self.nulls_first,
            asc: self.asc,
        })
    }
}

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct AggregateFunction {
    #[educe(PartialEq(ignore), Hash(ignore))]
    pub span: Span,
    pub func_name: String,
    pub distinct: bool,
    pub params: Vec<Scalar>,
    pub args: Vec<ScalarExpr>,
    pub return_type: Box<DataType>,
    pub sort_descs: Vec<AggregateFunctionScalarSortDesc>,

    pub display_name: String,
}

impl AggregateFunction {
    pub fn exprs(&self) -> impl Iterator<Item = &ScalarExpr> {
        self.args
            .iter()
            .chain(self.sort_descs.iter().map(|desc| &desc.expr))
    }

    pub fn exprs_mut(&mut self) -> impl Iterator<Item = &mut ScalarExpr> {
        self.args
            .iter_mut()
            .chain(self.sort_descs.iter_mut().map(|desc| &mut desc.expr))
    }
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
    pub ignore_null: bool,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct NtileFunction {
    pub n: u64,
    pub return_type: Box<DataType>,
}

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct WindowFunc {
    #[educe(PartialEq(ignore), Hash(ignore))]
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
pub struct LambdaFunc {
    #[educe(PartialEq(ignore), Hash(ignore))]
    pub span: Span,
    pub func_name: String,
    pub args: Vec<ScalarExpr>,
    pub lambda_expr: Box<RemoteExpr>,
    pub lambda_display: String,
    pub return_type: Box<DataType>,
}

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct FunctionCall {
    #[educe(Hash(ignore), PartialEq(ignore))]
    pub span: Span,
    pub func_name: String,
    pub params: Vec<Scalar>,
    pub arguments: Vec<ScalarExpr>,
}

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct CastExpr {
    #[educe(Hash(ignore), PartialEq(ignore))]
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
    #[educe(Hash(ignore), PartialEq(ignore))]
    pub span: Span,
    pub typ: SubqueryType,
    pub subquery: Box<SExpr>,
    // The expr that is used to compare the result of the subquery (IN/ANY/ALL), such as `t1.a in (select t2.a from t2)`, t1.a is `child_expr`.
    pub child_expr: Option<Box<ScalarExpr>>,
    // Comparison operator for Any/All, such as t1.a = Any (...), `compare_op` is `=`.
    pub compare_op: Option<SubqueryComparisonOp>,
    // Output column of Any/All and scalar subqueries.
    pub output_column: ColumnBinding,
    pub projection_index: Option<IndexType>,
    pub(crate) data_type: Box<DataType>,
    #[educe(Hash(method = "hash_column_set"))]
    pub outer_columns: ColumnSet,
    // If contain aggregation function in scalar subquery output
    pub contain_agg: Option<bool>,
}

impl SubqueryExpr {
    pub fn output_data_type(&self) -> DataType {
        match &self.typ {
            SubqueryType::Scalar => (*self.data_type).clone(),
            SubqueryType::Exists | SubqueryType::NotExists => DataType::Boolean,
            SubqueryType::Any | SubqueryType::All => {
                DataType::Nullable(Box::new(DataType::Boolean))
            }
        }
    }
}

fn hash_column_set<H: Hasher>(columns: &ColumnSet, state: &mut H) {
    columns.iter().for_each(|c| c.hash(state));
}

/// UDFCall includes script & lambda call
#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct UDFCall {
    #[educe(Hash(ignore), PartialEq(ignore))]
    pub span: Span,
    // name in meta
    pub name: String,
    // name in handler
    pub handler: String,
    pub headers: BTreeMap<String, String>,
    pub display_name: String,
    pub arg_types: Vec<DataType>,
    pub return_type: Box<DataType>,
    pub arguments: Vec<ScalarExpr>,
    pub udf_type: UDFType,
}

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct UDAFCall {
    #[educe(Hash(ignore), PartialEq(ignore))]
    pub span: Span,
    pub name: String, // name in meta
    pub display_name: String,
    pub arg_types: Vec<DataType>,
    pub state_fields: Vec<UDFField>,
    pub return_type: Box<DataType>,
    pub arguments: Vec<ScalarExpr>,
    pub udf_type: UDFType,
}

#[derive(Clone, Debug, Educe, serde::Serialize, serde::Deserialize)]
#[educe(PartialEq, Eq, Hash)]
pub struct UDFField {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum UDFLanguage {
    JavaScript,
    WebAssembly,
    Python,
}

impl FromStr for UDFLanguage {
    type Err = ErrorCode;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_lowercase().as_str() {
            "javascript" => Ok(Self::JavaScript),
            "wasm" => Ok(Self::WebAssembly),
            "python" => Ok(Self::Python),
            _ => Err(ErrorCode::BadArguments(format!(
                "Unsupported script language: {s}"
            ))),
        }
    }
}

impl Display for UDFLanguage {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            UDFLanguage::JavaScript => write!(f, "javascript"),
            UDFLanguage::WebAssembly => write!(f, "wasm"),
            UDFLanguage::Python => write!(f, "python"),
        }
    }
}

#[derive(Clone, Debug, Educe, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[educe(Hash(bound = false))]
pub struct UDFScriptCode {
    pub language: UDFLanguage,
    pub runtime_version: String,
    #[educe(Hash(ignore))]
    pub imports_stage_info: Vec<(StageInfo, String)>,
    pub imports: Vec<String>,
    pub packages: Vec<String>,
    pub code: Arc<Box<[u8]>>,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, serde::Serialize, serde::Deserialize, EnumAsInner)]
pub enum UDFType {
    Server(String), // server_addr
    Script(Box<UDFScriptCode>),
}

impl UDFType {
    pub fn match_type(&self, is_script: bool) -> bool {
        match self {
            UDFType::Server(_) => !is_script,
            UDFType::Script(_) => is_script,
        }
    }
}

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct UDFLambdaCall {
    #[educe(Hash(ignore), PartialEq(ignore))]
    pub span: Span,
    pub func_name: String,
    pub scalar: Box<ScalarExpr>,
}

// Different kinds of asynchronous functions have different arguments.
#[derive(Clone, Debug, Educe, serde::Serialize, serde::Deserialize)]
#[educe(PartialEq, Eq, Hash)]
pub enum AsyncFunctionArgument {
    // The argument of sequence function is sequence name.
    // Used by `nextval` function to call meta's `get_sequence_next_value` api
    // to get incremental values.
    SequenceFunction(String),
    // used for auto increment calling sequence
    AutoIncrement {
        key: AutoIncrementKey,
        expr: AutoIncrementExpr,
    },
    // The dictionary argument is connection URL of remote source, like Redis, MySQL ...
    // Used by `dict_get` function to connect source and read data.
    DictGetFunction(DictGetFunctionArgument),
    // Used by `read_file` function to read stage files.
    ReadFile(ReadFileFunctionArgument),
}

#[derive(Clone, Debug, Educe, serde::Serialize, serde::Deserialize)]
#[educe(PartialEq, Eq, Hash)]
pub struct ReadFileFunctionArgument {
    pub stage_name: Option<String>,
    #[educe(Hash(ignore), PartialEq(ignore))]
    pub stage_info: Option<Box<StageInfo>>,
}

#[derive(Clone, Debug, Educe, serde::Serialize, serde::Deserialize)]
#[educe(PartialEq, Eq, Hash)]
pub struct RedisSource {
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub db_index: Option<i64>,
}

impl Display for RedisSource {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "redis://")?;
        if let Some(username) = &self.username {
            write!(f, "{}:", username)?
        }
        if let Some(password) = &self.password {
            write!(f, "{}@", password)?;
        }
        write!(f, "{}:{}", self.host, self.port)?;
        if let Some(db_index) = &self.db_index {
            write!(f, "/{}", db_index)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Educe, serde::Serialize, serde::Deserialize)]
#[educe(PartialEq, Eq, Hash)]
pub struct SqlSource {
    // SQL source connection URL, like `mysql://user:password@localhost:3306/db`
    pub connection_url: String,
    pub table: String,
    pub key_field: String,
    pub value_field: String,
}

#[derive(Clone, Debug, Educe, EnumAsInner, serde::Serialize, serde::Deserialize)]
#[educe(PartialEq, Eq, Hash)]
pub enum DictionarySource {
    Mysql(SqlSource),
    Redis(RedisSource),
}

#[derive(Clone, Debug, Educe, serde::Serialize, serde::Deserialize)]
#[educe(PartialEq, Eq, Hash)]
pub struct DictGetFunctionArgument {
    pub dict_source: DictionarySource,
    pub default_value: Scalar,
}

// Asynchronous functions are functions that need to call remote interfaces.
#[derive(Clone, Debug, Educe)]
#[educe(PartialEq(bound(false)), Eq, Hash(bound(false)))]
pub struct AsyncFunctionCall {
    #[educe(Hash(ignore), PartialEq(ignore))]
    pub span: Span,
    pub func_name: String,
    pub display_name: String,
    pub return_type: Box<DataType>,
    pub arguments: Vec<ScalarExpr>,
    pub func_arg: AsyncFunctionArgument,
}

impl AsyncFunctionCall {
    pub async fn generate(
        &self,
        tenant: Tenant,
        catalog: Arc<dyn Catalog>,
        visibility_checker: Option<GrantObjectVisibilityChecker>,
    ) -> Result<Scalar> {
        match &self.func_arg {
            AsyncFunctionArgument::SequenceFunction(sequence_name) => {
                if let Some(visibility_checker) = &visibility_checker {
                    if !visibility_checker.check_seq_visibility(sequence_name) {
                        return Err(ErrorCode::PermissionDenied(format!(
                            "Permission denied: privilege ACCESS SEQUENCE is required on sequence {}",
                            sequence_name
                        )));
                    }
                }
                let req = GetSequenceNextValueReq {
                    ident: SequenceIdent::new(&tenant, sequence_name.clone()),
                    count: 1,
                };
                // Call meta's api to generate an incremental value.
                let reply = catalog
                    .get_sequence_next_value(req, &visibility_checker)
                    .await?;
                Ok(Scalar::Number(NumberScalar::UInt64(reply.start)))
            }
            AsyncFunctionArgument::AutoIncrement { key, expr } => {
                let req = GetAutoIncrementNextValueReq {
                    tenant,
                    expr: expr.clone(),
                    key: key.clone(),
                    count: 1,
                };
                // Call meta's api to generate an incremental value.
                let reply = catalog.get_autoincrement_next_value(req).await?;
                Ok(Scalar::Number(NumberScalar::UInt64(reply.start)))
            }
            AsyncFunctionArgument::DictGetFunction(_dict_get_function_argument) => {
                Err(ErrorCode::Internal("Cannot generate dict_get function"))
            }
            AsyncFunctionArgument::ReadFile(_) => {
                Err(ErrorCode::Internal("Cannot generate read_file function"))
            }
        }
    }
}

pub trait Visitor<'a>: Sized {
    fn visit(&mut self, expr: &'a ScalarExpr) -> Result<()> {
        walk_expr(self, expr)
    }

    fn visit_bound_column_ref(&mut self, _col: &'a BoundColumnRef) -> Result<()> {
        Ok(())
    }
    fn visit_constant(&mut self, _constant: &'a ConstantExpr) -> Result<()> {
        Ok(())
    }
    fn visit_window_function(&mut self, window: &'a WindowFunc) -> Result<()> {
        walk_window(self, window)
    }
    fn visit_aggregate_function(&mut self, aggregate: &'a AggregateFunction) -> Result<()> {
        for expr in aggregate.exprs() {
            self.visit(expr)?;
        }
        Ok(())
    }
    fn visit_lambda_function(&mut self, lambda: &'a LambdaFunc) -> Result<()> {
        for expr in &lambda.args {
            self.visit(expr)?;
        }
        Ok(())
    }
    fn visit_function_call(&mut self, func: &'a FunctionCall) -> Result<()> {
        for expr in &func.arguments {
            self.visit(expr)?;
        }
        Ok(())
    }
    fn visit_cast(&mut self, cast: &'a CastExpr) -> Result<()> {
        self.visit(&cast.argument)?;
        Ok(())
    }
    fn visit_subquery(&mut self, subquery: &'a SubqueryExpr) -> Result<()> {
        if let Some(child_expr) = subquery.child_expr.as_ref() {
            self.visit(child_expr)?;
        }
        Ok(())
    }
    fn visit_udf_call(&mut self, udf: &'a UDFCall) -> Result<()> {
        for expr in &udf.arguments {
            self.visit(expr)?;
        }
        Ok(())
    }

    fn visit_udf_lambda_call(&mut self, udf: &'a UDFLambdaCall) -> Result<()> {
        self.visit(&udf.scalar)
    }

    fn visit_udaf_call(&mut self, udaf: &'a UDAFCall) -> Result<()> {
        for expr in &udaf.arguments {
            self.visit(expr)?;
        }
        Ok(())
    }

    fn visit_async_function_call(&mut self, async_func: &'a AsyncFunctionCall) -> Result<()> {
        for expr in &async_func.arguments {
            self.visit(expr)?;
        }
        Ok(())
    }
}

#[recursive::recursive]
pub fn walk_expr<'a, V: Visitor<'a>>(visitor: &mut V, expr: &'a ScalarExpr) -> Result<()> {
    match expr {
        ScalarExpr::BoundColumnRef(expr) => visitor.visit_bound_column_ref(expr),
        ScalarExpr::ConstantExpr(expr) => visitor.visit_constant(expr),
        ScalarExpr::TypedConstantExpr(expr, _) => visitor.visit_constant(expr),
        ScalarExpr::WindowFunction(expr) => visitor.visit_window_function(expr),
        ScalarExpr::AggregateFunction(expr) => visitor.visit_aggregate_function(expr),
        ScalarExpr::LambdaFunction(expr) => visitor.visit_lambda_function(expr),
        ScalarExpr::FunctionCall(expr) => visitor.visit_function_call(expr),
        ScalarExpr::CastExpr(expr) => visitor.visit_cast(expr),
        ScalarExpr::SubqueryExpr(expr) => visitor.visit_subquery(expr),
        ScalarExpr::UDFCall(expr) => visitor.visit_udf_call(expr),
        ScalarExpr::UDFLambdaCall(expr) => visitor.visit_udf_lambda_call(expr),
        ScalarExpr::UDAFCall(expr) => visitor.visit_udaf_call(expr),
        ScalarExpr::AsyncFunctionCall(expr) => visitor.visit_async_function_call(expr),
    }
}

#[recursive::recursive]
pub fn walk_window<'a, V: Visitor<'a>>(visitor: &mut V, window: &'a WindowFunc) -> Result<()> {
    for expr in &window.partition_by {
        visitor.visit(expr)?;
    }
    for expr in &window.order_by {
        visitor.visit(&expr.expr)?;
    }
    match &window.func {
        WindowFuncType::Aggregate(func) => visitor.visit_aggregate_function(func)?,
        WindowFuncType::NthValue(func) => visitor.visit(&func.arg)?,
        WindowFuncType::LagLead(func) => {
            visitor.visit(&func.arg)?;
            if let Some(default) = func.default.as_ref() {
                visitor.visit(default)?
            }
        }
        WindowFuncType::RowNumber
        | WindowFuncType::CumeDist
        | WindowFuncType::Rank
        | WindowFuncType::DenseRank
        | WindowFuncType::PercentRank
        | WindowFuncType::Ntile(_) => (),
    }
    Ok(())
}

pub trait VisitorMut<'a>: Sized {
    fn visit(&mut self, expr: &'a mut ScalarExpr) -> Result<()> {
        walk_expr_mut(self, expr)
    }
    fn visit_bound_column_ref(&mut self, _col: &'a mut BoundColumnRef) -> Result<()> {
        Ok(())
    }
    fn visit_constant_expr(&mut self, _constant: &'a mut ConstantExpr) -> Result<()> {
        Ok(())
    }
    fn visit_window_function(&mut self, window: &'a mut WindowFunc) -> Result<()> {
        walk_window_mut(self, window)
    }
    fn visit_aggregate_function(&mut self, aggregate: &'a mut AggregateFunction) -> Result<()> {
        for expr in aggregate.exprs_mut() {
            self.visit(expr)?;
        }
        Ok(())
    }
    fn visit_lambda_function(&mut self, lambda: &'a mut LambdaFunc) -> Result<()> {
        for expr in &mut lambda.args {
            self.visit(expr)?;
        }
        Ok(())
    }
    fn visit_function_call(&mut self, func: &'a mut FunctionCall) -> Result<()> {
        for expr in &mut func.arguments {
            self.visit(expr)?;
        }
        Ok(())
    }
    fn visit_cast_expr(&mut self, cast: &'a mut CastExpr) -> Result<()> {
        self.visit(&mut cast.argument)?;
        Ok(())
    }
    fn visit_subquery_expr(&mut self, subquery: &'a mut SubqueryExpr) -> Result<()> {
        if let Some(child_expr) = subquery.child_expr.as_mut() {
            self.visit(child_expr)?;
        }
        Ok(())
    }
    fn visit_udf_call(&mut self, udf: &'a mut UDFCall) -> Result<()> {
        for expr in &mut udf.arguments {
            self.visit(expr)?;
        }
        Ok(())
    }

    fn visit_udf_lambda_call(&mut self, udf: &'a mut UDFLambdaCall) -> Result<()> {
        self.visit(&mut udf.scalar)
    }

    fn visit_udaf_call(&mut self, udaf: &'a mut UDAFCall) -> Result<()> {
        for expr in &mut udaf.arguments {
            self.visit(expr)?;
        }
        Ok(())
    }

    fn visit_async_function_call(&mut self, async_func: &'a mut AsyncFunctionCall) -> Result<()> {
        for expr in &mut async_func.arguments {
            self.visit(expr)?;
        }
        Ok(())
    }
}

#[recursive::recursive]
pub fn walk_expr_mut<'a, V: VisitorMut<'a>>(
    visitor: &mut V,
    expr: &'a mut ScalarExpr,
) -> Result<()> {
    match expr {
        ScalarExpr::BoundColumnRef(expr) => visitor.visit_bound_column_ref(expr),
        ScalarExpr::ConstantExpr(expr) => visitor.visit_constant_expr(expr),
        ScalarExpr::TypedConstantExpr(expr, _) => visitor.visit_constant_expr(expr),
        ScalarExpr::WindowFunction(expr) => visitor.visit_window_function(expr),
        ScalarExpr::AggregateFunction(expr) => visitor.visit_aggregate_function(expr),
        ScalarExpr::LambdaFunction(expr) => visitor.visit_lambda_function(expr),
        ScalarExpr::FunctionCall(expr) => visitor.visit_function_call(expr),
        ScalarExpr::CastExpr(expr) => visitor.visit_cast_expr(expr),
        ScalarExpr::SubqueryExpr(expr) => visitor.visit_subquery_expr(expr),
        ScalarExpr::UDFCall(expr) => visitor.visit_udf_call(expr),
        ScalarExpr::UDFLambdaCall(expr) => visitor.visit_udf_lambda_call(expr),
        ScalarExpr::UDAFCall(expr) => visitor.visit_udaf_call(expr),
        ScalarExpr::AsyncFunctionCall(expr) => visitor.visit_async_function_call(expr),
    }
}

#[recursive::recursive]
pub fn walk_window_mut<'a, V: VisitorMut<'a>>(
    visitor: &mut V,
    window: &'a mut WindowFunc,
) -> Result<()> {
    for expr in &mut window.partition_by {
        visitor.visit(expr)?;
    }
    for expr in &mut window.order_by {
        visitor.visit(&mut expr.expr)?;
    }
    match &mut window.func {
        WindowFuncType::Aggregate(func) => visitor.visit_aggregate_function(func)?,
        WindowFuncType::NthValue(func) => visitor.visit(&mut func.arg)?,
        WindowFuncType::LagLead(func) => {
            visitor.visit(&mut func.arg)?;
            if let Some(default) = func.default.as_mut() {
                visitor.visit(default)?
            }
        }
        WindowFuncType::RowNumber
        | WindowFuncType::CumeDist
        | WindowFuncType::Rank
        | WindowFuncType::DenseRank
        | WindowFuncType::PercentRank
        | WindowFuncType::Ntile(_) => (),
    }
    Ok(())
}

pub struct IndexPredicateChecker {
    allow_search_columns: bool,
    allow_vector_columns: bool,
    pub has_index_column: bool,
    pub valid: bool,
}

impl IndexPredicateChecker {
    pub fn new(allow_search_columns: bool, allow_vector_columns: bool) -> Self {
        Self {
            allow_search_columns,
            allow_vector_columns,
            has_index_column: false,
            valid: true,
        }
    }
}

impl<'a> Visitor<'a> for IndexPredicateChecker {
    fn visit(&mut self, expr: &'a ScalarExpr) -> Result<()> {
        if !self.valid {
            return Ok(());
        }
        walk_expr(self, expr)
    }

    fn visit_bound_column_ref(&mut self, column: &'a BoundColumnRef) -> Result<()> {
        let name = column.column.column_name.as_str();
        let is_search_column = name == SEARCH_MATCHED_COL_NAME || name == SEARCH_SCORE_COL_NAME;
        let is_vector_column = name == VECTOR_SCORE_COL_NAME;

        if (is_search_column && self.allow_search_columns)
            || (is_vector_column && self.allow_vector_columns)
        {
            self.has_index_column = true;
        } else {
            self.valid = false;
        }

        Ok(())
    }
}
