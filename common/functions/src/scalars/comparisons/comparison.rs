// Copyright 2021 Datafuse Labs.
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

use std::fmt;
use std::sync::Arc;

use common_arrow::arrow::compute::comparison;
use common_datavalues::prelude::*;
use common_datavalues::type_coercion::compare_coercion;
use common_datavalues::BooleanType;
use common_datavalues::DataValueComparisonOperator;
use common_datavalues::TypeID;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::cast_column_field;
use crate::scalars::ComparisonEqFunction;
use crate::scalars::ComparisonGtEqFunction;
use crate::scalars::ComparisonGtFunction;
use crate::scalars::ComparisonLikeFunction;
use crate::scalars::ComparisonLtEqFunction;
use crate::scalars::ComparisonLtFunction;
use crate::scalars::ComparisonNotEqFunction;
use crate::scalars::ComparisonRegexpFunction;
use crate::scalars::Function;
use crate::scalars::FunctionFactory;

#[derive(Clone)]
pub struct ComparisonFunction {
    op: DataValueComparisonOperator,
}

impl ComparisonFunction {
    pub fn register(factory: &mut FunctionFactory) {
        factory.register("=", ComparisonEqFunction::desc());
        factory.register("<", ComparisonLtFunction::desc());
        factory.register(">", ComparisonGtFunction::desc());
        factory.register("<=", ComparisonLtEqFunction::desc());
        factory.register(">=", ComparisonGtEqFunction::desc());
        factory.register("!=", ComparisonNotEqFunction::desc());
        factory.register("<>", ComparisonNotEqFunction::desc());
        factory.register("like", ComparisonLikeFunction::desc_like());
        factory.register("not like", ComparisonLikeFunction::desc_unlike());
        factory.register("regexp", ComparisonRegexpFunction::desc_regexp());
        factory.register("not regexp", ComparisonRegexpFunction::desc_unregexp());
        factory.register("rlike", ComparisonRegexpFunction::desc_regexp());
        factory.register("not rlike", ComparisonRegexpFunction::desc_unregexp());
    }

    pub fn try_create_func(op: DataValueComparisonOperator) -> Result<Box<dyn Function>> {
        Ok(Box::new(ComparisonFunction { op }))
    }
}

impl Function for ComparisonFunction {
    fn name(&self) -> &str {
        "ComparisonFunction"
    }

    fn return_type(
        &self,
        args: &[&common_datavalues::DataTypePtr],
    ) -> Result<common_datavalues::DataTypePtr> {
        // expect array & struct
        let has_array_struct = args
            .iter()
            .any(|arg| matches!(arg.data_type_id(), TypeID::Struct | TypeID::Array));

        if has_array_struct {
            return Err(ErrorCode::BadArguments(format!(
                "Illegal types {:?} of argument of function {}, can not be struct or array",
                args,
                self.name()
            )));
        }

        Ok(BooleanType::arc())
    }

    fn eval(
        &self,
        columns: &common_datavalues::ColumnsWithField,
        input_rows: usize,
    ) -> Result<common_datavalues::ColumnRef> {
        if columns[0].data_type() != columns[1].data_type() {
            // TODO cached it inside the function
            let least_supertype = compare_coercion(columns[0].data_type(), columns[1].data_type())?;
            let col0 = cast_column_field(&columns[0], &least_supertype)?;
            let col1 = cast_column_field(&columns[1], &least_supertype)?;

            let f0 = DataField::new(columns[0].field().name(), least_supertype.clone());
            let f1 = DataField::new(columns[1].field().name(), least_supertype.clone());

            let columns = vec![
                ColumnWithField::new(col0, f0),
                ColumnWithField::new(col1, f1),
            ];
            return self.eval(&columns, input_rows);
        }

        // TODO, this already convert to full column
        // Better to use comparator with scalar of arrow2

        let array0 = columns[0].column().as_arrow_array();
        let array1 = columns[1].column().as_arrow_array();

        let f = match self.op {
            DataValueComparisonOperator::Eq => comparison::eq,
            DataValueComparisonOperator::Lt => comparison::lt,
            DataValueComparisonOperator::LtEq => comparison::lt_eq,
            DataValueComparisonOperator::Gt => comparison::gt,
            DataValueComparisonOperator::GtEq => comparison::gt_eq,
            DataValueComparisonOperator::NotEq => comparison::neq,
            _ => unreachable!(),
        };

        let result = f(array0.as_ref(), array1.as_ref());
        let result = BooleanColumn::new(result);
        Ok(Arc::new(result))
    }
}

impl fmt::Display for ComparisonFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.op)
    }
}
