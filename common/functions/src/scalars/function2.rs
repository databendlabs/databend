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

use common_datavalues::prelude::DataColumn;
use common_datavalues::prelude::DataField as OldDataField;
use common_datavalues::DataTypeAndNullable;
use common_datavalues2::column_convert::convert2_new_column;
use common_datavalues2::column_convert::convert2_old_column;
use common_datavalues2::ColumnRef;
use common_datavalues2::ColumnWithField;
use common_datavalues2::ColumnsWithField;
use common_datavalues2::DataField;
use common_datavalues2::DataTypePtr;
use common_exception::Result;
use dyn_clone::DynClone;

use super::Function;
use super::Monotonicity;
use super::Monotonicity2;

pub trait Function2: fmt::Display + Sync + Send + DynClone {
    /// Returns the name of the function, should be unique.
    fn name(&self) -> &str;

    /// Calculate the monotonicity from arguments' monotonicity information.
    /// The input should be argument's monotonicity. For binary function it should be an
    /// array of left expression's monotonicity and right expression's monotonicity.
    /// For unary function, the input should be an array of the only argument's monotonicity.
    /// The returned monotonicity should have 'left' and 'right' fields None -- the boundary
    /// calculation relies on the function.eval method.
    fn get_monotonicity(&self, _args: &[Monotonicity2]) -> Result<Monotonicity2> {
        Ok(Monotonicity2::default())
    }

    /// The method returns the return_type of this function.
    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr>;

    /// Evaluate the function, e.g. run/execute the function.
    fn eval(&self, _columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef>;

    /// Whether the function passes through null input.
    /// Return true if the function just return null with any given null input.
    /// Return false if the function may return non-null with null input.
    ///
    /// For example, arithmetic plus('+') will output null for any null input, like '12 + null = null'.
    /// It has no idea of how to handle null, but just pass through.
    ///
    /// While ISNULL function  treats null input as a valid one. For example ISNULL(NULL, 'test') will return 'test'.
    fn passthrough_null(&self) -> bool {
        true
    }

    /// If all args are constant column, then we just return the constant result
    /// TODO, we should cache the constant result inside the context for better performance
    fn passthrough_constant(&self) -> bool {
        true
    }
}

dyn_clone::clone_trait_object!(Function2);

#[derive(Clone)]
pub struct Function2Convertor {
    inner: Box<dyn Function2>,
}

impl Function2Convertor {
    pub fn create(inner: Box<dyn Function2>) -> Box<dyn Function> {
        Box::new(Self { inner })
    }
}
impl Function for Function2Convertor {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn return_type(
        &self,
        args: &[common_datavalues::DataTypeAndNullable],
    ) -> Result<common_datavalues::DataTypeAndNullable> {
        let args = args
            .iter()
            .map(|arg| OldDataField::new("xx", arg.data_type().clone(), arg.is_nullable()))
            .collect::<Vec<_>>();

        let mut types = vec![];
        let fs: Vec<DataField> = args.iter().map(|f| f.clone().into()).collect();
        for t in fs.iter() {
            types.push(t.data_type());
        }

        let typ = self.inner.return_type(&types)?;
        let new_typ = DataField::new("xx", typ);
        let old_f: OldDataField = new_typ.into();

        Ok(DataTypeAndNullable::create(
            old_f.data_type(),
            old_f.is_nullable(),
        ))
    }

    fn eval(
        &self,
        columns: &common_datavalues::prelude::DataColumnsWithField,
        input_rows: usize,
    ) -> Result<DataColumn> {
        let columns: Vec<ColumnWithField> =
            columns.iter().map(convert2_new_column).collect::<Vec<_>>();
        let col = self.inner.eval(&columns, input_rows)?;
        let column = convert2_old_column(&col);
        Ok(column)
    }

    fn get_monotonicity(&self, args: &[Monotonicity]) -> Result<Monotonicity> {
        let args: Vec<Monotonicity2> = args.iter().map(|m| m.clone().into()).collect();
        let m = self.inner.get_monotonicity(&args)?;

        Ok(m.into())
    }

    fn passthrough_null(&self) -> bool {
        self.inner.passthrough_null()
    }
}

impl std::fmt::Display for Function2Convertor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

/// convert function into function2
#[derive(Clone)]
pub struct Function1Convertor {
    inner: Box<dyn Function>,
}

impl Function1Convertor {
    pub fn create(inner: Box<dyn Function>) -> Box<dyn Function2> {
        Box::new(Self { inner })
    }
}
impl Function2 for Function1Convertor {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        let args = args
            .iter()
            .map(|arg| DataField::new("xx", (*arg).clone()))
            .collect::<Vec<_>>();

        let mut types = vec![];
        let fs: Vec<OldDataField> = args.iter().map(|f| f.clone().into()).collect();
        for t in fs.iter() {
            types.push(DataTypeAndNullable::create(t.data_type(), t.is_nullable()));
        }

        let typ = self.inner.return_type(&types)?;

        let new_typ = DataField::new("xx", typ);
        let old_f: OldDataField = new_typ.into();

        Ok(DataTypeAndNullable::create(
            old_f.data_type(),
            old_f.is_nullable(),
        ))
    }

    fn eval(&self, _columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        todo!()
    }

    fn get_monotonicity(&self, args: &[Monotonicity2]) -> Result<Monotonicity2> {
        let args: Vec<Monotonicity> = args.iter().map(|m| m.clone().into()).collect();
        let m = self.inner.get_monotonicity(&args)?;
        Ok(m.into())
    }

    fn passthrough_null(&self) -> bool {
        true
    }

    fn passthrough_constant(&self) -> bool {
        true
    }
}

impl std::fmt::Display for Function1Convertor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}
