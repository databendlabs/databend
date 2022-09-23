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

use core::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use common_datavalues::prelude::*;
use common_datavalues::type_coercion::numerical_coercion;
use common_datavalues::with_match_physical_primitive_type;
use common_exception::ErrorCode;
use common_exception::Result;
use common_hashtable::HashSetWithStackMemory;
use common_hashtable::HashTableKeyable;
use common_hashtable::KeysRef;

use crate::scalars::default_column_cast;
use crate::scalars::AlwaysNullFunction;
use crate::scalars::Function;
use crate::scalars::FunctionContext;

#[derive(Clone)]
pub struct InEvalutorImpl<const NEGATED: bool, S: Scalar + Clone, Key: HashTableKeyable + Clone> {
    input_type: DataTypeImpl,
    nonull_least_super_dt: DataTypeImpl,
    set: Arc<HashSetWithStackMemory<64, Key>>,
    _col: ColumnRef,
    _s: PhantomData<S>,
}

pub fn create_by_column<const NEGATED: bool>(
    input_type: DataTypeImpl,
    col: ColumnRef,
) -> Result<Box<dyn Function>> {
    let mut nonull_least_super_dt = remove_nullable(&input_type);
    if nonull_least_super_dt.data_type_id().is_numeric()
        && col.data_type().data_type_id().is_numeric()
    {
        nonull_least_super_dt =
            numerical_coercion(&nonull_least_super_dt, &col.data_type(), true).unwrap();
    }

    if nonull_least_super_dt.data_type_id() == TypeID::Boolean {
        nonull_least_super_dt = UInt8Type::new_impl();
    }

    let type_id = nonull_least_super_dt.data_type_id().to_physical_type();
    if type_id == PhysicalTypeID::Null {
        return Ok(Box::new(AlwaysNullFunction {}));
    }

    with_match_physical_primitive_type!(type_id, |$T| {
        InEvalutorImpl::<NEGATED, $T, <$T as Scalar>::KeyType>::try_create(input_type, nonull_least_super_dt, col)
    },
    {
        if type_id == PhysicalTypeID::String {
            return InEvalutorImpl::<NEGATED, Vec<u8>, KeysRef>::try_create(input_type, nonull_least_super_dt, col);
        }

        Err(ErrorCode::BadDataValueType(format!(
            "Column with type: {:?} does not support in function",
            type_id
        )))
    })
}

pub fn create_by_values<const NEGATED: bool>(
    input_type: DataTypeImpl,
    values: Vec<DataValue>,
) -> Result<Box<dyn Function>> {
    debug_assert!(!values.is_empty());

    let mut cols = Vec::with_capacity(values.len());
    let mut types = Vec::with_capacity(values.len());

    for value in values {
        if value.is_null() {
            continue;
        }
        types.push(value.data_type());
        let col = value.data_type().create_column(&[value])?;
        cols.push(col);
    }

    let mut nonull_least_super_dt = remove_nullable(&input_type);
    // may have precision loss if contains float
    if nonull_least_super_dt.data_type_id().is_numeric() {
        for col in cols.iter() {
            if col.data_type().data_type_id().is_numeric() {
                nonull_least_super_dt =
                    numerical_coercion(&nonull_least_super_dt, &col.data_type(), true).unwrap();
            }
        }
    }

    let cols = cols
        .iter()
        .map(|c| default_column_cast(c, &nonull_least_super_dt))
        .collect::<Result<Vec<ColumnRef>>>()?;
    let col = Series::concat(&cols)?;
    create_by_column::<NEGATED>(input_type, col)
}

impl<const NEGATED: bool, S, Key> Function for InEvalutorImpl<NEGATED, S, Key>
where
    S: Scalar<KeyType = Key> + Sync + Send + Clone,
    Key: HashTableKeyable + Clone + 'static + Sync + Send,
{
    fn name(&self) -> &str {
        if NEGATED { "NOT IN" } else { "IN" }
    }

    fn return_type(&self) -> DataTypeImpl {
        let t = BooleanType::new_impl();
        if self.input_type.is_nullable() {
            NullableType::new_impl(t)
        } else {
            t
        }
    }

    fn eval(
        &self,
        func_ctx: FunctionContext,
        columns: &ColumnsWithField,
        input_rows: usize,
    ) -> Result<ColumnRef> {
        if self.return_type().is_null() {
            return Ok(NullColumn::new(input_rows).arc());
        }

        let column = columns[0].column();

        if column.is_const() {
            let column: &ConstColumn = Series::check_get(column)?;
            let cf: ColumnWithField =
                ColumnWithField::new(column.inner().clone(), columns[0].field().clone());
            let inner = self.eval(func_ctx, &[cf], 1)?;
            return Ok(ConstColumn::new(inner, input_rows).arc());
        }

        if column.is_nullable() {
            let column: &NullableColumn = Series::check_get(column)?;
            let cf: ColumnWithField =
                ColumnWithField::new(column.inner().clone(), columns[0].field().clone());
            let inner = self.eval(func_ctx, &[cf], input_rows)?;
            let result = NullableColumn::wrap_inner(inner, Some(column.ensure_validity().clone()));
            return Ok(result);
        }
        self.eval_nonull(func_ctx, column, input_rows)
    }
}

impl<const NEGATED: bool, S, Key> InEvalutorImpl<NEGATED, S, Key>
where
    S: Scalar<KeyType = Key> + Clone + Sync + Send,
    Key: HashTableKeyable + Clone + Send + Sync + 'static,
{
    fn try_create(
        input_type: DataTypeImpl,
        nonull_least_super_dt: DataTypeImpl,
        col: ColumnRef,
    ) -> Result<Box<dyn Function>> {
        let col = default_column_cast(&col, &nonull_least_super_dt)?;
        let col = col.convert_full_column();
        let c = Series::check_get_scalar::<S>(&col)?;

        let mut set = HashSetWithStackMemory::with_capacity(c.len());
        let mut inserted = false;
        for data in c.scalar_iter() {
            set.insert_key(&data.to_key(), &mut inserted);
        }
        Ok(Box::new(Self {
            input_type,
            nonull_least_super_dt,
            set: Arc::new(set),
            _col: col,
            _s: PhantomData,
        }))
    }

    fn eval_nonull(
        &self,
        _func_ctx: FunctionContext,
        column: &ColumnRef,
        _input_rows: usize,
    ) -> Result<ColumnRef> {
        let col = default_column_cast(column, &self.nonull_least_super_dt)?;

        let c = Series::check_get_scalar::<S>(&col)?;
        let set = self.set.as_ref();

        if NEGATED {
            let it = c.scalar_iter().map(|item| !set.contains(&item.to_key()));
            Ok(BooleanColumn::from_iterator(it).arc())
        } else {
            let it = c.scalar_iter().map(|item| set.contains(&item.to_key()));
            Ok(BooleanColumn::from_iterator(it).arc())
        }
    }
}

impl<const NEGATED: bool, S, Key> fmt::Display for InEvalutorImpl<NEGATED, S, Key>
where
    S: Clone + Scalar<KeyType = Key> + Sync + Send,
    Key: HashTableKeyable + Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if NEGATED {
            write!(f, "NOT IN")
        } else {
            write!(f, "IN")
        }
    }
}
