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

#[macro_export]
macro_rules! impl_try_create_datetime {
    ($op: expr, $func: ident, $add: expr) => {
        fn try_create_datetime(
            lhs_type: &DataType,
            rhs_type: &DataType,
        ) -> Result<Box<dyn Function>> {
            let op = $op;
            let e = Result::Err(ErrorCode::BadDataValueType(format!(
                "DataValue Error: Unsupported date coercion ({:?}) {} ({:?})",
                lhs_type, op, rhs_type
            )));

            if lhs_type.is_date_or_date_time() {
                with_match_date_type!(lhs_type, |$T| {
                    with_match_primitive_type!(rhs_type, |$D| {
                        BinaryArithmeticFunction::<$func<$T, $D, $T>>::try_create_func(
                            op.clone(),
                            lhs_type.clone(),
                        )
                    },{
                        with_match_date_type!(rhs_type, |$D| {
                            if $add {
                                BinaryArithmeticFunction::<$func<$T, $D, $T>>::try_create_func(
                                    op.clone(),
                                    lhs_type.clone(),
                                )
                            } else {
                                BinaryArithmeticFunction::<$func<$T, $D, i32>>::try_create_func(
                                    op.clone(),
                                    DataType::Int32,
                                )
                            }
                        }, e)
                    })
                },e)
            } else {
                with_match_primitive_type!(lhs_type, |$T| {
                    with_match_date_type!(rhs_type, |$D| {
                        BinaryArithmeticFunction::<$func<$T, $D, $D>>::try_create_func(
                            op.clone(),
                            rhs_type.clone(),
                        )
                    },e)
                },e)
            }
        }
    };
}

#[macro_export]
macro_rules! impl_unary_arith {
    ($name: ident, $method: tt) => {
        #[derive(Clone)]
        pub struct $name<T, R> {
            t: PhantomData<T>,
            r: PhantomData<R>,
        }

        impl<T, R> ArithmeticTrait for $name<T, R>
        where
            T: DFPrimitiveType + AsPrimitive<R>,
            R: DFPrimitiveType + Neg<Output = R>,
            DFPrimitiveArray<R>: IntoSeries,
        {
            fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
                unary_arithmetic!(columns[0].column(), |v: R| $method v)
            }
        }
    };
}

#[macro_export]
macro_rules! impl_wrapping_unary_arith {
    ($name: ident, $method: ident) => {
        #[derive(Clone)]
        pub struct $name<T, R> {
            t: PhantomData<T>,
            r: PhantomData<R>,
        }

        impl<T, R> ArithmeticTrait for $name<T, R>
        where
            T: DFPrimitiveType + AsPrimitive<R>,
            R: DFIntegerType + WrappingNeg,
            DFPrimitiveArray<R>: IntoSeries,
        {
            fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
                unary_arithmetic!(columns[0].column(), |v: R| v.$method())
            }
        }
    };
}

#[macro_export]
macro_rules! unary_arithmetic {
    ($self: expr, $op: expr) => {{
        let result: DataColumn = match $self {
            DataColumn::Array(series) => {
                let array: &DFPrimitiveArray<T> = series.static_cast();
                unary(array, |v| $op(v.as_())).into()
            }
            DataColumn::Constant(val, size) => {
                let v: T = DFTryFrom::try_from(val.clone()).unwrap_or(T::default());
                DataColumn::Constant($op(v.as_()).into(), size.clone())
            }
        };

        Ok(result)
    }};
}

#[macro_export]
macro_rules! impl_wrapping_binary_arith {
    ($name: ident, $method: ident) => {
        #[derive(Clone)]
        pub struct $name<T, D, R> {
            t: PhantomData<T>,
            d: PhantomData<D>,
            r: PhantomData<R>,
        }

        impl<T, D, R> ArithmeticTrait for $name<T, D, R>
        where
            T: DFPrimitiveType + AsPrimitive<R>,
            D: DFPrimitiveType + AsPrimitive<R>,
            R: DFIntegerType
                + WrappingAdd<Output = R>
                + WrappingSub<Output = R>
                + WrappingMul<Output = R>,
            DFPrimitiveArray<R>: IntoSeries,
        {
            fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
                binary_arithmetic!(columns[0].column(), columns[1].column(), R, |l: R, r: R| l
                    .$method(&r))
            }
        }
    };
}

#[macro_export]
macro_rules! impl_binary_arith {
    ($name: ident, $method: tt) => {
        #[derive(Clone)]
        pub struct $name<T, D, R> {
            t: PhantomData<T>,
            d: PhantomData<D>,
            r: PhantomData<R>,
        }

        impl<T, D, R> ArithmeticTrait for $name<T, D, R>
        where
            T: DFPrimitiveType + AsPrimitive<R>,
            D: DFPrimitiveType + AsPrimitive<R>,
            R: DFPrimitiveType + Add<Output = R> + Sub<Output = R> + Mul<Output = R>,
            DFPrimitiveArray<R>: IntoSeries,
        {
            fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
                binary_arithmetic!(columns[0].column(), columns[1].column(), R, |l: R, r: R| l
                $method r)
            }
        }
    };
}

#[macro_export]
macro_rules! with_match_date_type {
    (
         $key_type:expr, | $_:tt $T:ident | $body:tt,  $nbody:tt
     ) => {{
        macro_rules! __with_ty__ {
            ( $_ $T:ident ) => {
                $body
            };
        }

        match $key_type {
            DataType::Date16 => __with_ty__! { u16},
            DataType::Date32 => __with_ty__! { i32},
            DataType::DateTime32(_) => __with_ty__! { u32},

            _ => $nbody,
        }
    }};
}

#[macro_export]
macro_rules! binary_arithmetic {
    ($lhs: expr, $rhs: expr, $R:ty, $op: expr) => {{
        let result: DataColumn = match ($lhs, $rhs) {
            (DataColumn::Array(left), DataColumn::Array(right)) => {
                let lhs: &DFPrimitiveArray<T> = left.static_cast();
                let rhs: &DFPrimitiveArray<D> = right.static_cast();
                binary(lhs, rhs, |l, r| $op(l.as_(), r.as_())).into()
            }
            (DataColumn::Array(left), DataColumn::Constant(right, _)) => {
                let lhs: &DFPrimitiveArray<T> = left.static_cast();
                let rhs: D = DFTryFrom::try_from(right.clone()).unwrap_or(D::default());
                let r: $R = rhs.as_();
                unary(lhs, |l| $op(l.as_(), r)).into()
            }
            (DataColumn::Constant(left, _), DataColumn::Array(right)) => {
                let lhs: T = DFTryFrom::try_from(left.clone()).unwrap_or(T::default());
                let l: $R = lhs.as_();
                let rhs: &DFPrimitiveArray<D> = right.static_cast();
                unary(rhs, |r| $op(l, r.as_())).into()
            }
            (DataColumn::Constant(left, size), DataColumn::Constant(right, _)) => {
                let l: T = DFTryFrom::try_from(left.clone()).unwrap_or(T::default());
                let r: D = DFTryFrom::try_from(right.clone()).unwrap_or(D::default());
                DataColumn::Constant($op(l.as_(), r.as_()).into(), size.clone())
            }
        };

        Ok(result)
    }};
}

#[macro_export]
macro_rules! interval_arithmetic {
    ($self: expr, $rhs: expr, $R:ty, $op: expr) => {{
        let (interval, datetime) = validate_input($self, $rhs);
        let result: DataColumn = match (datetime.column(), interval.column()) {
            (DataColumn::Array(left), DataColumn::Array(right)) => {
                let lhs: &DFPrimitiveArray<$R> = left.static_cast();
                let rhs = right.i64()?;
                binary(lhs, rhs, |l, r| $op(l.as_(), r)).into()
            }
            (DataColumn::Array(left), DataColumn::Constant(right, _)) => {
                let lhs: &DFPrimitiveArray<$R> = left.static_cast();
                let r: i64 = DFTryFrom::try_from(right.clone()).unwrap_or(0i64);
                unary(lhs, |l| $op(l.as_(), r)).into()
            }
            (DataColumn::Constant(left, _), DataColumn::Array(right)) => {
                let lhs: $R = DFTryFrom::try_from(left.clone()).unwrap_or(<$R>::default());
                let l: i64 = lhs.as_();
                let rhs = right.i64()?;
                unary(rhs, |r| $op(l, r)).into()
            }
            (DataColumn::Constant(left, size), DataColumn::Constant(right, _)) => {
                let l: $R = DFTryFrom::try_from(left.clone()).unwrap_or(<$R>::default());
                let r: i64 = DFTryFrom::try_from(right.clone()).unwrap_or(0i64);
                DataColumn::Constant($op(l.as_(), r).into(), size.clone())
            }
        };

        Ok(result)
    }};
}

#[macro_export]
macro_rules! try_binary_arithmetic {
    ($lhs: expr, $rhs: expr, $M: ty, $op: expr, $scalar:expr) => {{
        let result: DataColumn = match ($lhs, $rhs) {
            (DataColumn::Array(left), DataColumn::Array(right)) => {
                let lhs: &DFPrimitiveArray<T> = left.static_cast();
                let rhs: &DFPrimitiveArray<D> = right.static_cast();
                try_binary(lhs, rhs, |l, r| $op(l.as_(), r.as_()))?.into()
            }
            (DataColumn::Array(left), DataColumn::Constant(right, _)) => {
                let lhs: &DFPrimitiveArray<T> = left.static_cast();
                let rhs: D = DFTryFrom::try_from(right.clone()).unwrap_or(D::one());
                $scalar(lhs, rhs.as_())?
            }
            (DataColumn::Constant(left, _), DataColumn::Array(right)) => {
                let lhs: T = DFTryFrom::try_from(left.clone()).unwrap_or(T::default());
                let l: $M = lhs.as_();
                let rhs: &DFPrimitiveArray<D> = right.static_cast();
                try_unary(rhs, |r| $op(l, r.as_()))?.into()
            }
            (DataColumn::Constant(left, size), DataColumn::Constant(right, _)) => {
                let lhs: T = DFTryFrom::try_from(left.clone()).unwrap_or(T::default());
                let rhs: D = DFTryFrom::try_from(right.clone()).unwrap_or(D::one());
                let value = $op(lhs.as_(), rhs.as_())?;
                DataColumn::Constant(value.into(), size.clone())
            }
        };

        Ok(result)
    }};
}
