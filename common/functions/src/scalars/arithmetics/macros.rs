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
    ($op: expr, $func: ident) => {
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
                        if !rhs_type.is_date_or_date_time() {
                            return e;
                        }
                        with_match_date_type!(rhs_type, |$D| {
                            match op {
                                DataValueArithmeticOperator::Plus => BinaryArithmeticFunction::<ArithmeticAdd<$T, $D, $T>>::try_create_func(
                                    op.clone(),
                                    lhs_type.clone(),
                                ),
                                DataValueArithmeticOperator::Minus => BinaryArithmeticFunction::<ArithmeticAdd<$T, $D, i32>>::try_create_func(
                                    op.clone(),
                                    DataType::Int32,
                                ),
                                _ => e,
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
macro_rules! impl_try_create_func {
    ($lhs: ident, $rhs: ident, $op: expr, $is_signed: tt, $func: ident, $wrapping_func: ident) => {{
        let op = $op;
        let e = Result::Err(ErrorCode::BadDataValueType(format!(
            "DataValue Error: Unsupported arithmetic ({:?}) {} ({:?})",
            $lhs, op, $rhs
        )));

        if !$lhs.is_numeric() || !$rhs.is_numeric() {
            return e;
        };

        match ($lhs, $rhs) {
            (DataType::Float64, _) => with_match_primitive_type!(
                $rhs,
                |$D| {
                    BinaryArithmeticFunction::<$func<f64, $D, f64>>::try_create_func(
                        op,
                        DataType::Float64,
                    )
                },
                e
            ),
            (DataType::Float32, _) => with_match_primitive_type!(
                $rhs,
                |$D| {
                    BinaryArithmeticFunction::<$func<f32, $D, f64>>::try_create_func(
                        op,
                        DataType::Float64,
                    )
                },
                e
            ),
            (_, DataType::Float64) => with_match_integer_64!(
                $lhs,
                |$T| {
                    BinaryArithmeticFunction::<$func<$T, f64, f64>>::try_create_func(
                        op,
                        DataType::Float64,
                    )
                },
                e
            ),
            (_, DataType::Float32) => with_match_integer_64!(
                $lhs,
                |$T| {
                    BinaryArithmeticFunction::<$func<$T, f32, f64>>::try_create_func(
                        op,
                        DataType::Float64,
                    )
                },
                e
            ),
            (DataType::Int64, _) => with_match_integer_64!(
                $rhs,
                |$D| {
                    BinaryArithmeticFunction::<$wrapping_func<i64, $D, i64>>::try_create_func(
                        op,
                        DataType::Int64,
                    )
                },
                e
            ),
            (DataType::UInt64, _) => with_match_integer_64!(
                $rhs,
                |$D| {
                    if $is_signed {
                        BinaryArithmeticFunction::<$wrapping_func<u64, $D, i64>>::try_create_func(
                            op,
                            DataType::Int64,
                        )
                    } else {
                        BinaryArithmeticFunction::<$wrapping_func<u64, $D, u64>>::try_create_func(
                            op,
                            DataType::UInt64,
                        )
                    }
                },
                e
            ),
            (_, DataType::UInt64) => with_match_integer_32!(
                $lhs,
                |$T| {
                    if $is_signed {
                        BinaryArithmeticFunction::<$wrapping_func<$T, u64, i64>>::try_create_func(
                            op,
                            DataType::Int64,
                        )
                    } else {
                        BinaryArithmeticFunction::<$wrapping_func<$T, u64, u64>>::try_create_func(
                            op,
                            DataType::UInt64,
                        )
                    }
                },
                e
            ),
            (_, DataType::Int64) => with_match_integer_32!(
                $lhs,
                |$T| {
                    BinaryArithmeticFunction::<$wrapping_func<$T, i64, i64>>::try_create_func(
                        op,
                        DataType::Int64,
                    )
                },
                e
            ),
            (DataType::UInt32, _) => with_match_integer_32!(
                $rhs,
                |$D| {
                    if $is_signed {
                        BinaryArithmeticFunction::<$wrapping_func<u32, $D, i64>>::try_create_func(
                            op,
                            DataType::Int64,
                        )
                    } else {
                        BinaryArithmeticFunction::<$wrapping_func<u32, $D, u64>>::try_create_func(
                            op,
                            DataType::UInt64,
                        )
                    }
                },
                e
            ),
            (DataType::Int32, _) => with_match_integer_32!(
                $rhs,
                |$D| {
                    BinaryArithmeticFunction::<$wrapping_func<i32, $D, i64>>::try_create_func(
                        op,
                        DataType::Int64,
                    )
                },
                e
            ),
            (_, DataType::UInt32) => with_match_integer_16!(
                $lhs,
                |$T| {
                    if $is_signed {
                        BinaryArithmeticFunction::<$wrapping_func<$T, u32, i64>>::try_create_func(
                            op,
                            DataType::Int64,
                        )
                    } else {
                        BinaryArithmeticFunction::<$wrapping_func<$T, u32, u64>>::try_create_func(
                            op,
                            DataType::UInt64,
                        )
                    }
                },
                e
            ),
            (_, DataType::Int32) => with_match_integer_16!(
                $lhs,
                |$T| {
                    BinaryArithmeticFunction::<$wrapping_func<$T, i32, i64>>::try_create_func(
                        op,
                        DataType::Int64,
                    )
                },
                e
            ),
            (DataType::UInt16, _) => with_match_integer_16!(
                $rhs,
                |$D| {
                    if $is_signed {
                        BinaryArithmeticFunction::<$func<u16, $D, i32>>::try_create_func(
                            op,
                            DataType::Int32,
                        )
                    } else {
                        BinaryArithmeticFunction::<$func<u16, $D, u32>>::try_create_func(
                            op,
                            DataType::UInt32,
                        )
                    }
                },
                e
            ),
            (DataType::Int16, _) => with_match_integer_16!(
                $rhs,
                |$D| {
                    BinaryArithmeticFunction::<$func<i16, $D, i32>>::try_create_func(
                        op,
                        DataType::Int32,
                    )
                },
                e
            ),
            (_, DataType::UInt16) => match $lhs {
                DataType::UInt8 => {
                    BinaryArithmeticFunction::<$func<u8, u16, u32>>::try_create_func(
                        op,
                        DataType::UInt32,
                    )
                }
                DataType::Int8 => BinaryArithmeticFunction::<$func<i8, u16, i32>>::try_create_func(
                    op,
                    DataType::Int32,
                ),
                _ => unreachable!(),
            },
            (_, DataType::Int16) => with_match_integer_8!(
                $lhs,
                |$T| {
                    BinaryArithmeticFunction::<$func<$T, i16, i32>>::try_create_func(
                        op,
                        DataType::Int32,
                    )
                },
                e
            ),
            (DataType::UInt8, _) => match $rhs {
                DataType::UInt8 => BinaryArithmeticFunction::<$func<u8, u8, u16>>::try_create_func(
                    op,
                    DataType::UInt16,
                ),
                DataType::Int8 => BinaryArithmeticFunction::<$func<i8, i8, i16>>::try_create_func(
                    op,
                    DataType::Int16,
                ),
                _ => unreachable!(),
            },
            (DataType::Int8, _) => with_match_integer_8!(
                $rhs,
                |$D| {
                    BinaryArithmeticFunction::<$func<i8, $D, i16>>::try_create_func(
                        op,
                        DataType::Int16,
                    )
                },
                e
            ),
            _ => unreachable!(),
        }
    }};
}

#[macro_export]
macro_rules! impl_wrapping_arithmetic {
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
            R: DFIntegerType + WrappingAdd<Output = R> + WrappingSub<Output = R>,
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
macro_rules! impl_arithmetic {
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
            R: DFPrimitiveType + Add<Output = R>,
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
macro_rules! with_match_integer_8 {
    (
         $key_type:expr, | $_:tt $T:ident | $body:tt,  $nbody:tt
     ) => {{
        macro_rules! __with_ty__ {
            ( $_ $T:ident ) => {
                $body
            };
        }

        match $key_type {
            DataType::Int8 => __with_ty__! { i8},
            DataType::UInt8 => __with_ty__! { u8},

            _ => $nbody,
        }
    }};
}

#[macro_export]
macro_rules! with_match_integer_16 {
    (
         $key_type:expr, | $_:tt $T:ident | $body:tt,  $nbody:tt
     ) => {{
        macro_rules! __with_ty__ {
            ( $_ $T:ident ) => {
                $body
            };
        }

        match $key_type {
            DataType::Int8 => __with_ty__! { i8},
            DataType::Int16 => __with_ty__! { i16},
            DataType::UInt8 => __with_ty__! { u8},
            DataType::UInt16 => __with_ty__! { u16},

            _ => $nbody,
        }
    }};
}

#[macro_export]
macro_rules! with_match_integer_32 {
    (
         $key_type:expr, | $_:tt $T:ident | $body:tt,  $nbody:tt
     ) => {{
        macro_rules! __with_ty__ {
            ( $_ $T:ident ) => {
                $body
            };
        }

        match $key_type {
            DataType::Int8 => __with_ty__! { i8},
            DataType::Int16 => __with_ty__! { i16},
            DataType::Int32 => __with_ty__! { i32},
            DataType::UInt8 => __with_ty__! { u8},
            DataType::UInt16 => __with_ty__! { u16},
            DataType::UInt32 => __with_ty__! { u32},

            _ => $nbody,
        }
    }};
}

#[macro_export]
macro_rules! with_match_integer_64 {
    (
         $key_type:expr, | $_:tt $T:ident | $body:tt,  $nbody:tt
     ) => {{
        macro_rules! __with_ty__ {
            ( $_ $T:ident ) => {
                $body
            };
        }

        match $key_type {
            DataType::Int8 => __with_ty__! { i8},
            DataType::Int16 => __with_ty__! { i16},
            DataType::Int32 => __with_ty__! { i32},
            DataType::Int64 => __with_ty__! { i64},
            DataType::UInt8 => __with_ty__! { u8},
            DataType::UInt16 => __with_ty__! { u16},
            DataType::UInt32 => __with_ty__! { u32},
            DataType::UInt64 => __with_ty__! { u64},

            _ => $nbody,
        }
    }};
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
macro_rules! binary_arithmetic_helper {
    ($lhs: ident, $rhs: ident, $T:ty, $R:ty, $op: expr) => {{
        match ($lhs.len(), $rhs.len()) {
            (a, b) if a == b => binary($lhs, $rhs, |l, r| $op(l.as_(), r.as_())),
            (_, 1) => {
                let opt_rhs = $rhs.get(0);
                match opt_rhs {
                    None => DFPrimitiveArray::<$R>::full_null($lhs.len()),
                    Some(rhs) => {
                        let r: $T = rhs.as_();
                        unary($lhs, |l| $op(l.as_(), r))
                    }
                }
            }
            (1, _) => {
                let opt_lhs = $lhs.get(0);
                match opt_lhs {
                    None => DFPrimitiveArray::<$R>::full_null($rhs.len()),
                    Some(lhs) => {
                        let l: $T = lhs.as_();
                        unary($rhs, |r| $op(l, r.as_()))
                    }
                }
            }
            _ => unreachable!(),
        }
        .into()
    }};
}

#[macro_export]
macro_rules! binary_arithmetic {
    ($self: expr, $rhs: expr, $R:ty, $op: expr) => {{
        let lhs = $self.to_minimal_array()?;
        let rhs = $rhs.to_minimal_array()?;
        let lhs: &DFPrimitiveArray<T> = lhs.static_cast();
        let rhs: &DFPrimitiveArray<D> = rhs.static_cast();

        let result: DataColumn = binary_arithmetic_helper!(lhs, rhs, $R, $R, $op);
        Ok(result.resize_constant($self.len()))
    }};
}

#[macro_export]
macro_rules! interval_arithmetic {
    ($self: expr, $rhs: expr, $R:ty, $op: expr) => {{
        let (interval, datetime) = validate_input($self, $rhs);
        let lhs = datetime.column().to_minimal_array()?;
        let lhs: &DFPrimitiveArray<$R> = lhs.static_cast();
        let rhs = interval.column().to_minimal_array()?;
        let rhs = rhs.i64()?;

        let result: DataColumn = binary_arithmetic_helper!(lhs, rhs, i64, $R, $op);
        Ok(result.resize_constant($self.column().len()))
    }};
}

#[macro_export]
macro_rules! arithmetic_helper {
    ($lhs: ident, $rhs: ident, $T: ty, $R: ty, $scalar: expr, $op: expr) => {{
        match ($lhs.len(), $rhs.len()) {
            (a, b) if a == b => binary($lhs, $rhs, |l, r| $op(l.as_(), r.as_())),
            (_, 1) => {
                let opt_rhs = $rhs.get(0);
                match opt_rhs {
                    None => DFPrimitiveArray::<$R>::full_null($lhs.len()),
                    Some(rhs) => {
                        let r: $T = rhs.as_();
                        $scalar($lhs, &r)
                    }
                }
            }
            (1, _) => {
                let opt_lhs = $lhs.get(0);
                match opt_lhs {
                    None => DFPrimitiveArray::<$R>::full_null($rhs.len()),
                    Some(lhs) => {
                        let l: $T = lhs.as_();
                        unary($rhs, |r| $op(l, r.as_()))
                    }
                }
            }
            _ => unreachable!(),
        }
        .into()
    }};
}
