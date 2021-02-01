// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

// for settings getter setter
macro_rules! apply_udf_enum {
    ($($TYPE: tt),* ) => {
        #[derive(Clone, Debug)]
        pub enum UDFFunction {
            $(
                $TYPE($TYPE),
            )*
        }
    };
}

macro_rules! apply_udf_register {
    ($(($NAME: expr, $TYPE: tt)),* ) => {
        pub fn register(map: FactoryFuncRef) -> FuseQueryResult<()> {
            let mut map = map.as_ref().lock()?;
            $(
                debug!("Register function {}", $NAME);
                map.insert($NAME, $TYPE::try_create);
            )*
            Ok(())
        }
    };
}

macro_rules! apply_udf_return_type {
    ($(($NAME: expr, $TYPE: tt)),* ) => {
        pub fn return_type(&self, input_schema: &DataSchema) -> FuseQueryResult<DataType> {
            match self {
                $(
                    UDFFunction::$TYPE(v) => v.return_type(input_schema),
                )*
            }
        }
    };
}

macro_rules! apply_udf_eval {
    ($(($NAME: expr, $TYPE: tt)),* ) => {
        pub fn eval(&mut self, block: &DataBlock) -> FuseQueryResult<DataColumnarValue> {
            match self {
                $(
                    UDFFunction::$TYPE(v) => v.eval(block),
                )*
            }
        }
    };
}

macro_rules! apply_udf_accumulate {
    ($(($NAME: expr, $TYPE: tt)),* ) => {
        pub fn accumulate(&mut self, block: &DataBlock) -> FuseQueryResult<()> {
            match self {
                $(
                    UDFFunction::$TYPE(v) => v.accumulate(block),
                )*
            }
        }
    };
}

macro_rules! apply_udf_accumulate_result {
    ($(($NAME: expr, $TYPE: tt)),* ) => {
        pub fn accumulate_result(&self) -> FuseQueryResult<Vec<DataValue>> {
            match self {
                $(
                    UDFFunction::$TYPE(v) => v.accumulate_result(),
                )*
            }
        }
    };
}

macro_rules! apply_udf_nullable {
    ($(($NAME: expr, $TYPE: tt)),* ) => {
        pub fn nullable(&self, input_schema: &DataSchema) -> FuseQueryResult<bool> {
            match self {
                $(
                    UDFFunction::$TYPE(v) => v.nullable(input_schema),
                )*
            }
        }
    };
}

macro_rules! apply_udf_set_depth {
    ($(($NAME: expr, $TYPE: tt)),* ) => {
        pub fn set_depth(&mut self, depth: usize) {
            match self {
                $(
                    UDFFunction::$TYPE(v) => v.set_depth(depth),
                )*
            }
        }
    };
}

macro_rules! apply_udf_merge {
    ($(($NAME: expr, $TYPE: tt)),* ) => {
        pub fn merge(&mut self, states: &[DataValue]) -> FuseQueryResult<()> {
            match self {
                $(
                    UDFFunction::$TYPE(v) => v.merge(states),
                )*
            }
        }
    };
}

macro_rules! apply_udf_merge_result {
    ($(($NAME: expr, $TYPE: tt)),* ) => {
        pub fn merge_result(&self) -> FuseQueryResult<DataValue> {
            match self {
                $(
                    UDFFunction::$TYPE(v) => v.merge_result(),
                )*
            }
        }
    };
}

macro_rules! apply_udf_impl_display {
    ($(($NAME: expr, $TYPE: tt)),* ) => {
        impl fmt::Display for UDFFunction {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self {
                    $(
                        UDFFunction::$TYPE(v) => write!(f, "{}", v),
                    )*
                }
            }
        }
    };
}

macro_rules! apply_macros {
    ($(($NAME: expr, $TYPE: tt)),* ) => {
        apply_udf_enum! { $($TYPE), * }

        impl UDFFunction {
            apply_udf_register!{ $( ($NAME, $TYPE) ), * }
            apply_udf_return_type! {  $( ($NAME, $TYPE) ), * }
            apply_udf_eval! {  $( ($NAME, $TYPE) ), * }
            apply_udf_accumulate! {  $( ($NAME, $TYPE) ), * }
            apply_udf_accumulate_result! {  $( ($NAME, $TYPE) ), * }
            apply_udf_nullable! {  $( ($NAME, $TYPE) ), * }
            apply_udf_set_depth! {  $( ($NAME, $TYPE) ), * }
            apply_udf_merge! {  $( ($NAME, $TYPE) ), * }
            apply_udf_merge_result! {  $( ($NAME, $TYPE) ), * }
        }

        apply_udf_impl_display! { $( ($NAME, $TYPE) ), *  }
    };
}
