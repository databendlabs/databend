// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

#[macro_export]
macro_rules! downcast_array {
    ($ARRAY:expr, $TYPE:ident) => {
        $ARRAY.as_any().downcast_ref::<$TYPE>().expect(
            format!(
                "Unsupported downcast datatype:{:?} item to {}",
                ($ARRAY).data_type(),
                stringify!($TYPE)
            )
            .as_str(),
        );
    };
}
