// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

macro_rules! processor_match_downcast {
    ( $any:expr, { $( $bind:ident : $ty:ty => $arm:expr ),*, _ => $default:expr } ) => (
        $(
            if $any.as_any().downcast_ref::<$ty>().is_some() {
                $arm
            } else
        )*
        {
            $default
        }
    )
}
