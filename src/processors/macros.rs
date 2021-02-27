// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

/// Downcast processor trait to the object with match.
/// processor_match_downcast!(a, {
///     empty:EmptyProcessor => {},
///     merge:MergeProcessor => {},
///     _=> {},
/// });
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
