//! contains a wide range of compute operations (e.g.
//! [`arithmetics`], [`aggregate`],
//! [`filter`], [`comparison`], and [`sort`])
//!
//! This module's general design is
//! that each operator has two interfaces, a statically-typed version and a dynamically-typed
//! version.
//! The statically-typed version expects concrete arrays (such as [`PrimitiveArray`](crate::arrow::array::PrimitiveArray));
//! the dynamically-typed version expects `&dyn Array` and errors if the the type is not
//! supported.
//! Some dynamically-typed operators have an auxiliary function, `can_*`, that returns
//! true if the operator can be applied to the particular `DataType`.

#[cfg(any(feature = "compute_aggregate", feature = "io_parquet"))]
#[cfg_attr(docsrs, doc(cfg(feature = "compute_aggregate")))]
pub mod aggregate;
pub mod arity;
#[cfg(feature = "compute_cast")]
#[cfg_attr(docsrs, doc(cfg(feature = "compute_cast")))]
pub mod cast;
#[cfg(feature = "compute_concatenate")]
#[cfg_attr(docsrs, doc(cfg(feature = "compute_concatenate")))]
pub mod concatenate;
#[cfg(feature = "compute_merge_sort")]
#[cfg_attr(docsrs, doc(cfg(feature = "compute_merge_sort")))]
pub mod merge_sort;
#[cfg(feature = "compute_sort")]
#[cfg_attr(docsrs, doc(cfg(feature = "compute_sort")))]
pub mod sort;
#[cfg(feature = "compute_take")]
#[cfg_attr(docsrs, doc(cfg(feature = "compute_take")))]
pub mod take;
mod utils;
