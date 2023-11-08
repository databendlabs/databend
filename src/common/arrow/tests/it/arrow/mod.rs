// this landed on 1.60. Let's not force everyone to bump just yet
#![allow(clippy::unnecessary_lazy_evaluations)]

mod array;
#[cfg(feature = "arrow")]
mod arrow;

mod bitmap;
mod buffer;
mod ffi;
mod scalar;
mod temporal_conversions;
mod types;

mod io;
mod test_util;

mod compute;
