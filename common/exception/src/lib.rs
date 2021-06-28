// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
mod exception_test;

pub mod exception;

pub use exception::ErrorCode;
pub use exception::Result;
pub use exception::ToErrorCode;

pub mod prelude {

    pub use crate::exception::ErrorCode;
    pub use crate::exception::Result;
    pub use crate::exception::ToErrorCode;
}
