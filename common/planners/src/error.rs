// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt::Debug;

use snafu::{Backtrace, Snafu};

pub type PlannerResult<T, E = PlannerError> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum PlannerError {
    #[snafu(display("Planner Internal Error: {}", message))]
    Internal {
        message: String,
        backtrace: Backtrace,
    },
}

impl PlannerError {
    pub fn build_internal_error(message: String) -> PlannerError {
        Internal { message }.build()
    }
}

// Internal convert.
impl From<common_functions::FunctionError> for PlannerError {
    fn from(err: common_functions::FunctionError) -> Self {
        Internal {
            message: err.to_string(),
        }
        .build()
    }
}

impl From<std::fmt::Error> for PlannerError {
    fn from(err: std::fmt::Error) -> Self {
        Internal {
            message: err.to_string(),
        }
        .build()
    }
}
