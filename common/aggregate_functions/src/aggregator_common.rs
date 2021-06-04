// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt::Display;

use common_exception::ErrorCodes;
use common_exception::Result;

pub fn assert_unary_arguments<D: Display>(name: D, actual: usize) -> Result<()> {
    if actual != 1 {
        return Err(ErrorCodes::NumberArgumentsNotMatch(format!(
            "{} expect to have single arguments, but got {}",
            name, actual
        )));
    }
    Ok(())
}

pub fn assert_binary_arguments<D: Display>(name: D, actual: usize) -> Result<()> {
    if actual != 2 {
        return Err(ErrorCodes::NumberArgumentsNotMatch(format!(
            "{} expect to have two arguments, but got {}",
            name, actual
        )));
    }
    Ok(())
}

#[allow(dead_code)]
pub fn assert_arguments<D: Display>(name: D, actual: usize, expected: usize) -> Result<()> {
    if actual != expected {
        return Err(ErrorCodes::NumberArgumentsNotMatch(format!(
            "{} expect to have {} arguments, but got {}",
            name, expected, actual
        )));
    }
    Ok(())
}

pub fn assert_variadic_arguments<D: Display>(
    name: D,
    actual: usize,
    expected: (usize, usize),
) -> Result<()> {
    if actual < expected.0 || actual > expected.1 {
        return Err(ErrorCodes::NumberArgumentsNotMatch(format!(
            "{} expect to have [{}, {}) arguments, but got {}",
            name, expected.0, expected.1, actual
        )));
    }
    Ok(())
}
