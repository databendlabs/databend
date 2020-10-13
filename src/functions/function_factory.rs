// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use crate::error::{Error, Result};

use crate::functions::{aggregate, arithmetic, Function};

pub struct FunctionFactory {}

impl FunctionFactory {
    pub fn get(name: &str, args: &[Function]) -> Result<Function> {
        match name.to_uppercase().as_str() {
            "+" => arithmetic::AddFunction::create(args),
            "-" => arithmetic::SubFunction::create(args),
            "*" => arithmetic::MulFunction::create(args),
            "/" => arithmetic::DivFunction::create(args),
            "COUNT" => aggregate::CountAggregateFunction::create(),
            _ => Err(Error::Unsupported(format!(
                "Unsupported Function: {}",
                name
            ))),
        }
    }
}
