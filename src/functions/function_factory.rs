// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use lazy_static::lazy_static;
use std::collections::HashMap;

use crate::error::{FuseQueryError, FuseQueryResult};
use crate::functions::{
    AggregatorFunction, ArithmeticFunction, ComparisonFunction, Function, LogicFunction,
};

pub struct FunctionFactory;
type Func = fn(args: &[Function]) -> FuseQueryResult<Function>;

lazy_static! {
    static ref FACTORY: HashMap<&'static str, Func> = {
        let mut map: HashMap<&'static str, Func> = HashMap::new();

        // Arithmetic functions.
        map.insert("+", ArithmeticFunction::try_create_add_func);
        map.insert("-", ArithmeticFunction::try_create_sub_func);
        map.insert("*", ArithmeticFunction::try_create_mul_func);
        map.insert("/", ArithmeticFunction::try_create_div_func);

        // Comparison functions.
        map.insert("=", ComparisonFunction::try_create_eq_func);
        map.insert("<", ComparisonFunction::try_create_lt_func);
        map.insert(">", ComparisonFunction::try_create_gt_func);
        map.insert("<=", ComparisonFunction::try_create_lt_eq_func);
        map.insert(">=", ComparisonFunction::try_create_gt_eq_func);

        // Logic functions.
        map.insert("and", LogicFunction::try_create_and_func);
        map.insert("or", LogicFunction::try_create_or_func);

        // Aggregate functions.
        map.insert("count", AggregatorFunction::try_create_count_func);
        map.insert("min", AggregatorFunction::try_create_min_func);
        map.insert("max", AggregatorFunction::try_create_max_func);
        map.insert("sum", AggregatorFunction::try_create_sum_func);
        map.insert("avg", AggregatorFunction::try_create_avg_func);

        map
    };
}

impl FunctionFactory {
    pub fn get(name: &str, args: &[Function]) -> FuseQueryResult<Function> {
        let creator = FACTORY
            .get(&*name.to_lowercase())
            .ok_or_else(|| FuseQueryError::Internal(format!("Unsupported Function: {}", name)))?;
        (creator)(args)
    }

    pub fn registered_names() -> Vec<String> {
        FACTORY.keys().into_iter().map(|x| x.to_string()).collect()
    }
}
