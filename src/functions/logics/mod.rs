// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

mod logic_test;

mod logic;
mod logic_and;
mod logic_or;

pub use logic::LogicFunction;
pub use logic_and::LogicAndFunction;
pub use logic_or::LogicOrFunction;
