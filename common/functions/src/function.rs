// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_exception::Result;
use dyn_clone::DynClone;

pub trait IFunction: fmt::Display + Sync + Send + DynClone {
    fn name(&self) -> &str;

    fn num_arguments(&self) -> usize {
        return 0;
    }

    // (1, 3) means we only accept [1, 3) arguments
    // None means it's not variadic function
    fn variadic_arguments(&self) -> Option<(usize, usize)> {
        None
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType>;
    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool>;
    fn eval(&self, columns: &[DataColumnarValue], _input_rows: usize) -> Result<DataColumnarValue>;
}
