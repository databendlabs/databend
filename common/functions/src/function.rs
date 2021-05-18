// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;
use std::sync::Arc;

use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::Result;
use dyn_clone::DynClone;

pub trait IFunction: fmt::Display + Sync + Send + DynClone {
    fn name(&self) -> &str;
    fn return_type(&self, args: &[DataType]) -> Result<DataType>;
    fn nullable(&self, input_schema: &DataSchema) -> Result<bool>;
    fn eval(&self, columns: &[DataColumnarValue], input_rows: usize) -> Result<DataColumnarValue>;
}
