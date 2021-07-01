// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::fmt;

use common_datavalues::columns::DataColumn;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::Result;
use dyn_clone::DynClone;

pub trait AggregateFunction: fmt::Display + Sync + Send + DynClone {
    fn name(&self) -> &str;
    fn return_type(&self) -> Result<DataType>;
    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool>;

    fn as_any(&self) -> &dyn Any;

    // TODO
    // accumulate is to accumulate the columns in batch mod
    // if some aggregate functions wants to iterate over the columns row by row, it doesn't need to implement this function
    fn accumulate(&mut self, columns: &[DataColumn], input_rows: usize) -> Result<()> {
        if columns.is_empty() {
            return Ok(());
        };

        (0..input_rows).try_for_each(|index| {
            let v = columns
                .iter()
                .map(|column| column.try_get(index))
                .collect::<Result<Vec<_>>>()?;
            self.accumulate_scalar(&v)
        })
    }

    // must be implemented even we implement `accumulate`, because the combinator need this function
    fn accumulate_scalar(&mut self, _values: &[DataValue]) -> Result<()> {
        Ok(())
    }

    fn accumulate_result(&self) -> Result<Vec<DataValue>>;
    fn merge(&mut self, _states: &[DataValue]) -> Result<()>;
    fn merge_result(&self) -> Result<DataValue>;
}

dyn_clone::clone_trait_object!(AggregateFunction);
