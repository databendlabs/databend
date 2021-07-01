// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::fmt;

use common_arrow::arrow;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::aggregate_function_factory::FactoryFunc;
use crate::AggregateFunction;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct DataGroupValues(Vec<DataGroupValue>);

#[derive(Clone)]
pub struct AggregateIfCombinator {
    name: String,
    argument_len: usize,
    nested_name: String,
    nested: Box<dyn AggregateFunction>,
}

impl AggregateIfCombinator {
    pub fn try_create(
        nested_name: &str,
        arguments: Vec<DataField>,
        nested_creator: FactoryFunc,
    ) -> Result<Box<dyn AggregateFunction>> {
        let name = format!("IfCombinator({})", nested_name);
        let argument_len = arguments.len();

        if argument_len == 0 {
            return Err(ErrorCode::NumberArgumentsNotMatch(format!(
                "{} expect to have more than one argument",
                name
            )));
        }

        match arguments[argument_len - 1].data_type() {
            DataType::Boolean => {}
            other => {
                return Err(ErrorCode::BadArguments(format!(
                    "The type of the last argument for {} must be boolean type, but got {:?}",
                    name, other
                )));
            }
        }

        let nested_arguments = &arguments[0..argument_len - 1];
        let nested = nested_creator(nested_name, nested_arguments.to_vec())?;

        Ok(Box::new(AggregateIfCombinator {
            name,
            argument_len,
            nested_name: nested_name.to_owned(),
            nested,
        }))
    }
}

impl AggregateFunction for AggregateIfCombinator {
    fn name(&self) -> &str {
        &self.name
    }

    fn return_type(&self) -> Result<DataType> {
        self.nested.return_type()
    }

    fn nullable(&self, input_schema: &DataSchema) -> Result<bool> {
        self.nested.nullable(input_schema)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn accumulate(&mut self, columns: &[DataColumn], _input_rows: usize) -> Result<()> {
        if columns.is_empty() {
            return Ok(());
        };

        let boolean_array = columns[self.argument_len - 1].to_array()?;
        let boolean_array = boolean_array.bool()?;

        let arrow_filter_array = boolean_array.downcast_ref();

        let mut column_array = Vec::with_capacity(self.argument_len - 1);
        let row_size = match columns.len() - 1 {
            0 => {
                // if it has no args, only return the row_count
                if boolean_array.null_count() > 0 {
                    // this greatly simplifies subsequent filtering code
                    // now we only have a boolean mask to deal with
                    arrow::compute::prep_null_mask_filter(arrow_filter_array)
                        .values()
                        .count_set_bits()
                } else {
                    arrow_filter_array.values().count_set_bits()
                }
            }
            1 => {
                // single array handle
                let array = columns[0].to_array()?;
                let data =
                    arrow::compute::filter(array.get_array_ref().as_ref(), arrow_filter_array)?;
                column_array.push(DataColumn::from(data));
                column_array[0].len()
            }
            _ => {
                // multi array handle
                let mut args_array = Vec::with_capacity(self.argument_len - 1);
                for column in columns.iter().take(self.argument_len - 1) {
                    let array = column.to_array()?;
                    args_array.push(array);
                }
                let data = DataArrayFilter::filter_batch_array(args_array, boolean_array)?;
                data.into_iter()
                    .map(DataColumn::from)
                    .for_each(|column| column_array.push(column));
                column_array[0].len()
            }
        };
        self.nested.accumulate(column_array.as_slice(), row_size)?;
        Ok(())
    }

    fn accumulate_result(&self) -> Result<Vec<DataValue>> {
        self.nested.accumulate_result()
    }

    fn merge(&mut self, states: &[DataValue]) -> Result<()> {
        self.nested.merge(states)
    }

    fn merge_result(&self) -> Result<DataValue> {
        self.nested.merge_result()
    }
}

impl fmt::Display for AggregateIfCombinator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.nested_name)
    }
}
