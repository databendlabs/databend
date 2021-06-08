// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::hash_map::RandomState;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::fmt;

use common_datavalues::*;
use common_exception::ErrorCodes;
use common_exception::Result;

use crate::aggregator_common::assert_variadic_arguments;
use crate::IAggregateFunction;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct DataGroupValues(Vec<DataGroupValue>);

#[derive(Clone)]
pub struct AggregateUniqFunction {
    display_name: String,
    arguments: Vec<DataField>,

    state: HashSet<DataGroupValues, RandomState>,
}

impl AggregateUniqFunction {
    pub fn try_create(
        display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<Box<dyn IAggregateFunction>> {
        assert_variadic_arguments(display_name, arguments.len(), (1, 32))?;

        Ok(Box::new(AggregateUniqFunction {
            display_name: display_name.to_string(),
            arguments,
            state: HashSet::new(),
        }))
    }
}

impl IAggregateFunction for AggregateUniqFunction {
    fn name(&self) -> &str {
        "AggregateUniqFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.arguments[0].data_type().clone())
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn accumulate_scalar(&mut self, values: &[DataValue]) -> Result<()> {
        if !values.iter().any(|v| v.is_null()) {
            self.state.insert(DataGroupValues(
                values
                    .iter()
                    .map(DataGroupValue::try_from)
                    .collect::<Result<Vec<_>>>()?,
            ));
        }
        Ok(())
    }

    // serialize as Vec<DataValue::List>
    fn accumulate_result(&self) -> Result<Vec<DataValue>> {
        let mut results = self
            .arguments
            .iter()
            .map(|f| DataValue::List(Some(vec![]), f.data_type().clone()))
            .collect::<Vec<_>>();

        let mut vecs = results
            .iter_mut()
            .map(|result| match result {
                DataValue::List(Some(ref mut vec), _) => vec,
                _ => unreachable!(),
            })
            .collect::<Vec<_>>();

        self.state.iter().for_each(|group_values| {
            group_values
                .0
                .iter()
                .enumerate()
                .for_each(|(col_index, group_value)| {
                    vecs[col_index].push(DataValue::from(group_value));
                })
        });

        Ok(results)
    }

    fn merge(&mut self, states: &[DataValue]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let col_values = states
            .iter()
            .map(|value| match value {
                DataValue::List(Some(values), _) => Ok(values),
                _ => Err(ErrorCodes::BadDataValueType(format!(
                    "Unexpceted accumulate state type: {}",
                    value.data_type()
                ))),
            })
            .collect::<Result<Vec<_>>>()?;

        (0..col_values[0].len()).try_for_each(|row_index| {
            let row_values = col_values
                .iter()
                .map(|col| col[row_index].clone())
                .collect::<Vec<_>>();
            self.accumulate_scalar(&row_values)
        })
    }

    fn merge_result(&self) -> Result<DataValue> {
        Ok(DataValue::UInt64(Some(self.state.len() as u64)))
    }
}

impl fmt::Display for AggregateUniqFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl AggregateUniqFunction {}
