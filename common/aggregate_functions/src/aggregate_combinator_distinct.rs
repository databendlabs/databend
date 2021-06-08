// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::collections::hash_map::RandomState;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::fmt;

use common_datavalues::*;
use common_exception::ErrorCodes;
use common_exception::Result;

use crate::aggregate_function_factory::FactoryFunc;
use crate::aggregator_common::assert_variadic_arguments;
use crate::AggregateCountFunction;
use crate::IAggregateFunction;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct DataGroupValues(Vec<DataGroupValue>);

#[derive(Clone)]
pub struct AggregateDistinctCombinator {
    name: String,

    nested_name: String,
    arguments: Vec<DataField>,
    nested: Box<dyn IAggregateFunction>,
    state: HashSet<DataGroupValues, RandomState>,
}

impl AggregateDistinctCombinator {
    pub fn try_create_uniq(
        nested_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<Box<dyn IAggregateFunction>> {
        AggregateDistinctCombinator::try_create(
            nested_name,
            arguments,
            AggregateCountFunction::try_create,
        )
    }

    pub fn try_create(
        nested_name: &str,
        arguments: Vec<DataField>,
        nested_creator: FactoryFunc,
    ) -> Result<Box<dyn IAggregateFunction>> {
        let name = format!("DistinctCombinator({})", nested_name);
        assert_variadic_arguments(&name, arguments.len(), (1, 32))?;

        let nested_arguments = match nested_name {
            "count" | "uniq" => vec![],
            _ => arguments.clone(),
        };

        let nested = nested_creator(nested_name, nested_arguments)?;
        Ok(Box::new(AggregateDistinctCombinator {
            nested_name: nested_name.to_owned(),
            arguments,
            nested,
            name,
            state: HashSet::new(),
        }))
    }
}

impl IAggregateFunction for AggregateDistinctCombinator {
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
        // faster path for count
        if self
            .nested
            .as_any()
            .downcast_ref::<AggregateCountFunction>()
            .is_some()
        {
            Ok(DataValue::UInt64(Some(self.state.len() as u64)))
        } else {
            // accumulate_scalar
            let mut nested = self.nested.clone();

            // todo: make it parallelized
            self.state.iter().try_for_each(|group_values| {
                let values = group_values.0.iter().map(|v| v.into()).collect::<Vec<_>>();
                nested.accumulate_scalar(&values)
            })?;

            // merge_result
            nested.merge_result()
        }
    }
}

impl fmt::Display for AggregateDistinctCombinator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.nested_name)
    }
}
