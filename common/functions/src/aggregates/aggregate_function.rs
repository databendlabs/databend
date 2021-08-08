// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;
use std::sync::Arc;

use bytes::BytesMut;
use common_datavalues::series::Series;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::Result;

use super::StateAddr;

pub type AggregateFunctionRef = Arc<dyn AggregateFunction>;

/// AggregateFunction
/// In AggregateFunction, all datablock columns are not ConstantColumn, we take the column as Series
pub trait AggregateFunction: fmt::Display + Sync + Send {
    fn name(&self) -> &str;
    fn return_type(&self) -> Result<DataType>;
    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool>;

    fn allocate_state(&self, arena: &bumpalo::Bump) -> StateAddr;

    // accumulate is to accumulate the arrays in batch mode
    // common used when there is no group by for aggregate function
    fn accumulate(&self, place: StateAddr, arrays: &[Series], input_rows: usize) -> Result<()> {
        (0..input_rows).try_for_each(|row| self.accumulate_row(place, row, arrays))
    }

    // used when we need to caclulate row by row
    fn accumulate_row(&self, _place: StateAddr, _row: usize, _arrays: &[Series]) -> Result<()> {
        Ok(())
    }

    // serialize  the state into binary array
    fn serialize(&self, _place: StateAddr, _writer: &mut BytesMut) -> Result<()>;

    fn deserialize(&self, _place: StateAddr, _reader: &mut &[u8]) -> Result<()>;

    fn merge(&self, _place: StateAddr, _rhs: StateAddr) -> Result<()>;

    // TODO append the value into the column builder
    fn merge_result(&self, _place: StateAddr) -> Result<DataValue>;
}
