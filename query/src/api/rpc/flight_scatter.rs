// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_planners::Expression;

pub trait FlightScatter: Sized {
    fn try_create(schema: DataSchemaRef, expr: Option<Expression>, num: usize) -> Result<Self>;

    fn execute(&self, data_block: &DataBlock) -> Result<Vec<DataBlock>>;
}
