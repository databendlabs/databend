// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::DataValue;
use common_exception::Result;

pub type StateAddr = usize;
pub trait GetState<'a, T> {
    fn get(place: StateAddr) -> &'a mut T {
        unsafe { &mut *(place as *mut T) }
    }
}

pub struct AggregateSingeValueState {
    pub value: DataValue,
}

impl<'a> GetState<'a, AggregateSingeValueState> for AggregateSingeValueState {}

impl AggregateSingeValueState {
    pub fn serialize(&self, writer: &mut Vec<u8>) -> Result<()> {
        serde_json::to_writer(writer, &self.value)?;
        Ok(())
    }

    pub fn deserialize(&mut self, reader: &[u8]) -> Result<()> {
        self.value = serde_json::from_slice(reader)?;
        Ok(())
    }
}
