// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::ErrorCode;
use common_exception::Result;

use crate::prelude::*;

macro_rules! creator {
    ($T: ident, $data_type: expr, $capacity: expr) => {
        if $T::data_type() == $data_type {
            return Ok(Box::new(PrimitiveArrayBuilder::<$T>::with_capacity(
                $capacity,
            )));
        }
    };
}

impl DataType {
    pub fn create_deserializer(&self, capacity: usize) -> Result<Box<dyn ArrayDeserializer>> {
        let data_type = self.clone();
        dispatch_numeric_types! { creator, data_type, capacity}

        match self {
            DataType::Boolean => Ok(Box::new(BooleanArrayBuilder::with_capacity(capacity))),

            DataType::UInt8 => Ok(Box::new(PrimitiveArrayBuilder::<UInt8Type>::with_capacity(
                capacity,
            ))),

            DataType::Utf8 => Ok(Box::new(Utf8ArrayBuilder::with_capacity(capacity))),

            other => Err(ErrorCode::BadDataValueType(format!(
                "create_deserializer does not support type '{:?}'",
                other
            ))),
        }
    }
}
