// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::ErrorCode;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sled::IVec;

/// Serialize/deserialize(ser/de) to/from sled values.
pub trait SledSerde {
    fn ser(&self) -> Result<IVec, ErrorCode>;
    fn de<T: AsRef<[u8]>>(v: T) -> Result<Self, ErrorCode>
    where Self: Sized;
}

impl<SD: Serialize + DeserializeOwned + Sized> SledSerde for SD {
    /// (ser)ialize a value to `sled::IVec`.
    fn ser(&self) -> Result<IVec, ErrorCode> {
        let x = serde_json::to_vec(self)?;
        Ok(x.into())
    }

    /// (de)serialize a value from `sled::IVec`.
    fn de<T: AsRef<[u8]>>(v: T) -> Result<Self, ErrorCode> {
        let s = serde_json::from_slice(v.as_ref())?;
        Ok(s)
    }
}
