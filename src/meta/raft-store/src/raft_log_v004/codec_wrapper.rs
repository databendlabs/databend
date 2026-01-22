// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::type_name;
use std::fmt;
use std::io;
use std::ops::Deref;
use std::ops::DerefMut;

use log::error;
use raft_log::codeq::Decode;
use raft_log::codeq::Encode;
use raft_log::codeq::OffsetWriter;
use serde::de::DeserializeOwned;

/// Codec wrapper to implement Encode/Decode for foreign types.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Cw<T: 'static>(pub T);

impl<T: 'static> Deref for Cw<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: 'static> DerefMut for Cw<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: 'static> fmt::Display for Cw<T>
where T: fmt::Display
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<T> Encode for Cw<T>
where T: serde::Serialize
{
    fn encode<W: io::Write>(&self, mut w: W) -> Result<usize, io::Error> {
        let mut ow = OffsetWriter::new(&mut w);

        rmp_serde::encode::write_named(&mut ow, &self.0).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{e}; when:(encode: {})", type_name::<T>()),
            )
        })?;

        let n = ow.offset();

        Ok(n)
    }
}

impl<T> Decode for Cw<T>
where T: DeserializeOwned
{
    fn decode<R: io::Read>(r: R) -> Result<Self, io::Error> {
        // rmp_serde::decode::from_read returns when a value is successfully decoded.
        let d = rmp_serde::decode::from_read(r).map_err(|e| {
            error!("error: {:?} when:(decoding: {})", e, type_name::<T>());
            let typ = match &e {
                rmp_serde::decode::Error::InvalidMarkerRead(io_err) => io_err.kind(),
                rmp_serde::decode::Error::InvalidDataRead(io_err) => io_err.kind(),
                _ => io::ErrorKind::InvalidData,
            };
            io::Error::new(typ, format!("{e}; when:(decode: {})", type_name::<T>()))
        })?;

        Ok(Cw(d))
    }
}

impl<T: 'static> Cw<T> {
    pub fn unpack(self) -> T {
        self.0
    }

    pub fn to_inner(&self) -> T
    where T: Clone {
        self.0.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use databend_common_meta_types::raft_types::LogId;
    use databend_common_meta_types::raft_types::new_log_id;
    use raft_log::codeq::Decode;
    use raft_log::codeq::Encode;

    use super::Cw;

    #[test]
    fn test_cw_codec() -> Result<(), io::Error> {
        let mut buf = Vec::new();

        let log_id = Cw(new_log_id(1, 2, 3));
        log_id.encode(&mut buf)?;

        let log_id = Cw(new_log_id(4, 5, 6));
        log_id.encode(&mut buf)?;

        let mut r = buf.as_slice();

        let l1: Cw<LogId> = Decode::decode(&mut r)?;
        let l2: Cw<LogId> = Decode::decode(&mut r)?;
        assert_eq!(0, r.len());
        assert!(Cw::<LogId>::decode(&mut r).is_err());

        assert_eq!(l1.0, new_log_id(1, 2, 3));
        assert_eq!(l2.0, new_log_id(4, 5, 6));

        Ok(())
    }
}
