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

//! Implement the conversion between `Marked` and other types

use std::io;

use crate::marked::Marked;

impl TryFrom<Marked> for Marked<String> {
    type Error = io::Error;

    /// Convert Marked<Vec<u8>> to Marked<String>
    fn try_from(marked: Marked) -> Result<Self, Self::Error> {
        // convert Vec<u8> to String
        match marked {
            Marked::TombStone { internal_seq } => Ok(Marked::TombStone { internal_seq }),
            Marked::Normal {
                internal_seq,
                value,
                meta,
            } => {
                let s = String::from_utf8(value).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("fail to convert Vec<u8> to String: {}", e),
                    )
                })?;
                Ok(Marked::Normal {
                    internal_seq,
                    value: s,
                    meta,
                })
            }
        }
    }
}

impl From<Marked<String>> for Marked {
    /// Convert Marked<String> to Marked<Vec<u8>>
    fn from(value: Marked<String>) -> Self {
        match value {
            Marked::TombStone { internal_seq } => Marked::TombStone { internal_seq },
            Marked::Normal {
                internal_seq,
                value,
                meta,
            } => {
                let v = value.into_bytes();
                Marked::Normal {
                    internal_seq,
                    value: v,
                    meta,
                }
            }
        }
    }
}
