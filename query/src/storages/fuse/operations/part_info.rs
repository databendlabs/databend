//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use common_exception::ErrorCode;
use common_exception::Result;

/// Holds the location and length information of a given [Part].
///
/// It is intended to be encoded as string, and assigned to the field `name` of a given [`Part`],
/// then passed around to the `read` method of [FuseTable], where the name filed will be decoded back to [PartInfo]
///
/// [Part]: common_planners::Part
/// [FuseTable]: crate::storages::fuse::FuseTable
///
#[derive(PartialEq, Debug)]
pub struct PartInfo<'a>(&'a str, u64);

impl<'a> PartInfo<'a> {
    #[inline]
    pub fn new(location: &'a str, length: u64) -> Self {
        Self(location, length)
    }

    #[inline]
    pub fn location(&self) -> &str {
        self.0
    }

    #[inline]
    pub fn length(&self) -> u64 {
        self.1
    }

    #[inline]
    pub fn decode(part_name: &'a str) -> Result<PartInfo> {
        let parts = part_name.split('-').collect::<Vec<_>>();
        if parts.len() != 2 {
            return Err(ErrorCode::LogicalError(format!(
                "invalid format of `Part.name` , expects 'name-length', got {}",
                part_name
            )));
        }
        let part_location = parts[0];
        let part_len = parts[1].parse::<u64>().map_err(|e| {
            ErrorCode::LogicalError(format!(
                "invalid format of `Part.name` format, expects number for length', but got {}, {}",
                parts[1], e
            ))
        })?;
        Ok(Self(part_location, part_len))
    }

    #[inline]
    pub fn encode(&self) -> String {
        format!("{}-{}", self.0, self.1,)
    }
}
