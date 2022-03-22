//  Copyright 2022 Datafuse Labs.
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

use std::marker::PhantomData;

use common_exception::ErrorCode;

use crate::storages::fuse::meta::v0::segment::SegmentInfo as SegmentInfoV0;
use crate::storages::fuse::meta::v0::snapshot::TableSnapshot as TableSnapshotV0;
use crate::storages::fuse::meta::v1::segment::SegmentInfo;
use crate::storages::fuse::meta::v1::snapshot::TableSnapshot;

pub struct Versioned<T> {
    _p: PhantomData<T>,
}

impl<T> Versioned<T> {
    pub fn new() -> Self {
        Self { _p: PhantomData }
    }
}

impl<T> Default for Versioned<T> {
    fn default() -> Self {
        Self::new()
    }
}

pub enum SnapshotVersion {
    V0(Versioned<TableSnapshotV0>),
    V1(Versioned<TableSnapshot>),
}

pub enum SegmentInfoVersion {
    V0(Versioned<SegmentInfoV0>),
    V1(Versioned<SegmentInfo>),
}

mod converters {
    use super::*;
    impl TryFrom<u64> for SnapshotVersion {
        type Error = ErrorCode;
        fn try_from(value: u64) -> std::result::Result<Self, Self::Error> {
            match value {
                0 => Ok(SnapshotVersion::V0(Versioned::new())),
                1 => Ok(SnapshotVersion::V1(Versioned::new())),
                _ => Err(ErrorCode::LogicalError(format!(
                    "unknown snapshot version {value}, versions supported: 0, 1"
                ))),
            }
        }
    }

    impl TryFrom<u64> for SegmentInfoVersion {
        type Error = ErrorCode;
        fn try_from(value: u64) -> std::result::Result<Self, Self::Error> {
            match value {
                0 => Ok(SegmentInfoVersion::V0(Versioned::new())),
                1 => Ok(SegmentInfoVersion::V1(Versioned::new())),
                _ => Err(ErrorCode::LogicalError(format!(
                    "unknown segment version {value}, versions supported: 0, 1"
                ))),
            }
        }
    }
}
