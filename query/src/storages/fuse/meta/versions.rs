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

use common_datablocks::DataBlock;
use common_exception::ErrorCode;

use crate::storages::fuse::meta::v0::segment::SegmentInfo as SegmentInfoV0;
use crate::storages::fuse::meta::v0::snapshot::TableSnapshot as TableSnapshotV0;
use crate::storages::fuse::meta::v1::segment::SegmentInfo;
use crate::storages::fuse::meta::v1::snapshot::TableSnapshot;

// TODO tuple struct
pub struct Versioned<T, const V: u64> {
    _p: PhantomData<T>,
}

impl<T, const V: u64> Versioned<T, V> {
    pub fn new() -> Self {
        Self { _p: PhantomData }
    }
}

pub enum SnapshotVersion {
    V0(Versioned<TableSnapshotV0, 0>),
    V1(Versioned<TableSnapshot, 1>),
}

pub enum SegmentInfoVersion {
    V0(Versioned<SegmentInfoV0, 0>),
    V1(Versioned<SegmentInfo, 1>),
}

pub enum BlockVersion {
    V0(Versioned<DataBlock, 0>),
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

    impl TryFrom<u64> for BlockVersion {
        type Error = ErrorCode;
        fn try_from(value: u64) -> std::result::Result<Self, Self::Error> {
            match value {
                0 => Ok(BlockVersion::V0(Versioned::new())),
                _ => Err(ErrorCode::LogicalError(format!(
                    "unknown block version {value}, versions supported: 0"
                ))),
            }
        }
    }

    impl From<&BlockVersion> for u64 {
        fn from(v: &BlockVersion) -> u64 {
            match v {
                BlockVersion::V0(_) => 0,
            }
        }
    }

    impl From<&SegmentInfoVersion> for u64 {
        fn from(v: &SegmentInfoVersion) -> u64 {
            match v {
                SegmentInfoVersion::V0(_) => 0,
                SegmentInfoVersion::V1(_) => 1,
            }
        }
    }
}

mod experiment {}
