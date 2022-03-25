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

//! Versions and Migration
//!
//! TODO doc
//!

use std::marker::PhantomData;

use common_datablocks::DataBlock;
use common_exception::ErrorCode;

use crate::storages::fuse::meta::common::Versioned;
use crate::storages::fuse::meta::v0;
use crate::storages::fuse::meta::v1;

pub const CURRNET_SEGMETN_VERSION: u64 = v1::SegmentInfo::VERSION;
pub const CURRNET_SNAPSHOT_VERSION: u64 = v1::TableSnapshot::VERSION;
pub const CURRNET_BLOCK_VERSION: u64 = DataBlock::VERSION;

impl Versioned<0> for v0::SegmentInfo {}
impl Versioned<1> for v1::SegmentInfo {}

pub enum SegmentInfoVersion {
    V0(PhantomData<v0::SegmentInfo>),
    V1(PhantomData<v1::SegmentInfo>),
}

impl Versioned<0> for v0::TableSnapshot {}
impl Versioned<1> for v1::TableSnapshot {}

pub enum SnapshotVersion {
    V0(PhantomData<v0::TableSnapshot>),
    V1(PhantomData<v1::TableSnapshot>),
}

impl Versioned<0> for DataBlock {}

#[allow(dead_code)]
pub enum BlockVersion {
    V0(PhantomData<DataBlock>),
}

mod converters {
    use super::*;

    impl TryFrom<u64> for SegmentInfoVersion {
        type Error = ErrorCode;
        fn try_from(value: u64) -> std::result::Result<Self, Self::Error> {
            match value {
                0 => Ok(SegmentInfoVersion::V0(ver_eq::<_, 0>(PhantomData))),
                1 => Ok(SegmentInfoVersion::V1(ver_eq::<_, 1>(PhantomData))),
                _ => Err(ErrorCode::LogicalError(format!(
                    "unknown segment version {value}, versions supported: 0, 1"
                ))),
            }
        }
    }

    impl TryFrom<u64> for SnapshotVersion {
        type Error = ErrorCode;
        fn try_from(value: u64) -> std::result::Result<Self, Self::Error> {
            match value {
                0 => Ok(SnapshotVersion::V0(ver_eq::<_, 0>(PhantomData))),
                1 => Ok(SnapshotVersion::V1(ver_eq::<_, 1>(PhantomData))),
                _ => Err(ErrorCode::LogicalError(format!(
                    "unknown snapshot segment version {value}, versions supported: 0, 1"
                ))),
            }
        }
    }

    /// Statically check that if T implements Versoined<U> where U equals V
    #[inline]
    fn ver_eq<T, const V: u64>(v: PhantomData<T>) -> PhantomData<T>
    where T: Versioned<V> {
        v
    }
}
